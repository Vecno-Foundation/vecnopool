//src/treasury/sharehandler.rs

use anyhow::{Context, Result};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use log::{debug, info, warn};
use crate::database::db::Db;
use crate::stratum::jobs::Jobs;

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Contribution {
    pub address: String,
    pub difficulty: i64,
    pub timestamp: i64,
    pub job_id: String,
    pub daa_score: i64,
    pub extranonce: String,
    pub nonce: String,
    pub reward_block_hash: Option<String>,
}

#[derive(Debug)]
pub struct Sharehandler {
    pub db: Arc<Db>,
    pub share_batch: mpsc::Sender<Contribution>,
    pub worker_share_counts: Arc<DashMap<String, Arc<AtomicU64>>>,
    pub worker_invalid_shares: Arc<DashMap<String, Arc<AtomicU64>>>,
    pub worker_duplicate_shares: Arc<DashMap<String, Arc<AtomicU64>>>,
    pub worker_log_times: Arc<DashMap<String, Arc<AtomicU64>>>,
    pub worker_submitted_shares: Arc<DashMap<String, Arc<tokio::sync::RwLock<HashSet<(String, String)>>>>>,
    pub pool_fee: f64,
    pub jobs: Arc<Jobs>,
}

impl Sharehandler {
    pub async fn new(
        db: Arc<Db>,
        pool_fee: f64,
        jobs: Arc<Jobs>,
        window_time_ms: u64,
    ) -> Result<Self> {
        // Initialize total_shares by querying the database for recent shares
        let total_shares = db.get_total_shares(window_time_ms / 1000)
            .await
            .context("Failed to load total shares")?;
        let total_shares = Arc::new(AtomicU64::new(total_shares));
        let (share_batch, mut share_batch_rx) = mpsc::channel::<Contribution>(500);
        let worker_share_counts = Arc::new(DashMap::new());
        let worker_invalid_shares = Arc::new(DashMap::new());
        let worker_duplicate_shares = Arc::new(DashMap::new());
        let worker_log_times = Arc::new(DashMap::new());
        let worker_submitted_shares = Arc::new(DashMap::new());

        // Load initial share counts from the database
        if let Ok(sums) = db.get_share_counts(None).await {
            for (address, count) in sums {
                worker_share_counts.insert(address.clone(), Arc::new(AtomicU64::new(count)));
                worker_invalid_shares.insert(address.clone(), Arc::new(AtomicU64::new(0)));
                worker_duplicate_shares.insert(address.clone(), Arc::new(AtomicU64::new(0)));
                worker_submitted_shares.insert(address, Arc::new(tokio::sync::RwLock::new(HashSet::new())));
            }
        }

        // Spawn batch processing task
        let db_clone = db.clone();
        let total_shares_clone = total_shares.clone();
        let worker_share_counts_clone = worker_share_counts.clone();
        let worker_log_times_clone = worker_log_times.clone();

        tokio::spawn(async move {
            let mut batch = Vec::<Contribution>::new();
            let mut window_submission_counts: HashMap<String, u64> = HashMap::new();
            // Initialize window_submission_counts from database
            if let Ok(counts) = db_clone.get_share_counts(Some(window_time_ms / 1000)).await {
                for (address, count) in counts {
                    window_submission_counts.insert(address, count);
                }
            }
            loop {
                tokio::select! {
                    Some(share) = share_batch_rx.recv() => {
                        batch.push(share.clone());
                        let share_count = worker_share_counts_clone
                            .entry(share.address.clone())
                            .or_insert(Arc::new(AtomicU64::new(0)))
                            .clone();
                        let count = share_count.fetch_add(1, AtomicOrdering::Relaxed) + 1;
                        *window_submission_counts.entry(share.address.clone()).or_insert(0) += 1;
                        let current_time = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Time went backwards")
                            .as_secs();
                        let last_log_time = worker_log_times_clone
                            .entry(share.address.clone())
                            .or_insert(Arc::new(AtomicU64::new(0)))
                            .load(AtomicOrdering::Relaxed);
                        if count % 500 == 0 || current_time >= last_log_time + 30 {
                            info!(target: "sharehandler",
                                "Recording batched share: worker={}, total_submissions={}",
                                share.address, count
                            );
                            worker_log_times_clone.get(&share.address).unwrap().store(current_time, AtomicOrdering::Relaxed);
                        }
                        if batch.len() >= 500 {
                            Self::process_batch(
                                &db_clone,
                                &total_shares_clone,
                                &mut window_submission_counts,
                                &mut batch,
                                window_time_ms,
                            ).await;
                        }
                    }
                    _ = tokio::time::sleep(tokio::time::Duration::from_millis(10)) => {
                        if !batch.is_empty() {
                            Self::process_batch(
                                &db_clone,
                                &total_shares_clone,
                                &mut window_submission_counts,
                                &mut batch,
                                window_time_ms,
                            ).await;
                        }
                    }
                }
            }
        });

        Ok(Sharehandler {
            db,
            share_batch,
            worker_share_counts,
            worker_invalid_shares,
            worker_duplicate_shares,
            worker_log_times,
            worker_submitted_shares,
            pool_fee,
            jobs,
        })
    }

    async fn process_batch(
        db: &Arc<Db>,
        total_shares: &Arc<AtomicU64>,
        window_submission_counts: &mut HashMap<String, u64>,
        batch: &mut Vec<Contribution>,
        window_time_ms: u64,
    ) {
        let mut valid_shares = HashMap::new();
        let mut failed_shares = HashMap::new();

        let current_time_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;

        for share in batch.drain(..) {
            // Validate timestamp to ensure share is within the acceptable time window
            if current_time_ms < (share.timestamp as u64) * 1000 || current_time_ms - (share.timestamp as u64) * 1000 > window_time_ms {
                info!(target: "sharehandler",
                    "Skipping invalid share timestamp for worker={}: timestamp={}",
                    share.address, share.timestamp
                );
                *failed_shares.entry(share.address.clone()).or_insert(0) += 1;
                continue;
            }

            // Record share to database
            if let Err(e) = db.record_share(
                &share.address,
                share.difficulty,
                share.timestamp as u64,
                &share.job_id,
                share.daa_score as u64,
                &share.extranonce,
                &share.nonce,
            ).await {
                info!(target: "sharehandler",
                    "Failed to record batched share for worker={}: {:?}", share.address, e
                );
                *failed_shares.entry(share.address.clone()).or_insert(0) += 1;
            } else {
                *valid_shares.entry(share.address.clone()).or_insert(0) += 1;
                total_shares.fetch_add(1, AtomicOrdering::Relaxed);
                *window_submission_counts.entry(share.address.clone()).or_insert(0) += 1;
            }
        }

        for (address, count) in valid_shares {
            info!(target: "sharehandler", "Recorded {} batched shares for worker={}", count, address);
        }

        debug!(target: "sharehandler",
            "Processed batch: total_shares={}, window_submission_counts={:?}",
            total_shares.load(AtomicOrdering::Relaxed), window_submission_counts
        );
    }

    pub async fn record_share(&self, contribution: &Contribution, miner_difficulty: u64, is_valid: bool, is_duplicate: bool) -> Result<()> {
        // Check if job_id exists
        if contribution.job_id.parse::<u8>().is_ok() {
            let job_id_u8 = contribution.job_id.parse::<u8>().unwrap();
            if self.jobs.get_job(job_id_u8).await.is_none() {
                warn!(target: "sharehandler",
                    "Invalid job_id for worker={}: job_id={}", contribution.address, contribution.job_id
                );
                return Err(anyhow::anyhow!("Invalid job_id: {}", contribution.job_id));
            }
        } else {
            warn!(target: "sharehandler",
                "Invalid job_id format for worker={}: job_id={}", contribution.address, contribution.job_id
            );
            return Err(anyhow::anyhow!("Invalid job_id format: {}", contribution.job_id));
        }

        let network_difficulty = match contribution.job_id.parse::<u8>() {
            Ok(job_id_u8) => self.jobs
                .get_job_params(job_id_u8)
                .await
                .map(|params| params.difficulty())
                .unwrap_or(miner_difficulty),
            Err(_) => {
                warn!(target: "sharehandler",
                    "Invalid job_id format for worker={}: job_id={}", contribution.address, contribution.job_id
                );
                miner_difficulty
            }
        };

        debug!(target: "sharehandler",
            "Validating share for worker={}: miner_difficulty={}, contribution_difficulty={}, network_difficulty={}, job_id={}",
            contribution.address, miner_difficulty, contribution.difficulty, network_difficulty, contribution.job_id
        );

        // Check for duplicate shares using reward_block_hash and nonce
        let block_hash = match contribution.reward_block_hash.as_ref() {
            Some(hash) => hash.clone(),
            None => {
                warn!(target: "sharehandler",
                    "Missing reward_block_hash for worker={}: job_id={}, extranonce={}, nonce={}",
                    contribution.address, contribution.job_id, contribution.extranonce, contribution.nonce
                );
                return Err(anyhow::anyhow!("Missing reward_block_hash"));
            }
        };
        let share_key = (block_hash, contribution.nonce.clone());

        let submitted_shares_entry = self.worker_submitted_shares
            .entry(contribution.address.clone())
            .or_insert(Arc::new(tokio::sync::RwLock::new(HashSet::new())))
            .clone();

        let mut submitted_shares = submitted_shares_entry.write().await;

        if submitted_shares.contains(&share_key) || is_duplicate {
            self.worker_duplicate_shares
                .entry(contribution.address.clone())
                .or_insert(Arc::new(AtomicU64::new(0)))
                .fetch_add(1, AtomicOrdering::Relaxed);
            warn!(target: "sharehandler",
                "Duplicate share detected for address={}: block_hash={}, nonce={}",
                contribution.address, share_key.0, share_key.1
            );
            return Err(anyhow::anyhow!("Duplicate share"));
        }
        if !is_valid {
            self.worker_invalid_shares
                .entry(contribution.address.clone())
                .or_insert(Arc::new(AtomicU64::new(0)))
                .fetch_add(1, AtomicOrdering::Relaxed);
            warn!(target: "sharehandler",
                "Invalid share for address={}: difficulty={}", contribution.address, contribution.difficulty
            );
            return Err(anyhow::anyhow!("Invalid share"));
        }

        submitted_shares.insert(share_key);

        // Record share to database
        if let Err(e) = self.db.record_share(
            &contribution.address,
            contribution.difficulty,
            contribution.timestamp as u64,
            &contribution.job_id,
            contribution.daa_score as u64,
            &contribution.extranonce,
            &contribution.nonce,
        ).await {
            info!(target: "sharehandler",
                "Failed to record share for worker={}: {:?}", contribution.address, e
            );
            return Err(e);
        }

        // Update share counters
        let share_count = self.worker_share_counts
            .entry(contribution.address.clone())
            .or_insert(Arc::new(AtomicU64::new(0)))
            .clone();
        let count = share_count.fetch_add(1, AtomicOrdering::Relaxed) + 1;

        // Log share information periodically
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        let last_log_time = self.worker_log_times
            .entry(contribution.address.clone())
            .or_insert(Arc::new(AtomicU64::new(0)))
            .load(AtomicOrdering::Relaxed);
        if count % 500 == 0 || current_time >= last_log_time + 30 {
            let invalid_shares = self.worker_invalid_shares
                .get(&contribution.address)
                .map(|c| c.load(AtomicOrdering::Relaxed))
                .unwrap_or(0);
            let total_shares = count + invalid_shares;
            let rejection_rate = if total_shares > 0 {
                (invalid_shares as f64 / total_shares as f64) * 100.0
            } else {
                0.0
            };
            info!(target: "sharehandler",
                "{}",
                json!({
                    "event": "share_recorded",
                    "worker": contribution.address,
                    "total_submissions": count,
                    "rejection_rate_percent": format!("{:.2}", rejection_rate),
                    "pool_fee_percent": self.pool_fee,
                    "network_difficulty": network_difficulty
                })
            );
            self.worker_log_times.get(&contribution.address).unwrap().store(current_time, AtomicOrdering::Relaxed);
        }

        // Send share to batch processing
        if let Err(e) = self.share_batch.send(contribution.clone()).await {
            info!(target: "sharehandler",
                "Failed to send share to batch for worker={}: {:?}", contribution.address, e
            );
        }

        info!(target: "sharehandler", "Recorded share for worker={}", contribution.address);
        Ok(())
    }
}