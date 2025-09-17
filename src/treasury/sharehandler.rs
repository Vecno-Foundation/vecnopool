//src/treasury/sharehandler.rs

use anyhow::{Context, Result};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, VecDeque, HashSet};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, RwLock};
use log::{debug, info, warn};
use crate::database::db::Db;
use crate::metrics::{
    TOTAL_SHARES_RECORDED, SHARE_WINDOW_SIZE, SHARE_PROCESSING_FAILED,
    MINER_INVALID_SHARES, MINER_DUPLICATED_SHARES,
};
use crate::stratum::jobs::{Jobs};

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
    pub share_window: Arc<RwLock<VecDeque<Contribution>>>,
    pub total_shares: Arc<AtomicU64>,
    pub n_window: usize,
    pub window_time_ms: u64,
    pub share_batch: mpsc::Sender<Contribution>,
    pub worker_share_counts: Arc<DashMap<String, Arc<AtomicU64>>>,
    pub worker_invalid_shares: Arc<DashMap<String, Arc<AtomicU64>>>,
    pub worker_duplicate_shares: Arc<DashMap<String, Arc<AtomicU64>>>,
    pub worker_log_times: Arc<DashMap<String, Arc<AtomicU64>>>,
    pub worker_submitted_shares: Arc<DashMap<String, Arc<RwLock<HashSet<(String, String)>>>>>,
    pub pool_fee: f64,
    pub jobs: Arc<Jobs>,
}

impl Sharehandler {
    pub async fn new(
        db: Arc<Db>,
        n_window: usize,
        window_time_ms: u64,
        _mining_addr: String,
        pool_fee: f64,
        jobs: Arc<Jobs>,
    ) -> Result<Self> {
        let mut share_window = VecDeque::new();
        let total_shares = db.load_recent_shares(&mut share_window, n_window)
            .await
            .context("Failed to load recent shares")?;
        let share_window = Arc::new(RwLock::new(share_window));
        let total_shares = Arc::new(AtomicU64::new(total_shares));
        let (share_batch, mut share_batch_rx) = mpsc::channel::<Contribution>(500);
        let worker_share_counts = Arc::new(DashMap::new());
        let worker_invalid_shares = Arc::new(DashMap::new());
        let worker_duplicate_shares = Arc::new(DashMap::new());
        let worker_log_times = Arc::new(DashMap::new());
        let worker_submitted_shares = Arc::new(DashMap::new());

        if let Ok(sums) = db.get_share_counts(None).await {
            for entry in sums.iter() {
                worker_share_counts.insert(entry.key().clone(), Arc::new(AtomicU64::new(*entry.value())));
                worker_invalid_shares.insert(entry.key().clone(), Arc::new(AtomicU64::new(0)));
                worker_duplicate_shares.insert(entry.key().clone(), Arc::new(AtomicU64::new(0)));
                worker_submitted_shares.insert(entry.key().clone(), Arc::new(RwLock::new(HashSet::new())));
            }
        }

        debug!(target: "sharehandler",
            "Initialized sharehandler: n_window={}, window_time_ms={}, pool_fee={}",
            n_window, window_time_ms, pool_fee
        );

        let db_clone = db.clone();
        let share_window_clone = share_window.clone();
        let total_shares_clone = total_shares.clone();
        let worker_share_counts_clone = worker_share_counts.clone();
        let worker_log_times_clone = worker_log_times.clone();

        tokio::spawn(async move {
            let mut batch = Vec::<Contribution>::new();
            let mut window_submission_counts: HashMap<String, u64> = HashMap::new();
            {
                let window = share_window_clone.read().await;
                SHARE_WINDOW_SIZE.set(window.len() as f64);
                for share in window.iter() {
                    *window_submission_counts.entry(share.address.clone()).or_insert(0) += 1;
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
                                &share_window_clone,
                                &total_shares_clone,
                                &mut window_submission_counts,
                                &mut batch,
                                n_window,
                                window_time_ms,
                            ).await;
                        }
                    }
                    _ = tokio::time::sleep(tokio::time::Duration::from_millis(10)) => {
                        if !batch.is_empty() {
                            Self::process_batch(
                                &db_clone,
                                &share_window_clone,
                                &total_shares_clone,
                                &mut window_submission_counts,
                                &mut batch,
                                n_window,
                                window_time_ms,
                            ).await;
                        }
                    }
                }
            }
        });

        Ok(Sharehandler {
            db,
            share_window,
            total_shares,
            n_window,
            window_time_ms,
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
        share_window: &Arc<RwLock<VecDeque<Contribution>>>,
        total_shares: &Arc<AtomicU64>,
        window_submission_counts: &mut HashMap<String, u64>,
        batch: &mut Vec<Contribution>,
        n_window: usize,
        window_time_ms: u64,
    ) {
        let mut valid_shares = HashMap::new();
        let mut failed_shares = HashMap::new();

        let current_time_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;

        let mut share_window = share_window.write().await;
        for share in batch.drain(..) {
            if current_time_ms < (share.timestamp as u64) * 1000 || current_time_ms - (share.timestamp as u64) * 1000 > window_time_ms {
                info!(target: "sharehandler",
                    "Skipping invalid share timestamp for worker={}: timestamp={}",
                    share.address, share.timestamp
                );
                *failed_shares.entry(share.address.clone()).or_insert(0) += 1;
                continue;
            }

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
                share_window.push_back(share.clone());
                total_shares.fetch_add(1, AtomicOrdering::Relaxed);
                *window_submission_counts.entry(share.address.clone()).or_insert(0) += 1;
            }
        }

        for (address, count) in valid_shares {
            TOTAL_SHARES_RECORDED.with_label_values(&[&address]).inc_by(count as f64);
            info!(target: "sharehandler", "Recorded {} batched shares for worker={}", count, address);
        }
        for (address, count) in failed_shares {
            SHARE_PROCESSING_FAILED.with_label_values(&[&address]).inc_by(count as f64);
        }

        while share_window.len() > n_window || (share_window.len() > 0 && current_time_ms - share_window.front().map(|s| (s.timestamp as u64) * 1000).unwrap_or(0) > window_time_ms) {
            if let Some(old_share) = share_window.pop_front() {
                total_shares.fetch_sub(1, AtomicOrdering::Relaxed);
                if let Some(count) = window_submission_counts.get_mut(&old_share.address) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        window_submission_counts.remove(&old_share.address);
                    }
                }
            }
        }

        debug!(target: "sharehandler",
            "Share window size: {}, oldest timestamp: {:?}", share_window.len(), share_window.front().map(|s| s.timestamp)
        );
        SHARE_WINDOW_SIZE.set(share_window.len() as f64);
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
            .or_insert(Arc::new(RwLock::new(HashSet::new())))
            .clone();

        let (mut share_window, mut submitted_shares) = tokio::join!(
            self.share_window.write(),
            submitted_shares_entry.write()
        );

        if submitted_shares.contains(&share_key) || is_duplicate {
            self.worker_duplicate_shares
                .entry(contribution.address.clone())
                .or_insert(Arc::new(AtomicU64::new(0)))
                .fetch_add(1, AtomicOrdering::Relaxed);
            MINER_DUPLICATED_SHARES.with_label_values(&[&contribution.address]).inc();
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
            MINER_INVALID_SHARES.with_label_values(&[&contribution.address]).inc();
            warn!(target: "sharehandler",
                "Invalid share for address={}: difficulty={}", contribution.address, contribution.difficulty
            );
            return Err(anyhow::anyhow!("Invalid share"));
        }

        submitted_shares.insert(share_key);

        let share_count = self.worker_share_counts
            .entry(contribution.address.clone())
            .or_insert(Arc::new(AtomicU64::new(0)))
            .clone();
        let count = share_count.fetch_add(1, AtomicOrdering::Relaxed) + 1;

        let current_time_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;

        share_window.push_back(contribution.clone());
        self.total_shares.fetch_add(1, AtomicOrdering::Relaxed);

        let mut window_submission_counts: HashMap<String, u64> = HashMap::new();
        for share in share_window.iter() {
            *window_submission_counts.entry(share.address.clone()).or_insert(0) += 1;
        }

        while share_window.len() > self.n_window || (share_window.len() > 0 && current_time_ms - share_window.front().map(|s| (s.timestamp as u64) * 1000).unwrap_or(0) > self.window_time_ms) {
            if let Some(old_share) = share_window.pop_front() {
                self.total_shares.fetch_sub(1, AtomicOrdering::Relaxed);
                if let Some(count) = window_submission_counts.get_mut(&old_share.address) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        window_submission_counts.remove(&old_share.address);
                    }
                }
            }
        }

        debug!(target: "sharehandler",
            "Share window size: {}, window_submissions: {:?}", share_window.len(), window_submission_counts.get(&contribution.address)
        );
        SHARE_WINDOW_SIZE.set(share_window.len() as f64);

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
            SHARE_PROCESSING_FAILED.with_label_values(&[&contribution.address]).inc();
            Err(e)
        } else {
            TOTAL_SHARES_RECORDED.with_label_values(&[&contribution.address]).inc();
            info!(target: "sharehandler", "Recorded share for worker={}", contribution.address);
            if let Err(e) = self.share_batch.send(contribution.clone()).await {
                info!(target: "sharehandler",
                    "Failed to send share to batch for worker={}: {:?}", contribution.address, e
                );
                SHARE_PROCESSING_FAILED.with_label_values(&[&contribution.address]).inc();
            }
            Ok(())
        }
    }

    pub async fn get_share_counts(&self, address: &str) -> Result<(u64, u64)> {
        let share_window = self.share_window.read().await;
        let window_difficulty = share_window
            .iter()
            .filter(|share| share.address == address)
            .map(|share| share.difficulty as u64)
            .sum::<u64>();
        let total_submissions = self.worker_share_counts
            .get(address)
            .map(|count| count.load(AtomicOrdering::Relaxed))
            .unwrap_or(0);
        debug!(target: "sharehandler",
            "Share counts: total={}, window_difficulty={} for address={}",
            total_submissions, window_difficulty, address
        );
        Ok((total_submissions, window_difficulty))
    }

    pub async fn should_log_share(&self, address: &str, count: u64) -> bool {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        let last_log_time = self.worker_log_times
            .entry(address.to_string())
            .or_insert(Arc::new(AtomicU64::new(0)))
            .load(AtomicOrdering::Relaxed);
        count % 500 == 0 || current_time >= last_log_time + 30
    }

    pub async fn update_log_time(&self, address: &str) {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        if let Some(log_time) = self.worker_log_times.get(address) {
            log_time.store(current_time, AtomicOrdering::Relaxed);
        }
    }
}