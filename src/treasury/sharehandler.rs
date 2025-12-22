//src/treasury/sharehandler.rs

use anyhow::{Context, Result};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use log::{info, debug};
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
    pub pool_difficulty: u64,
}

#[derive(Debug)]
pub struct Sharehandler {
    pub db: Arc<Db>,
    pub share_batch: mpsc::Sender<Contribution>,
    pub worker_share_counts: Arc<DashMap<String, Arc<AtomicU64>>>,
    pub last_periodic_log: Arc<AtomicU64>,
    pub pool_fee: f64,
    pub jobs: Arc<Jobs>,
    pub is_synced: Arc<AtomicBool>,
}

impl Sharehandler {
    pub async fn new(
        db: Arc<Db>,
        pool_fee: f64,
        jobs: Arc<Jobs>,
        window_time_ms: u64,
        is_synced: Arc<AtomicBool>,
    ) -> Result<Self> {
        let total_shares = db.get_total_shares(window_time_ms / 1000).await.context("Failed to load total shares")?;
        let total_shares = Arc::new(AtomicU64::new(total_shares));
        let (share_batch, mut share_batch_rx) = mpsc::channel::<Contribution>(500);

        let worker_share_counts = Arc::new(DashMap::new());
        let last_periodic_log = Arc::new(AtomicU64::new(0));

        if let Ok(sums) = db.get_share_counts(None).await {
            for (address, count) in sums {
                worker_share_counts.insert(address, Arc::new(AtomicU64::new(count)));
            }
        }

        let db_clone = db.clone();
        let total_shares_clone = total_shares.clone();

        tokio::spawn(async move {
            let mut batch = Vec::new();

            loop {
                tokio::select! {
                    Some(share) = share_batch_rx.recv() => {
                        batch.push(share);

                        if batch.len() >= 500 {
                            Self::flush_batch(&db_clone, &total_shares_clone, &mut batch, window_time_ms).await;
                        }
                    }

                    _ = tokio::time::sleep(tokio::time::Duration::from_millis(20)) => {
                        if !batch.is_empty() {
                            Self::flush_batch(&db_clone, &total_shares_clone, &mut batch, window_time_ms).await;
                        }
                    }
                }
            }
        });

        Ok(Self {
            db,
            share_batch,
            worker_share_counts,
            last_periodic_log,
            pool_fee,
            jobs,
            is_synced,
        })
    }

    async fn flush_batch(
        db: &Db,
        total_shares: &Arc<AtomicU64>,
        batch: &mut Vec<Contribution>,
        window_time_ms: u64,
    ) {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        for share in batch.drain(..) {
            let share_time_ms = (share.timestamp as u64) * 1000;
            if now_ms < share_time_ms || now_ms - share_time_ms > window_time_ms {
                continue;
            }

            if db.record_share(
                &share.address,
                share.difficulty,
                share.timestamp as u64,
                &share.job_id,
                share.daa_score as u64,
                &share.extranonce,
                &share.nonce,
            ).await.is_ok() {
                total_shares.fetch_add(1, AtomicOrdering::Relaxed);
            }
        }
    }

    pub async fn record_share(
        &self,
        contribution: &Contribution,
        _miner_difficulty: u64,
        is_valid: bool,
    ) -> Result<()> {
        if !self.is_synced.load(AtomicOrdering::Relaxed) {
            debug!(
                "Share ignored (node not synced): {} | diff={} | block={}",
                contribution.address,
                contribution.difficulty,
                contribution.reward_block_hash.is_some()
            );
            return Ok(());
        }
        if let Ok(job_id) = contribution.job_id.parse::<u8>() {
            if self.jobs.get_job(job_id).await.is_none() {
                debug!("Ignoring share for stale job {}", job_id);
                return Ok(());
            }
        }

        if !is_valid {
            return Ok(());
        }

        let db_result = self.db.record_share(
            &contribution.address,
            contribution.difficulty,
            contribution.timestamp as u64,
            &contribution.job_id,
            contribution.daa_score as u64,
            &contribution.extranonce,
            &contribution.nonce,
        ).await;

        if db_result.is_err() {
            return db_result.map_err(|e| e);
        }
        let is_block = contribution.reward_block_hash.is_some();
        info!(
            target: "sharehandler",
            "SHARE ACCEPTED → {} | diff: {} | pool_diff: {} | job: {} | {} (recorded to DB)",
            contribution.address,
            contribution.difficulty,
            contribution.pool_difficulty,
            contribution.job_id,
            if is_block { "BLOCK FOUND!" } else { "share" }
        );

        let counter = self.worker_share_counts
            .entry(contribution.address.clone())
            .or_insert(Arc::new(AtomicU64::new(0)))
            .clone();
        let count = counter.fetch_add(1, AtomicOrdering::Relaxed) + 1;
        if count % 1000 == 0 {
            info!(target: "sharehandler",
                "{}",
                json!({
                    "event": "shares_milestone",
                    "worker": contribution.address,
                    "shares": count,
                    "last_difficulty": contribution.difficulty,
                    "pool_fee_percent": self.pool_fee
                })
            );
        }

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let last_log = self.last_periodic_log.load(AtomicOrdering::Relaxed);

        if now >= last_log + 60 {
            let mut miner_stats = Vec::new();
            for entry in self.worker_share_counts.iter() {
                let addr = entry.key();
                let shares = entry.value().load(AtomicOrdering::Relaxed);
                if shares > 0 {
                    miner_stats.push((addr.clone(), shares));
                }
            }

            miner_stats.sort_by(|a, b| b.1.cmp(&a.1));

            info!(target: "sharehandler", "=== MINER SHARE STATUS ({} active) ===", miner_stats.len());
            for (addr, shares) in miner_stats {
                info!(target: "sharehandler", "  • {} → {} shares", addr, shares);
            }
            info!(target: "sharehandler", "======================================");

            self.last_periodic_log.store(now, AtomicOrdering::Relaxed);
        }

        let _ = self.share_batch.send(contribution.clone()).await;

        Ok(())
    }
}