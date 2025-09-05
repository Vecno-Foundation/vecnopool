use anyhow::{Context, Result};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, RwLock};
use log::{debug, info, warn};
use crate::database::db::Db;
use crate::metrics::{TOTAL_SHARES_RECORDED, SHARE_WINDOW_SIZE, SHARE_PROCESSING_FAILED};

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Contribution {
    pub address: String,
    pub difficulty: i64,
    pub timestamp: i64,
    pub job_id: String,
    pub daa_score: i64,
    pub extranonce: String,
    pub nonce: String,
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
    pub worker_log_times: Arc<DashMap<String, Arc<AtomicU64>>>,
}

impl Sharehandler {
    pub async fn new(db: Arc<Db>, n_window: usize, window_time_ms: u64, _mining_addr: String) -> Result<Self> {
        let mut share_window = VecDeque::new();
        let total_shares = db.load_recent_shares(&mut share_window, n_window)
            .await
            .context("Failed to load recent shares")?;
        let share_window = Arc::new(RwLock::new(share_window));
        let total_shares = Arc::new(AtomicU64::new(total_shares));
        let (share_batch, mut share_batch_rx) = mpsc::channel::<Contribution>(1000);
        let worker_share_counts = Arc::new(DashMap::new());
        let worker_log_times = Arc::new(DashMap::new());

        if let Ok(sums) = db.get_share_counts(None).await {
            for entry in sums.iter() {
                worker_share_counts.insert(entry.key().clone(), Arc::new(AtomicU64::new(*entry.value())));
            }
        }

        debug!("Initialized sharehandler: n_window={n_window}, window_time_ms={window_time_ms}");

        let db_clone = db.clone();
        let share_window_clone = share_window.clone();
        let total_shares_clone = total_shares.clone();
        let worker_share_counts_clone = worker_share_counts.clone();
        let worker_log_times_clone = worker_log_times.clone();
        let n_window = n_window;
        let window_time_ms = window_time_ms;

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
                        if count % 100 == 0 || current_time >= last_log_time + 10 {
                            info!("Recording share: worker={worker}, total_submissions={count}", worker = share.address);
                            worker_log_times_clone.get(&share.address).unwrap().store(current_time, AtomicOrdering::Relaxed);
                        }
                        if batch.len() >= 100 {
                            for share in batch.drain(..) {
                                if let Err(e) = db_clone.record_share(
                                    &share.address,
                                    1,
                                    share.timestamp as u64,
                                    &share.job_id,
                                    share.daa_score as u64,
                                    &share.extranonce,
                                    &share.nonce,
                                ).await {
                                    warn!("Failed to record batched share for worker={worker}: {e:?}", worker = share.address);
                                    SHARE_PROCESSING_FAILED.with_label_values(&[&share.address]).inc();
                                } else {
                                    TOTAL_SHARES_RECORDED.with_label_values(&[&share.address]).inc();
                                }
                                let mut w = share_window_clone.write().await;
                                w.push_back(share.clone());
                                while w.len() > n_window {
                                    if let Some(old_share) = w.pop_front() {
                                        total_shares_clone.fetch_sub(1, AtomicOrdering::Relaxed);
                                        if let Some(count) = window_submission_counts.get_mut(&old_share.address) {
                                            *count -= 1;
                                            if *count == 0 {
                                                window_submission_counts.remove(&old_share.address);
                                            }
                                        }
                                    }
                                }
                                let current_time_ms = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .expect("Time went backwards")
                                    .as_millis() as u64;
                                while w.len() > 0 && current_time_ms - w.front().map(|s| (s.timestamp as u64) * 1000).unwrap_or(0) > window_time_ms {
                                    if let Some(old_share) = w.pop_front() {
                                        total_shares_clone.fetch_sub(1, AtomicOrdering::Relaxed);
                                        if let Some(count) = window_submission_counts.get_mut(&old_share.address) {
                                            *count -= 1;
                                            if *count == 0 {
                                                window_submission_counts.remove(&old_share.address);
                                            }
                                        }
                                    }
                                }
                                SHARE_WINDOW_SIZE.set(w.len() as f64);
                            }
                        }
                    }
                    _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                        if !batch.is_empty() {
                            for share in batch.drain(..) {
                                if let Err(e) = db_clone.record_share(
                                    &share.address,
                                    1,
                                    share.timestamp as u64,
                                    &share.job_id,
                                    share.daa_score as u64,
                                    &share.extranonce,
                                    &share.nonce,
                                ).await {
                                    warn!("Failed to record batched share for worker={worker}: {e:?}", worker = share.address);
                                    SHARE_PROCESSING_FAILED.with_label_values(&[&share.address]).inc();
                                } else {
                                    TOTAL_SHARES_RECORDED.with_label_values(&[&share.address]).inc();
                                }
                                let mut w = share_window_clone.write().await;
                                w.push_back(share);
                                while w.len() > n_window {
                                    if let Some(old_share) = w.pop_front() {
                                        total_shares_clone.fetch_sub(1, AtomicOrdering::Relaxed);
                                        if let Some(count) = window_submission_counts.get_mut(&old_share.address) {
                                            *count -= 1;
                                            if *count == 0 {
                                                window_submission_counts.remove(&old_share.address);
                                            }
                                        }
                                    }
                                }
                                let current_time_ms = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .expect("Time went backwards")
                                    .as_millis() as u64;
                                while w.len() > 0 && current_time_ms - w.front().map(|s| (s.timestamp as u64) * 1000).unwrap_or(0) > window_time_ms {
                                    if let Some(old_share) = w.pop_front() {
                                        total_shares_clone.fetch_sub(1, AtomicOrdering::Relaxed);
                                        if let Some(count) = window_submission_counts.get_mut(&old_share.address) {
                                            *count -= 1;
                                            if *count == 0 {
                                                window_submission_counts.remove(&old_share.address);
                                            }
                                        }
                                    }
                                }
                                SHARE_WINDOW_SIZE.set(w.len() as f64);
                            }
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
            worker_log_times,
        })
    }

    pub async fn record_share(&self, contribution: &Contribution) -> Result<()> {
        let share_count = self.worker_share_counts
            .entry(contribution.address.clone())
            .or_insert(Arc::new(AtomicU64::new(0)))
            .clone();
        let count = share_count.fetch_add(1, AtomicOrdering::Relaxed) + 1;
        let mut share_window = self.share_window.write().await;
        share_window.push_back(contribution.clone());
        self.total_shares.fetch_add(1, AtomicOrdering::Relaxed);

        let current_time_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;
        let mut window_submission_counts: HashMap<String, u64> = HashMap::new();
        for share in share_window.iter() {
            *window_submission_counts.entry(share.address.clone()).or_insert(0) += 1;
        }

        while share_window.len() > self.n_window {
            if let Some(old_share) = share_window.pop_front() {
                self.total_shares.fetch_sub(1, AtomicOrdering::Relaxed);
                if let Some(count) = window_submission_counts.get_mut(&old_share.address) {
                    *count -= 1;
                    if *count == 0 {
                        window_submission_counts.remove(&old_share.address);
                    }
                }
            }
        }
        while share_window.len() > 0 && current_time_ms - share_window.front().map(|s| (s.timestamp as u64) * 1000).unwrap_or(0) > self.window_time_ms {
            if let Some(old_share) = share_window.pop_front() {
                self.total_shares.fetch_sub(1, AtomicOrdering::Relaxed);
                if let Some(count) = window_submission_counts.get_mut(&old_share.address) {
                    *count -= 1;
                    if *count == 0 {
                        window_submission_counts.remove(&old_share.address);
                    }
                }
            }
        }
        SHARE_WINDOW_SIZE.set(share_window.len() as f64);

        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        let last_log_time = self.worker_log_times
            .entry(contribution.address.clone())
            .or_insert(Arc::new(AtomicU64::new(0)))
            .load(AtomicOrdering::Relaxed);
        if count % 100 == 0 || current_time >= last_log_time + 10 {
            info!("Recording share: worker={worker}, total_submissions={count}", worker = contribution.address);
            self.worker_log_times.get(&contribution.address).unwrap().store(current_time, AtomicOrdering::Relaxed);
        }

        if let Err(e) = self.db.record_share(
            &contribution.address,
            1,
            contribution.timestamp as u64,
            &contribution.job_id,
            contribution.daa_score as u64,
            &contribution.extranonce,
            &contribution.nonce,
        ).await {
            warn!("Failed to record share for worker={worker}: {e:?}", worker = contribution.address);
            SHARE_PROCESSING_FAILED.with_label_values(&[&contribution.address]).inc();
            Err(e)
        } else {
            TOTAL_SHARES_RECORDED.with_label_values(&[&contribution.address]).inc();
            if let Err(e) = self.share_batch.send(contribution.clone()).await {
                warn!("Failed to send share to batch for worker={worker}: {e:?}", worker = contribution.address);
                SHARE_PROCESSING_FAILED.with_label_values(&[&contribution.address]).inc();
            }
            Ok(())
        }
    }

    pub async fn get_share_counts(&self, address: &str) -> Result<(u64, u64)> {
        let share_window = self.share_window.read().await;
        let window_submissions = share_window
            .iter()
            .filter(|share| share.address == address)
            .count() as u64;
        let total_submissions = self.worker_share_counts
            .get(address)
            .map(|count| count.load(AtomicOrdering::Relaxed))
            .unwrap_or(0);
        debug!(
            "Share counts: total={total_submissions}, window={window_submissions} for address={address}"
        );
        Ok((total_submissions, window_submissions))
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
        count % 100 == 0 || current_time >= last_log_time + 10
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