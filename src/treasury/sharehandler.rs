//src/treasury/sharehandler.rs

use anyhow::{Context, Result};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, RwLock};
use log::{debug, info};
use crate::database::db::Db;
use crate::metrics::{TOTAL_SHARES_RECORDED, SHARE_WINDOW_SIZE, SHARE_PROCESSING_FAILED};
use crate::stratum::protocol::StratumConn;

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
    pub worker_log_times: Arc<DashMap<String, Arc<AtomicU64>>>,
    pub worker_share_rates: Arc<DashMap<String, Arc<RwLock<VecDeque<(u64, u64)>>>>>,
    pub worker_last_difficulty_check: Arc<DashMap<String, Arc<AtomicU64>>>,
    pub pool_fee: f64,
}

impl Sharehandler {
    pub async fn new(db: Arc<Db>, n_window: usize, window_time_ms: u64, _mining_addr: String, pool_fee: f64) -> Result<Self> {
        let mut share_window = VecDeque::new();
        let total_shares = db.load_recent_shares(&mut share_window, n_window)
            .await
            .context("Failed to load recent shares")?;
        let share_window = Arc::new(RwLock::new(share_window));
        let total_shares = Arc::new(AtomicU64::new(total_shares));
        let (share_batch, mut share_batch_rx) = mpsc::channel::<Contribution>(10000);
        let worker_share_counts = Arc::new(DashMap::new());
        let worker_log_times = Arc::new(DashMap::new());
        let worker_share_rates = Arc::new(DashMap::new());
        let worker_last_difficulty_check = Arc::new(DashMap::new());

        if let Ok(sums) = db.get_share_counts(None).await {
            for entry in sums.iter() {
                worker_share_counts.insert(entry.key().clone(), Arc::new(AtomicU64::new(*entry.value())));
            }
        }

        debug!("Initialized sharehandler: n_window={n_window}, window_time_ms={window_time_ms}, pool_fee={pool_fee}");

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
                        if count % 100 == 0 || current_time >= last_log_time + 10 {
                            info!("Recording share: worker={}, total_submissions={}", share.address, count);
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
                    _ = tokio::time::sleep(tokio::time::Duration::from_millis(50)) => {
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
            worker_log_times,
            worker_share_rates,
            worker_last_difficulty_check,
            pool_fee,
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
        let current_time_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;
        for share in batch.drain(..) {
            if current_time_ms < (share.timestamp as u64) * 1000 || current_time_ms - (share.timestamp as u64) * 1000 > window_time_ms {
                info!("Skipping invalid share timestamp for worker={}: timestamp={}", share.address, share.timestamp);
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
                info!("Failed to record batched share for worker={}: {:?}", share.address, e);
                SHARE_PROCESSING_FAILED.with_label_values(&[&share.address]).inc();
            } else {
                TOTAL_SHARES_RECORDED.with_label_values(&[&share.address]).inc();
                info!("Recorded batched share for worker={}", share.address);
            }
            let mut share_window = share_window.write().await;
            share_window.push_back(share.clone());
            total_shares.fetch_add(1, AtomicOrdering::Relaxed);
            while share_window.len() > n_window {
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
            while share_window.len() > 0 && current_time_ms - share_window.front().map(|s| (s.timestamp as u64) * 1000).unwrap_or(0) > window_time_ms {
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
            debug!("Share window size: {}, oldest timestamp: {:?}", share_window.len(), share_window.front().map(|s| s.timestamp));
            SHARE_WINDOW_SIZE.set(share_window.len() as f64);
        }
    }

    pub async fn record_share(&self, contribution: &Contribution, miner_difficulty: u64) -> Result<()> {
        let share_count = self.worker_share_counts
            .entry(contribution.address.clone())
            .or_insert(Arc::new(AtomicU64::new(0)))
            .clone();
        let count = share_count.fetch_add(1, AtomicOrdering::Relaxed) + 1;

        let current_time_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;
        let share_rates = self.worker_share_rates
            .entry(contribution.address.clone())
            .or_insert(Arc::new(RwLock::new(VecDeque::new())))
            .clone();
        let mut rates = share_rates.write().await;
        rates.push_back((current_time_ms, miner_difficulty));
        while rates.len() > 0 && current_time_ms - rates.front().map(|(ts, _)| ts).unwrap_or(&0) > 30_000 {
            rates.pop_front();
        }

        let mut share_window = self.share_window.write().await;
        share_window.push_back(contribution.clone());
        self.total_shares.fetch_add(1, AtomicOrdering::Relaxed);

        let mut window_submission_counts: HashMap<String, u64> = HashMap::new();
        for share in share_window.iter() {
            *window_submission_counts.entry(share.address.clone()).or_insert(0) += 1;
        }

        while share_window.len() > self.n_window {
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
        while share_window.len() > 0 && current_time_ms - share_window.front().map(|s| (s.timestamp as u64) * 1000).unwrap_or(0) > self.window_time_ms {
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
        debug!("Share window size: {}, window_submissions: {:?}", share_window.len(), window_submission_counts.get(&contribution.address));
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
            let recent_shares = rates.len() as f64;
            let share_rate = recent_shares / 30.0;
            let average_difficulty = if recent_shares > 0.0 {
                rates.iter().map(|(_, diff)| *diff as f64).sum::<f64>() / recent_shares
            } else {
                miner_difficulty as f64
            };
            let effective_hashrate = (share_rate * average_difficulty) / 1_000_000.0;
            info!(
                "Recording share: worker={}, total_submissions={}, share_rate={:.2} shares/s, effective_hashrate={:.2} Mhash/s, average_difficulty={:.0}",
                contribution.address, count, share_rate, effective_hashrate, average_difficulty
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
            info!("Failed to record share for worker={}: {:?}", contribution.address, e);
            SHARE_PROCESSING_FAILED.with_label_values(&[&contribution.address]).inc();
            Err(e)
        } else {
            TOTAL_SHARES_RECORDED.with_label_values(&[&contribution.address]).inc();
            info!("Recorded share for worker={}", contribution.address);
            if let Err(e) = self.share_batch.send(contribution.clone()).await {
                info!("Failed to send share to batch for worker={}: {:?}", contribution.address, e);
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
            "Share counts: total={}, window={} for address={}",
            total_submissions, window_submissions, address
        );
        Ok((total_submissions, window_submissions))
    }

    pub async fn get_dynamic_difficulty(&self, address: &str, base_difficulty: u64) -> u64 {
        const ADJUSTMENT_FACTOR: f64 = 0.2;
        const MAX_SHARE_RATE: f64 = 5.0;
        const MIN_SHARE_RATE: f64 = 1.0;
        const DIFFICULTY_CHECK_INTERVAL_MS: u64 = 10_000;

        let mut target_difficulty = base_difficulty;

        let current_time_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;
        let last_check = self.worker_last_difficulty_check
            .entry(address.to_string())
            .or_insert(Arc::new(AtomicU64::new(0)))
            .load(AtomicOrdering::Relaxed);
        if current_time_ms - last_check > DIFFICULTY_CHECK_INTERVAL_MS {
            let share_rates = self.worker_share_rates
                .get(address)
                .map(|entry| entry.clone())
                .unwrap_or_else(|| Arc::new(RwLock::new(VecDeque::new())));
            let rates = share_rates.read().await;
            let recent_shares = rates.iter().filter(|(ts, _)| current_time_ms - *ts <= 30_000).count() as f64;
            let average_difficulty = if recent_shares > 0.0 {
                rates.iter().filter(|(ts, _)| current_time_ms - *ts <= 30_000)
                    .map(|(_, diff)| *diff as f64)
                    .sum::<f64>() / recent_shares
            } else {
                base_difficulty as f64
            };
            if average_difficulty > 0.0 && base_difficulty as f64 > average_difficulty * 1.1 {
                target_difficulty = base_difficulty;
                debug!("Resetting difficulty for {} to network base_difficulty={} due to network increase", address, base_difficulty);
                self.worker_last_difficulty_check.get(address).unwrap().store(current_time_ms, AtomicOrdering::Relaxed);
            }
        }

        let share_rates = self.worker_share_rates
            .get(address)
            .map(|entry| entry.clone())
            .unwrap_or_else(|| Arc::new(RwLock::new(VecDeque::new())));
        let rates = share_rates.read().await;
        let recent_shares = rates.iter().filter(|(ts, _)| current_time_ms - *ts <= 30_000).count() as f64;
        let share_rate = recent_shares / 30.0;

        if share_rate > MAX_SHARE_RATE {
            target_difficulty = ((target_difficulty as f64) * (1.0 + ADJUSTMENT_FACTOR)) as u64;
            debug!("Increasing difficulty for {}: share_rate={:.2} shares/s, new_difficulty={}", address, share_rate, target_difficulty);
        } else if share_rate < MIN_SHARE_RATE && target_difficulty > base_difficulty {
            target_difficulty = ((target_difficulty as f64) * (1.0 - ADJUSTMENT_FACTOR)).max(base_difficulty as f64) as u64;
            debug!("Decreasing difficulty for {}: share_rate={:.2} shares/s, new_difficulty={}", address, share_rate, target_difficulty);
        }

        let final_difficulty = target_difficulty;

        debug!(
            "Dynamic difficulty for {}: base_difficulty={}, share_rate={:.2}, final_difficulty={}",
            address, base_difficulty, share_rate, final_difficulty
        );
        final_difficulty
    }

    pub async fn get_balances_and_hashrate(&self, address: &str, stratum_conn: &StratumConn<'_>) -> Result<(u64, u64, f64)> {
        let available_balance = sqlx::query_scalar::<_, i64>("SELECT available_balance FROM balances WHERE address = ?")
            .bind(address)
            .fetch_optional(&self.db.pool)
            .await
            .context("Failed to fetch available balance")?
            .unwrap_or(0) as u64;

        let unconfirmed_blocks = self.db.get_unconfirmed_blocks().await
            .context("Failed to get unconfirmed blocks")?;

        let mut pending_balance = 0;

        for block in unconfirmed_blocks {
            let share_counts = self.db.get_shares_in_window(block.daa_score as u64, 1000).await
                .context("Failed to get share counts for reward distribution")?;
            let total_shares: u64 = share_counts.iter().map(|entry| *entry.value()).sum();
            if total_shares == 0 {
                continue;
            }

            let miner_shares = share_counts.get(address).map(|entry| *entry.value()).unwrap_or(0);
            let share_percentage = miner_shares as f64 / total_shares as f64;
            let block_amount = block.amount as u64;
            let miner_reward = ((block_amount as f64) * share_percentage) as u64;
            pending_balance += miner_reward;
        }

        let share_rates = self.worker_share_rates
            .get(address)
            .map(|entry| entry.clone())
            .unwrap_or_else(|| Arc::new(RwLock::new(VecDeque::new())));
        let rates = share_rates.read().await;
        let current_time_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;
        let recent_shares = rates.iter().filter(|(ts, _)| current_time_ms - *ts <= 30_000).count() as f64;
        let average_difficulty = if recent_shares > 0.0 {
            rates.iter().filter(|(ts, _)| current_time_ms - *ts <= 30_000)
                .map(|(_, diff)| *diff as f64)
                .sum::<f64>() / recent_shares
        } else {
            stratum_conn.difficulty as f64
        };
        let hashrate = (recent_shares / 30.0) * average_difficulty / 1_000_000.0;

        debug!(
            "Balances for address={}: available={} VE, pending={} VE, effective_hashrate={:.2} Mhash/s, recent_shares={}, average_difficulty={:.0}",
            address, available_balance as f64 / 100_000_000.0, pending_balance as f64 / 100_000_000.0, hashrate, recent_shares, average_difficulty
        );
        Ok((available_balance, pending_balance, hashrate))
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