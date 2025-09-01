use anyhow::{Context, Result};
use dashmap::DashMap;
use rusqlite::params;
use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, RwLock};
use log::{debug, info, warn};
use rusqlite::OptionalExtension;
use tokio_retry::strategy::ExponentialBackoff;
use tokio_retry::Retry;
use reqwest::Client;
use serde_json::Value;
use serde::Deserialize;
use std::sync::atomic::Ordering;
use crate::database::db::Db;
use crate::stratum::jobs::Jobs;

pub mod db;

#[derive(Debug, Clone)]
pub struct Contribution {
    pub address: String,
    pub difficulty: u64,
    pub timestamp: u64,
    pub job_id: String,
    pub daa_score: u64,
    pub extranonce: String,
    pub nonce: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Payout {
    pub address: String,
    pub amount: String,
    #[serde(alias = "txid")]
    pub txId: String,
}

#[derive(Debug)]
pub struct StratumDb {
    pub db: Arc<Db>,
    pub balances: Arc<DashMap<String, u64>>,
    pub share_window: Arc<RwLock<VecDeque<Contribution>>>,
    pub total_shares: Arc<AtomicU64>,
    pub n_window: usize,
    pub window_time_ms: u64,
    pub share_batch: mpsc::Sender<Contribution>,
    pub worker_share_counts: Arc<DashMap<String, Arc<AtomicU64>>>,
    pub worker_log_times: Arc<DashMap<String, Arc<AtomicU64>>>,
    pub network_id: String,
}

impl StratumDb {
    pub async fn new(path: &Path, n_window: usize, window_time_ms: u64, network_id: String) -> Result<Self> {
        let db = Arc::new(Db::new(path).context("Failed to initialize DB")?);
        let balances = Arc::new(DashMap::new());
        db.load_balances(&balances).context("Failed to load balances")?;
        let mut share_window = VecDeque::new();
        let total_shares = db.load_recent_shares(&mut share_window, n_window)
            .context("Failed to load recent shares")?;
        let share_window = Arc::new(RwLock::new(share_window));
        let total_shares = Arc::new(AtomicU64::new(total_shares));
        let (share_batch, mut share_batch_rx) = mpsc::channel::<Contribution>(1000);
        let worker_share_counts = Arc::new(DashMap::new());
        let worker_log_times = Arc::new(DashMap::new());

        if let Ok(sums) = db.get_total_submissions_all() {
            for entry in sums.iter() {
                worker_share_counts.insert(entry.key().clone(), Arc::new(AtomicU64::new(*entry.value())));
            }
        }

        debug!("Initialized StratumDb with n_window={} and window_time_ms={}", n_window, window_time_ms);

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
                        let count = share_count.load(AtomicOrdering::Relaxed);
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
                            info!("Recording share for worker {}, total submissions: {}", share.address, count + 1);
                            worker_log_times_clone.get(&share.address).unwrap().store(current_time, AtomicOrdering::Relaxed);
                        }
                        if batch.len() >= 100 {
                            for share in batch.drain(..) {
                                if let Err(e) = db_clone.record_share(
                                    &share.address,
                                    share.difficulty,
                                    share.timestamp,
                                    &share.job_id,
                                    share.daa_score,
                                    &share.extranonce,
                                    &share.nonce,
                                ) {
                                    warn!("Failed to record batched share: {}", e);
                                }
                                let mut w = share_window_clone.write().await;
                                while w.len() > n_window {
                                    if let Some(old_share) = w.pop_front() {
                                        total_shares_clone.fetch_sub(old_share.difficulty, AtomicOrdering::Relaxed);
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
                                while w.len() > 0 && current_time_ms - w.front().map(|s| s.timestamp * 1000).unwrap_or(0) > window_time_ms {
                                    if let Some(old_share) = w.pop_front() {
                                        total_shares_clone.fetch_sub(old_share.difficulty, AtomicOrdering::Relaxed);
                                        if let Some(count) = window_submission_counts.get_mut(&old_share.address) {
                                            *count -= 1;
                                            if *count == 0 {
                                                window_submission_counts.remove(&old_share.address);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                        if !batch.is_empty() {
                            for share in batch.drain(..) {
                                if let Err(e) = db_clone.record_share(
                                    &share.address,
                                    share.difficulty,
                                    share.timestamp,
                                    &share.job_id,
                                    share.daa_score,
                                    &share.extranonce,
                                    &share.nonce,
                                ) {
                                    warn!("Failed to record batched share: {}", e);
                                }
                                let mut w = share_window_clone.write().await;
                                while w.len() > n_window {
                                    if let Some(old_share) = w.pop_front() {
                                        total_shares_clone.fetch_sub(old_share.difficulty, AtomicOrdering::Relaxed);
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
                                while w.len() > 0 && current_time_ms - w.front().map(|s| s.timestamp * 1000).unwrap_or(0) > window_time_ms {
                                    if let Some(old_share) = w.pop_front() {
                                        total_shares_clone.fetch_sub(old_share.difficulty, AtomicOrdering::Relaxed);
                                        if let Some(count) = window_submission_counts.get_mut(&old_share.address) {
                                            *count -= 1;
                                            if *count == 0 {
                                                window_submission_counts.remove(&old_share.address);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        let db_clone = db.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
                if let Err(e) = db_clone.prune_old_shares(24) {
                    warn!("Prune failed: {e}");
                }
            }
        });

        Ok(StratumDb {
            db,
            balances,
            share_window,
            total_shares,
            n_window,
            window_time_ms,
            share_batch,
            worker_share_counts,
            worker_log_times,
            network_id,
        })
    }

    pub async fn distribute_rewards(&self, block_hash: String, reward_block_hash: String, daa_score: u64) -> Result<()> {
        let subsidy = 200000000u64; // Example subsidy, adjust as needed
        let pool_fee_percent = 2u8; // Example fee, adjust as needed
        let miner_reward = subsidy * (100 - pool_fee_percent as u64) / 100;
        let pool_fee = subsidy - miner_reward;
        let rebate = (pool_fee as f64 * 0.0033) as u64;

        debug!(
            "Starting allocation. Miner Reward: {} sompi, Pool Fee: {} sompi, Rebate: {} sompi, Block Hash: {}, Reward Block Hash: {}, DAA Score: {}",
            miner_reward, pool_fee, rebate, block_hash, reward_block_hash, daa_score
        );

        let mut works: HashMap<String, (String, u64)> = HashMap::new();
        let mut total_work = 0u64;

        let mut shares = self.get_shares_since_last_allocation(daa_score).await;
        if shares.is_empty() {
            shares = self.get_difficulty_and_time_since_last_allocation().await;
            warn!("No shares found for daa_score {}, using fallback with {} shares", daa_score, shares.len());
        }

        for share in shares.iter() {
            let entry = works.entry(share.address.clone()).or_insert((share.address.clone(), 0));
            entry.1 += share.difficulty;
            total_work += share.difficulty;
        }

        if total_work == 0 {
            warn!("No work found for allocation, total shares: {}", shares.len());
            return Ok(());
        }

        let pool_address = std::env::var("MINING_ADDR").context("MINING_ADDR must be set in .env")?;
        let scaled_total = total_work as u128 * 100;
        for (address, (_miner_id, difficulty)) in works.iter() {
            let scaled_work = *difficulty as u128 * 100;
            let share = ((scaled_work * miner_reward as u128) / scaled_total) as u64;
            let nacho_rebate_kas = ((scaled_work * rebate as u128) / scaled_total) as u64;

            if share > 0 || nacho_rebate_kas > 0 {
                self.db.add_balance_with_wallet_total("pool", address, share, nacho_rebate_kas)?;
                *self.balances.entry(address.clone()).or_insert(0) += share + nacho_rebate_kas;
                let share_ve = share as f64 / 1e8;
                let rebate_ve = nacho_rebate_kas as f64 / 1e8;
                debug!(
                    "Allocated {} VE, rebate {} VE to {} (difficulty: {})",
                    share_ve, rebate_ve, address, difficulty
                );
            }
        }

        if !works.is_empty() && pool_fee > 0 {
            self.db.add_balance("pool", &pool_address, pool_fee)?;
            *self.balances.entry(pool_address.clone()).or_insert(0) += pool_fee;
            let pool_fee_ve = pool_fee as f64 / 1e8;
            info!(
                "Treasury generated {} VE revenue for block {} in reward block {}",
                pool_fee_ve, block_hash, reward_block_hash
            );
        }

        if !reward_block_hash.is_empty() {
            self.db.add_block_details(
                &block_hash,
                "pool",
                &reward_block_hash,
                &pool_address,
                daa_score,
                &pool_address,
                subsidy,
            )?;
        }

        // Process payouts
        self.process_payouts().await?;

        let share_window = self.share_window.read().await;
        debug!("Current share window size: {}", share_window.len());

        for entry in self.worker_share_counts.iter() {
            let count = entry.value().load(AtomicOrdering::Relaxed);
            let last_log_time = self.worker_log_times.get(entry.key())
                .map(|t| t.load(AtomicOrdering::Relaxed))
                .unwrap_or(0);
            debug!("Worker {}: total_shares={}, last_log_time={}", entry.key(), count, last_log_time);
            if let Ok(total) = self.db.get_total_submissions(entry.key()) {
                debug!("DB total submissions for {}: {}", entry.key(), total);
            }
        }

        Ok(())
    }

    async fn process_payouts(&self) -> Result<()> {
        // Check balances before calling /processPayouts
        let min_balance = 100000000; // 1 VE
        let balances = self.db.get_balances_for_payout(min_balance)?;
        debug!("Balances eligible for payout: {:?}", balances);
        if balances.is_empty() {
            warn!("No balances eligible for payout (min_balance: {} sompi)", min_balance);
            return Ok(());
        }

        let client = Client::new();
        let retry_strategy = ExponentialBackoff::from_millis(100).take(3);
        
        // Call the /processPayouts endpoint
        let response = Retry::spawn(retry_strategy, || async {
            client
                .post("http://localhost:8181/processPayouts")
                .send()
                .await
        })
        .await
        .context("Failed to call processPayouts")?;

        let status = response.status();
        let text = response
            .text()
            .await
            .context("Failed to read processPayouts response text")?;
        let result: Value = serde_json::from_str(&text)
            .map_err(|e| {
                warn!("Failed to parse JSON response: {}. Raw response: {}", e, text);
                e
            })
            .context("Failed to parse processPayouts response as JSON")?;
        
        debug!("processPayouts response (status: {}): {:?}", status, result);

        if let Some(error) = result.get("error") {
            return Err(anyhow::anyhow!("Payout processing failed: {:?}", error));
        }

        let transactions: Vec<Payout> = serde_json::from_value(result["result"].clone())
            .map_err(|e| {
                warn!("Failed to parse transactions: {}. Response: {:?}", e, result);
                e
            })
            .context("Failed to parse transactions")?;

        if transactions.is_empty() {
            warn!("No transactions returned from processPayouts");
            return Ok(());
        }

        // Log payouts and update database
        for payout in transactions {
            let amount: u64 = payout.amount.parse().context("Failed to parse amount as u64")?;
            info!(
                "Payout of {} VE to {} (txid: {})",
                amount as f64 / 1e8,
                payout.address,
                payout.txId
            );
            self.db.add_payment(&payout.address, amount, &payout.txId)?;
            self.db.reset_balance_by_address(&payout.address)?;
        }

        Ok(())
    }

    pub async fn record_share(&self, contribution: Contribution) -> Result<()> {
        let share_count = self.worker_share_counts
            .entry(contribution.address.clone())
            .or_insert(Arc::new(AtomicU64::new(0)))
            .clone();
        let count = share_count.fetch_add(1, AtomicOrdering::Relaxed) + 1;
        let mut share_window = self.share_window.write().await;
        share_window.push_back(contribution.clone());
        self.total_shares.fetch_add(contribution.difficulty, AtomicOrdering::Relaxed);

        let mut window_submission_counts: HashMap<String, u64> = HashMap::new();
        for share in share_window.iter() {
            *window_submission_counts.entry(share.address.clone()).or_insert(0) += 1;
        }

        while share_window.len() > self.n_window {
            if let Some(old_share) = share_window.pop_front() {
                self.total_shares.fetch_sub(old_share.difficulty, AtomicOrdering::Relaxed);
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
        while share_window.len() > 0 && current_time_ms - share_window.front().map(|s| s.timestamp * 1000).unwrap_or(0) > self.window_time_ms {
            if let Some(old_share) = share_window.pop_front() {
                self.total_shares.fetch_sub(old_share.difficulty, AtomicOrdering::Relaxed);
                if let Some(count) = window_submission_counts.get_mut(&old_share.address) {
                    *count -= 1;
                    if *count == 0 {
                        window_submission_counts.remove(&old_share.address);
                    }
                }
            }
        }

        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        let last_log_time = self.worker_log_times
            .entry(contribution.address.clone())
            .or_insert(Arc::new(AtomicU64::new(0)))
            .load(AtomicOrdering::Relaxed);
        if count % 100 == 0 || current_time >= last_log_time + 10 {
            info!("Recording share for worker {}, total submissions: {}", contribution.address, count);
            self.worker_log_times.get(&contribution.address).unwrap().store(current_time, AtomicOrdering::Relaxed);
        }

        if let Err(e) = self.share_batch.send(contribution).await {
            warn!("Failed to send share to batch: {}", e);
        }
        Ok(())
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
        debug!("Share counts for {}: total={}, window={}", address, total_submissions, window_submissions);
        Ok((total_submissions, window_submissions))
    }

    pub async fn get_shares_since_last_allocation(&self, daa_score: u64) -> Vec<Contribution> {
        let mut shares = Vec::new();
        let mut share_window = self.share_window.write().await;
        let current_time_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        let time_window_secs = 300; // 5 minutes
        while let Some(share) = share_window.front() {
            if share.daa_score <= daa_score || (share.timestamp <= current_time_ms && current_time_ms - share.timestamp <= time_window_secs) {
                if let Some(s) = share_window.pop_front() {
                    shares.push(s.clone());
                    self.total_shares.fetch_sub(s.difficulty, AtomicOrdering::Relaxed);
                }
            } else {
                break;
            }
        }
        debug!("Retrieved {} shares for daa_score {}", shares.len(), daa_score);
        shares
    }

    pub async fn get_difficulty_and_time_since_last_allocation(&self) -> Vec<Contribution> {
        let mut shares = Vec::new();
        let current_time_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;
        const MAX_ELAPSED_MS: u64 = 5 * 60 * 1000;

        for entry in self.worker_share_counts.iter() {
            let address = entry.key().clone();
            let last_log_time = self.worker_log_times
                .get(&address)
                .map(|t| t.load(AtomicOrdering::Relaxed))
                .unwrap_or(0);
            let time_since_last_share = current_time_ms - last_log_time * 1000;
            let capped_time = time_since_last_share.min(MAX_ELAPSED_MS);
            let time_weight = capped_time as f64 / MAX_ELAPSED_MS as f64;
            let min_diff = 1;
            let raw_difficulty = (min_diff as f64 * time_weight).round() as u64;
            let scaled_difficulty = if raw_difficulty == 0 {
                (min_diff as f64 * 0.1).max(1.0).floor() as u64
            } else {
                raw_difficulty
            };

            shares.push(Contribution {
                address: address.clone(),
                difficulty: scaled_difficulty,
                timestamp: current_time_ms / 1000,
                job_id: "".to_string(),
                daa_score: current_time_ms / 1000,
                extranonce: "".to_string(),
                nonce: "".to_string(),
            });
        }

        debug!("Generated {} fallback shares", shares.len());
        shares
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
            log_time.store(current_time, Ordering::Relaxed);
        }
    }

    pub async fn validate_share(&self, contribution: &Contribution, jobs: &Jobs, extranonce: &str, nonce: &str) -> Result<bool> {
        let is_duplicate = {
            let conn = self.db.get_conn();
            let mut stmt = conn.prepare(
                "SELECT COUNT(*) FROM shares WHERE job_id = ?1 AND address = ?2 AND extranonce = ?3 AND nonce = ?4",
            )?;
            let count: i64 = stmt.query_row(
                params![contribution.job_id, contribution.address, extranonce, nonce],
                |row| row.get(0),
            ).optional()?.unwrap_or(0);
            count > 0
        };

        if is_duplicate {
            warn!("Duplicate share detected for job_id={} address={} extranonce={} nonce={}", contribution.job_id, contribution.address, extranonce, nonce);
            return Ok(false);
        }

        let job_id_num = match contribution.job_id.parse::<u8>() {
            Ok(id) => id,
            Err(e) => {
                warn!("Invalid job_id={} for address={}: {}", contribution.job_id, contribution.address, e);
                return Ok(false);
            }
        };
        if jobs.get_job(job_id_num).await.is_none() {
            warn!("Stale share detected for job_id={} address={}", contribution.job_id, contribution.address);
            return Ok(false);
        }

        debug!("Share validated for job_id={} address={} extranonce={} nonce={}", contribution.job_id, contribution.address, extranonce, nonce);
        Ok(true)
    }
}