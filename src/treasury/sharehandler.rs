use anyhow::{Context, Result};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, RwLock};
use log::{debug, info, warn};
use tokio_retry::strategy::ExponentialBackoff;
use tokio_retry::Retry;
use reqwest::Client;
use crate::database::db::Db;
use crate::stratum::jobs::Jobs;
use crate::treasury::reward_table::get_block_reward;

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

#[derive(Debug, Clone, Deserialize)]
pub struct Payout {
    pub address: String,
    pub amount: String,
    #[serde(alias = "txid")]
    pub tx_id: String,
}

#[derive(Debug)]
pub struct Sharehandler {
    pub db: Arc<Db>,
    pub balances: Arc<DashMap<String, u64>>,
    pub share_window: Arc<RwLock<VecDeque<Contribution>>>,
    pub total_shares: Arc<AtomicU64>,
    pub n_window: usize,
    pub window_time_ms: u64,
    pub share_batch: mpsc::Sender<Contribution>,
    pub worker_share_counts: Arc<DashMap<String, Arc<AtomicU64>>>,
    pub worker_log_times: Arc<DashMap<String, Arc<AtomicU64>>>,
}

impl Sharehandler {
    pub async fn new(db: Arc<Db>, n_window: usize, window_time_ms: u64) -> Result<Self> {
        let balances = Arc::new(DashMap::new());
        db.load_balances(&balances).await.context("Failed to load balances")?;
        let mut share_window = VecDeque::new();
        let total_shares = db.load_recent_shares(&mut share_window, n_window)
            .await
            .context("Failed to load recent shares")?;
        let share_window = Arc::new(RwLock::new(share_window));
        let total_shares = Arc::new(AtomicU64::new(total_shares));
        let (share_batch, mut share_batch_rx) = mpsc::channel::<Contribution>(1000);
        let worker_share_counts = Arc::new(DashMap::new());
        let worker_log_times = Arc::new(DashMap::new());

        if let Ok(sums) = db.get_total_submissions_all().await {
            for entry in sums.iter() {
                worker_share_counts.insert(entry.key().clone(), Arc::new(AtomicU64::new(*entry.value())));
            }
        }

        debug!("Initialized sharehandler with n_window={} and window_time_ms={}", n_window, window_time_ms);

        let db_clone = db.clone();
        let share_window_clone = share_window.clone();
        let total_shares_clone = total_shares.clone();
        let worker_share_counts_clone = worker_share_counts.clone();
        let worker_log_times_clone = worker_log_times.clone();
        let n_window = n_window;
        let window_time_ms = window_time_ms;

        let stratum_handler_clone = Arc::new(Sharehandler {
            db: db_clone.clone(),
            balances: balances.clone(),
            share_window: share_window_clone.clone(),
            total_shares: total_shares_clone.clone(),
            n_window,
            window_time_ms,
            share_batch: share_batch.clone(),
            worker_share_counts: worker_share_counts_clone.clone(),
            worker_log_times: worker_log_times_clone.clone(),
        });

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
                                    share.difficulty as u64,
                                    share.timestamp as u64,
                                    &share.job_id,
                                    share.daa_score as u64,
                                    &share.extranonce,
                                    &share.nonce,
                                ).await {
                                    warn!("Failed to record batched share: {}", e);
                                }
                                let mut w = share_window_clone.write().await;
                                w.push_back(share.clone());
                                while w.len() > n_window {
                                    if let Some(old_share) = w.pop_front() {
                                        total_shares_clone.fetch_sub(old_share.difficulty as u64, AtomicOrdering::Relaxed);
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
                                        total_shares_clone.fetch_sub(old_share.difficulty as u64, AtomicOrdering::Relaxed);
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
                                    share.difficulty as u64,
                                    share.timestamp as u64,
                                    &share.job_id,
                                    share.daa_score as u64,
                                    &share.extranonce,
                                    &share.nonce,
                                ).await {
                                    warn!("Failed to record batched share: {}", e);
                                }
                                let mut w = share_window_clone.write().await;
                                w.push_back(share.clone());
                                while w.len() > n_window {
                                    if let Some(old_share) = w.pop_front() {
                                        total_shares_clone.fetch_sub(old_share.difficulty as u64, AtomicOrdering::Relaxed);
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
                                        total_shares_clone.fetch_sub(old_share.difficulty as u64, AtomicOrdering::Relaxed);
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

        tokio::spawn({
            let handler = stratum_handler_clone.clone();
            async move {
                loop {
                    tokio::time::sleep(tokio::time::Duration::from_secs(360)).await; // 6 minutes
                    if let Err(e) = handler.check_confirmations().await {
                        warn!("Failed to check confirmations: {}", e);
                    }
                    if let Err(e) = handler.process_payouts().await {
                        warn!("Failed to process payouts: {}", e);
                    }
                }
            }
        });

        Ok(Sharehandler {
            db,
            balances,
            share_window,
            total_shares,
            n_window,
            window_time_ms,
            share_batch,
            worker_share_counts,
            worker_log_times,
        })
    }

    pub async fn distribute_rewards(&self, block_hash: String, reward_block_hash: String, daa_score: u64, template_amount: Option<u64>) -> Result<()> {
        let mut subsidy = get_block_reward(daa_score)
            .context(format!("Failed to get block reward for daa_score {}", daa_score))?;
        if let Some(template_amount) = template_amount {
            if subsidy != template_amount {
                warn!(
                    "Block reward mismatch: table gives {} sompi, template gives {} sompi for daa_score {}. Using template amount.",
                    subsidy, template_amount, daa_score
                );
                subsidy = template_amount;
            }
        }
        debug!("Using block reward of {} sompi for daa_score {}", subsidy, daa_score);

        let pool_fee_percent = 2u8;
        let miner_reward = subsidy * (100 - pool_fee_percent as u64) / 100;
        let pool_fee = subsidy - miner_reward;

        debug!(
            "Starting allocation. Miner Reward: {} sompi, Pool Fee: {} sompi, Block Hash: {}, Reward Block Hash: {}, DAA Score: {}",
            miner_reward, pool_fee, block_hash, reward_block_hash, daa_score
        );

        let mut works: HashMap<String, (String, u64)> = HashMap::new();
        let mut total_work = 0u64;

        let shares = self.get_shares_since_last_allocation(daa_score).await;
        if shares.is_empty() {
            warn!("No shares found for daa_score {}, skipping allocation", daa_score);
            return Ok(());
        }

        for share in shares.iter() {
            let entry = works.entry(share.address.clone()).or_insert((share.address.clone(), 0));
            entry.1 += share.difficulty as u64;
            total_work += share.difficulty as u64;
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

            if share > 0 {
                self.db.add_pending_balance("pool", address, share).await?;
                *self.balances.entry(address.clone()).or_insert(0) += share;
                let share_ve = share as f64 / 1e8;
                debug!(
                    "Allocated {} VE to {} (difficulty: {})",
                    share_ve, address, difficulty
                );
            }
        }

        if !works.is_empty() && pool_fee > 0 {
            self.db.add_pending_balance("pool", &pool_address, pool_fee).await?;
            *self.balances.entry(pool_address.clone()).or_insert(0) += pool_fee;
            let pool_fee_ve = pool_fee as f64 / 1e8;
            info!(
                "Treasury generated {} VE revenue for block {} in reward block {}",
                pool_fee_ve, block_hash, reward_block_hash
            );
        }

        if !reward_block_hash.is_empty() && reward_block_hash != "reward_block_hash_placeholder" {
            self.db.add_block_details(
                &block_hash,
                "pool",
                &reward_block_hash,
                &pool_address,
                daa_score,
                &pool_address,
                subsidy,
            ).await?;
        } else {
            warn!("Skipping block details storage due to invalid reward_block_hash: {}", reward_block_hash);
        }

        let share_window = self.share_window.read().await;
        debug!("Current share window size: {}", share_window.len());

        for entry in self.worker_share_counts.iter() {
            let count = entry.value().load(AtomicOrdering::Relaxed);
            let last_log_time = self.worker_log_times.get(entry.key())
                .map(|t| t.load(AtomicOrdering::Relaxed))
                .unwrap_or(0);
            debug!("Worker {}: total_shares={}, last_log_time={}", entry.key(), count, last_log_time);
            if let Ok(total) = self.db.get_total_submissions(entry.key()).await {
                debug!("DB total submissions for {}: {}", entry.key(), total);
            }
        }

        Ok(())
    }

    pub async fn process_payouts(&self) -> Result<()> {
        let min_balance = 100_000_000; // 1 VE
        let balances = self.db.get_balances_for_payout(min_balance).await?;
        debug!("Balances eligible for payout: {:?}", balances);
        if balances.is_empty() {
            warn!("No balances eligible for payout (min_balance: {} sompi)", min_balance);
            return Ok(());
        }

        let client = Client::new();
        let retry_strategy = ExponentialBackoff::from_millis(100).take(3);
        
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
        let result: serde_json::Value = serde_json::from_str(&text)
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

        for payout in transactions {
            let amount: u64 = payout.amount.parse().context("Failed to parse amount as u64")?;
            info!(
                "Payout of {} VE to {} (tx_id: {})",
                amount as f64 / 1e8,
                payout.address,
                payout.tx_id
            );
            self.db.add_payment(&payout.address, amount, &payout.tx_id).await?;
            self.db.reset_available_balance(&payout.address).await?;
        }

        Ok(())
    }

    pub async fn check_confirmations(&self) -> Result<()> {
        let blocks = self.db.get_unconfirmed_blocks().await?;
        let client = Client::new();
        let retry_strategy = ExponentialBackoff::from_millis(100).take(3);

        for block in blocks {
            if block.reward_block_hash == "reward_block_hash_placeholder" {
                warn!("Skipping confirmation check for invalid reward_block_hash: {}", block.reward_block_hash);
                continue;
            }
            let url = format!("https://api.vecnoscan.org/blocks/{}?includeColor=false", block.reward_block_hash);
            let response: serde_json::Value = Retry::spawn(retry_strategy.clone(), || async {
                let response = client.get(&url).send().await?.json().await;
                debug!("Vecnoscan response for block {}: {:?}", block.reward_block_hash, response);
                response
            })
            .await
            .context(format!("Failed to fetch block details for hash {}", block.reward_block_hash))?;

            let confirmations = response["verboseData"]["blueScore"]
                .as_u64()
                .map(|blue_score| blue_score.saturating_sub(block.daa_score as u64))
                .unwrap_or(0);

            if confirmations >= 100 {
                self.db.update_block_confirmations(&block.reward_block_hash, confirmations).await?;
                self.db.move_pending_to_available(&block.miner_id, &block.pool_wallet, block.amount as u64).await?;
                info!("Block {} confirmed with {} confirmations, moved {} sompi to available balance for {}", 
                    block.reward_block_hash, confirmations, block.amount, block.pool_wallet);
            }
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
        self.total_shares.fetch_add(contribution.difficulty as u64, AtomicOrdering::Relaxed);

        let mut window_submission_counts: HashMap<String, u64> = HashMap::new();
        for share in share_window.iter() {
            *window_submission_counts.entry(share.address.clone()).or_insert(0) += 1;
        }

        while share_window.len() > self.n_window {
            if let Some(old_share) = share_window.pop_front() {
                self.total_shares.fetch_sub(old_share.difficulty as u64, AtomicOrdering::Relaxed);
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
        while share_window.len() > 0 && current_time_ms - share_window.front().map(|s| (s.timestamp as u64) * 1000).unwrap_or(0) > self.window_time_ms {
            if let Some(old_share) = share_window.pop_front() {
                self.total_shares.fetch_sub(old_share.difficulty as u64, AtomicOrdering::Relaxed);
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

        if let Err(e) = self.db.record_share(
            &contribution.address,
            contribution.difficulty as u64,
            contribution.timestamp as u64,
            &contribution.job_id,
            contribution.daa_score as u64,
            &contribution.extranonce,
            &contribution.nonce,
        ).await {
            warn!("Failed to record share: {}", e);
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
        let time_window_secs = 60; // 1 minute for testing
        while let Some(share) = share_window.front() {
            if (share.daa_score as u64) <= daa_score || (share.timestamp as u64 <= current_time_ms && current_time_ms - share.timestamp as u64 <= time_window_secs) {
                if let Some(s) = share_window.pop_front() {
                    shares.push(s.clone());
                    self.total_shares.fetch_sub(s.difficulty as u64, AtomicOrdering::Relaxed);
                }
            } else {
                break;
            }
        }
        debug!("Retrieved {} shares for daa_score {}", shares.len(), daa_score);
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
            log_time.store(current_time, AtomicOrdering::Relaxed);
        }
    }

    pub async fn validate_share(&self, contribution: &Contribution, jobs: &Jobs, extranonce: &str, nonce: &str) -> Result<bool> {
        let is_duplicate = {
            let count = self.db.check_duplicate_share(&contribution.job_id, &contribution.address, extranonce, nonce).await?;
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