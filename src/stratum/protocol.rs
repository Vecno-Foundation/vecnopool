use anyhow::{Result, anyhow};
use log::{debug, info, warn};
use serde::Serialize;
use serde_json::json;
use tokio::io::{AsyncWriteExt, BufReader, Lines};
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::sync::{broadcast, mpsc, watch, RwLock, Mutex};
use hex;
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use crate::stratum::{Id, Request, Response};
use crate::stratum::jobs::{Jobs, JobParams, PendingResult};
use crate::treasury::sharehandler::{Contribution, Sharehandler};
use crate::treasury::share_validator::validate_share;
use crate::pow;
use crate::vecnod::RpcBlock;
use crate::uint::{U256, u256_to_hex};
use crate::api::fetch_block_details;
use crate::metrics::{
    MINER_ADDED_SHARES, MINER_INVALID_SHARES, MINER_DUPLICATED_SHARES, MINER_ADDRESS_MISMATCH,
    SHARE_VALIDATION_LATENCY,
};

const NEW_LINE: &'static str = "\n";

pub struct StratumConn<'a> {
    pub reader: Lines<BufReader<ReadHalf<'a>>>,
    pub writer: Arc<Mutex<WriteHalf<'a>>>,
    pub recv: watch::Receiver<Option<JobParams>>,
    pub jobs: Arc<Jobs>,
    pub pending_send: mpsc::UnboundedSender<PendingResult>,
    pub pending_recv: mpsc::UnboundedReceiver<PendingResult>,
    pub worker: [u8; 4],
    pub id: u64,
    pub subscribed: bool,
    pub difficulty: u64,
    pub authorized: bool,
    pub payout_addr: Option<String>,
    pub share_handler: Arc<Sharehandler>,
    pub last_template: Arc<RwLock<Option<RpcBlock>>>,
    pub extranonce: String,
    pub mining_addr: String,
    pub client: reqwest::Client,
    pub duplicate_share_count: Arc<AtomicU64>,
    pub payout_notify_recv: broadcast::Receiver<PayoutNotification>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PayoutNotification {
    pub address: String,
    pub amount: u64,
    pub tx_id: String,
    pub timestamp: i64,
}

// Helper function to handle share validation and submission
async fn validate_and_submit_share(
    writer: &Arc<Mutex<WriteHalf<'_>>>,
    i: Id,
    address: String,
    job_id: u8,
    nonce: u64,
    template: RpcBlock,
    jobs: &Arc<Jobs>,
    share_handler: &Arc<Sharehandler>,
    pending_send: &mpsc::UnboundedSender<PendingResult>,
    extranonce: String,
    difficulty: u64,
    client: &reqwest::Client,
    mining_addr: &str,
    duplicate_share_count: &Arc<AtomicU64>,
) -> Result<()> {
    let timer = SHARE_VALIDATION_LATENCY.start_timer(); // Start latency timer

    // Compute block hash with nonce
    let block_hash = {
        let mut header = template.header.clone().unwrap();
        header.nonce = nonce;
        let pow_hash = header.hash(false)?;
        hex::encode(pow_hash.as_bytes())
    };

    let nonce_hex = format!("{:016x}", nonce);
    let contribution = Contribution {
        address: address.clone(),
        difficulty: 1,
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs() as i64,
        job_id: job_id.to_string(),
        daa_score: template
            .header
            .as_ref()
            .map(|h| h.daa_score as i64)
            .unwrap_or(0),
        extranonce: extranonce.clone(),
        nonce: nonce_hex.clone(),
        reward_block_hash: Some(block_hash.clone()),
    };

    // Validate share
    if !validate_share(&contribution, jobs, share_handler.db.clone(), &nonce_hex).await? {
        let count = duplicate_share_count.fetch_add(1, Ordering::SeqCst) + 1;
        MINER_DUPLICATED_SHARES.with_label_values(&[&address]).inc();
        write_error_response(writer, i, 22, "Invalid or duplicate share".into()).await?;
        debug!(
            "Share rejected: job_id={}, block_hash={}, nonce={} for worker={}",
            job_id, block_hash, nonce_hex, address
        );
        if count > 100 {
            info!(
                "Excessive duplicate shares: count={} for worker={}",
                count, address
            );
            return Err(anyhow!("Excessive duplicate shares from {}", address));
        }
        timer.stop_and_record();
        return Ok(());
    }

    // Log diagnostic information
    let log_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut header = template.header.clone().unwrap();
        header.nonce = nonce;
        let pow_hash = header.hash(false)?;
        let pow_u256 = U256::from_little_endian(pow_hash.as_bytes());
        let network_target = pow::u256_from_compact_target(header.bits);
        let difficulty_f64 = (difficulty as f64) / ((1u64 << 32) as f64);

        debug!(
            "Share submitted: job_id={}, nonce={:016x}, pow_u256={}, network_target={}, difficulty={}",
            job_id, nonce, u256_to_hex(&pow_u256), u256_to_hex(&network_target), difficulty_f64
        );

        Ok::<(), anyhow::Error>(())
    }));

    if let Err(panic_err) = log_result {
        info!(
            "Diagnostic logging panicked: job_id={}, error={:?}",
            job_id, panic_err
        );
        write_error_response(writer, i, 23, "Internal server error".into()).await?;
        timer.stop_and_record();
        return Ok(());
    }

    // Submit share to jobs
    debug!("Submitting share to jobs: job_id={}, nonce={:016x}, block_hash={}", job_id, nonce, block_hash);
    if jobs.submit(i.clone(), job_id, nonce, pending_send.clone()).await {
        MINER_ADDED_SHARES.with_label_values(&[&address]).inc();
        info!("Share accepted: job_id={} for worker={}", job_id, address);
        if let Err(e) = share_handler.record_share(&contribution, difficulty).await {
            info!("Failed to record share for worker={}: {:?}", address, e);
        }

        // Sleep to simulate network delay (reduced for Vecno's fast blockchain)
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let pool_fee = share_handler.pool_fee;
        info!("Using pool fee {}% for block {}", pool_fee, block_hash);
        let (_reward_block_hash, _daa_score) = match fetch_block_details(
            share_handler.db.clone(),
            client,
            &block_hash,
            mining_addr,
            pool_fee,
        ).await {
            Ok(result) => result,
            Err(_e) => (block_hash.clone(), contribution.daa_score as u64),
        };

        let (total_submissions, window_submissions) = share_handler.get_share_counts(&address).await
            .map_err(|e| anyhow!("Failed to get share counts: {}", e))?;
        if share_handler.should_log_share(&address, total_submissions).await {
            info!(
                "Share accepted: job_id={}, worker={}, total_submissions={}, window_submissions={}",
                job_id, address, total_submissions, window_submissions
            );
            share_handler.update_log_time(&address).await;
            let params = json!([address.clone(), total_submissions, window_submissions]);
            write_notification(writer, "mining.share_update", Some(params)).await?;
        }
        write_response(writer, i, Some(true)).await?;
    } else {
        MINER_INVALID_SHARES.with_label_values(&[&address]).inc();
        debug!("Unable to submit share: job_id={}, block_hash={}", job_id, block_hash);
        write_error_response(writer, i, 20, "Unable to submit share".into()).await?;
    }

    timer.stop_and_record(); // Record latency
    Ok(())
}

// Helper functions for writing to the socket
async fn write_response<T: Serialize>(writer: &Arc<Mutex<WriteHalf<'_>>>, id: Id, result: Option<T>) -> Result<()> {
    let res = Response::ok(id, result)?;
    write(writer, &res).await
}

async fn write_error_response(writer: &Arc<Mutex<WriteHalf<'_>>>, id: Id, code: u64, message: Box<str>) -> Result<()> {
    let res = Response::err(id, code, message)?;
    write(writer, &res).await
}

async fn write_notification(writer: &Arc<Mutex<WriteHalf<'_>>>, method: &'static str, params: Option<serde_json::Value>) -> Result<()> {
    let req = Request {
        id: None,
        method: method.into(),
        params,
    };
    write(writer, &req).await
}

async fn write<T: Serialize>(writer: &Arc<Mutex<WriteHalf<'_>>>, data: &T) -> Result<()> {
    let data = serde_json::to_vec(data)?;
    let mut writer = writer.lock().await;
    writer.write_all(&data).await?;
    writer.write_all(NEW_LINE.as_bytes()).await?;
    Ok(())
}

impl<'a> StratumConn<'a> {
    pub async fn write_template(&mut self) -> Result<()> {
        debug!("Sending template to worker: {:?}", self.payout_addr);
        let (base_difficulty, params) = {
            let borrow = self.recv.borrow();
            match borrow.as_ref() {
                Some(j) => (j.difficulty(), j.to_value()),
                None => {
                    debug!("No job template available for worker: {:?}", self.payout_addr);
                    return Ok(());
                }
            }
        };
        debug!("Base difficulty for job: {}", base_difficulty);
        self.write_request("mining.notify", Some(params)).await?;

        let address = self.payout_addr.as_ref().unwrap_or(&String::new()).clone();
        if !address.is_empty() && self.authorized {
            let adjusted_difficulty = self.share_handler.get_dynamic_difficulty(&address, base_difficulty).await;
            let difficulty_f64 = (adjusted_difficulty as f64) / ((1u64 << 32) as f64);
            if self.difficulty != adjusted_difficulty {
                self.difficulty = adjusted_difficulty;
                info!(
                    "Sending difficulty {} (raw: {}) to worker: {}",
                    difficulty_f64, adjusted_difficulty, address
                );
                self.write_request("mining.set_difficulty", Some(json!([difficulty_f64])))
                    .await?;
            }
        }

        Ok(())
    }

    async fn write_request(
        &mut self,
        method: &'static str,
        params: Option<serde_json::Value>,
    ) -> Result<()> {
        self.id += 1;
        let req = Request {
            id: Some(self.id.into()),
            method: method.into(),
            params,
        };
        self.write(&req).await
    }

    async fn write_response<T: Serialize>(&mut self, id: Id, result: Option<T>) -> Result<()> {
        let res = Response::ok(id, result)?;
        self.write(&res).await
    }

    async fn write_error_response(&mut self, id: Id, code: u64, message: Box<str>) -> Result<()> {
        let res = Response::err(id, code, message)?;
        self.write(&res).await
    }

    async fn write_notification(&mut self, method: &'static str, params: Option<serde_json::Value>) -> Result<()> {
        let req = Request {
            id: None,
            method: method.into(),
            params,
        };
        self.write(&req).await
    }

    async fn write<T: Serialize>(&mut self, data: &T) -> Result<()> {
        let data = serde_json::to_vec(data)?;
        debug!("Writing to miner: {} for worker: {:?}", String::from_utf8_lossy(&data), self.payout_addr);
        let mut writer = self.writer.lock().await;
        writer.write_all(&data).await?;
        writer.write_all(NEW_LINE.as_bytes()).await?;
        Ok(())
    }

    async fn send_payout_notification(&mut self, payout: PayoutNotification) -> Result<()> {
        if self.payout_addr.as_ref() == Some(&payout.address) {
            debug!("Sending payout notification to worker: {:?}, amount={} VE, tx_id={}",
                   self.payout_addr, payout.amount as f64 / 100_000_000.0, payout.tx_id);
            self.write_notification(
                "mining.payout",
                Some(json!({
                    "address": payout.address,
                    "amount": payout.amount as f64 / 100_000_000.0,
                    "tx_id": payout.tx_id,
                    "timestamp": payout.timestamp
                }))
            ).await?;
        }
        Ok(())
    }

    pub async fn run(&mut self) -> Result<()> {
        debug!("Initialized connection with sharehandler for worker: {:?}", self.payout_addr);
        debug!("Last template status: has_template={:?} for worker: {:?}", self.last_template.read().await.is_some(), self.payout_addr);

        loop {
            tokio::select! {
                res = self.recv.changed() => match res {
                    Err(_) => break,
                    Ok(_) => {
                        if self.subscribed {
                            self.write_template().await?;
                        }
                    }
                },
                item = self.payout_notify_recv.recv() => {
                    match item {
                        Ok(payout) => {
                            self.send_payout_notification(payout).await?;
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            info!("Payout notification receiver lagged by {} messages for worker {:?}", n, self.payout_addr);
                            continue;
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            info!("Payout notification channel closed for worker {:?}", self.payout_addr);
                            break;
                        }
                    }
                },
                item = self.pending_recv.recv() => {
                    let res = item.expect("channel is always open").into_response()?;
                    self.write(&res).await?;
                },
                res = read(&mut self.reader) => match res {
                    Ok(Some(msg)) => {
                        debug!("Processing message: method={} for worker: {:?}", msg.method, self.payout_addr);
                        match (msg.id, &*msg.method, msg.params) {
                            (Some(id), "mining.subscribe", Some(_p)) => {
                                debug!("Worker subscribed: {:?}", self.payout_addr);
                                self.subscribed = true;
                                self.extranonce = format!("{:08x}", u32::from_be_bytes(self.worker));
                                debug!(
                                    "Setting extranonce: {} from worker bytes: {:?}, worker_hex: {:08x}",
                                    self.extranonce, self.worker, u32::from_be_bytes(self.worker)
                                );
                                self.write_response(id, Some(true)).await?;
                                self.write_request(
                                    "set_extranonce",
                                    Some(json!([self.extranonce, 4])),
                                ).await?;
                            }
                            (Some(id), "mining.authorize", Some(p)) => {
                                let params: Vec<String> = serde_json::from_value(p)?;
                                if params.len() < 1 || !params[0].starts_with("vecno:") || params[0].len() < 10 {
                                    self.write_error_response(id, 21, "Invalid address format".into()).await?;
                                    continue;
                                }
                                self.payout_addr = Some(params[0].clone());
                                self.authorized = true;
                                let address = self.payout_addr.as_ref().unwrap();
                                let base_difficulty = self.recv.borrow().as_ref().map_or(0, |j| j.difficulty());
                                let adjusted_difficulty = self.share_handler.get_dynamic_difficulty(address, base_difficulty).await;
                                self.difficulty = adjusted_difficulty;
                                let difficulty_f64 = (adjusted_difficulty as f64) / ((1u64 << 32) as f64);
                                info!(
                                    "Sending initial difficulty {} (raw: {}) to worker: {}",
                                    difficulty_f64, adjusted_difficulty, address
                                );
                                self.write_request("mining.set_difficulty", Some(json!([difficulty_f64])))
                                    .await?;
                                self.write_response(id, Some(true)).await?;
                                self.write_template().await?;
                            }
                            (Some(id), "mining.get_shares", Some(p)) => {
                                if !self.authorized {
                                    self.write_error_response(id, 24, "Unauthorized worker".into()).await?;
                                    continue;
                                }
                                let params: Vec<String> = serde_json::from_value(p)?;
                                let address = params.get(0).cloned().unwrap_or_default();
                                if address != *self.payout_addr.as_ref().unwrap_or(&String::new()) {
                                    MINER_ADDRESS_MISMATCH.with_label_values(&[&address]).inc();
                                    self.write_error_response(id, 23, "Unknown worker".into()).await?;
                                    continue;
                                }
                                let (total_submissions, window_submissions) = self.share_handler.get_share_counts(&address).await
                                    .map_err(|e| anyhow!("Failed to get share counts: {}", e))?;
                                debug!(
                                    "Sending share counts: total={}, window={} for address={}",
                                    total_submissions, window_submissions, address
                                );
                                self.write_response(id, Some(json!([total_submissions, window_submissions]))).await?;
                            }
                            (Some(id), "mining.get_balance", Some(p)) => {
                                if !self.authorized {
                                    self.write_error_response(id, 24, "Unauthorized worker".into()).await?;
                                    continue;
                                }
                                let params: Vec<String> = serde_json::from_value(p)?;
                                let address = params.get(0).cloned().unwrap_or_default();
                                if address != *self.payout_addr.as_ref().unwrap_or(&String::new()) {
                                    MINER_ADDRESS_MISMATCH.with_label_values(&[&address]).inc();
                                    self.write_error_response(id, 23, "Unknown worker".into()).await?;
                                    continue;
                                }
                                let (available_balance, pending_balance, effective_hashrate) = self.share_handler.get_balances_and_hashrate(&address, self).await
                                    .map_err(|e| anyhow!("Failed to get balances: {}", e))?;
                                debug!(
                                    "Sending balance: available={} VE, pending={} VE, effective_hashrate={} Mhash/s for address={}",
                                    available_balance as f64 / 100_000_000.0,
                                    pending_balance as f64 / 100_000_000.0,
                                    effective_hashrate,
                                    address
                                );
                                self.write_response(id, Some(json!({
                                    "available_balance": available_balance as f64 / 100_000_000.0,
                                    "pending_balance": pending_balance as f64 / 100_000_000.0,
                                    "effective_hashrate": effective_hashrate
                                }))).await?;
                            }
                            (Some(id), "mining.configure", Some(_)) => {
                                debug!("Received mining.configure for worker: {:?}", self.payout_addr);
                                self.write_response(id, Some(json!({
                                    "version-rolling": false,
                                    "minimum-difficulty": true
                                }))).await?;
                            }
                            (Some(i), "mining.submit", Some(p)) => {
                                if !self.authorized {
                                    self.write_error_response(i, 24, "Unauthorized worker".into()).await?;
                                    continue;
                                }
                                let (address, id_str, nonce_str): (String, String, String) = serde_json::from_value(p)?;
                                if address != *self.payout_addr.as_ref().unwrap_or(&String::new()) {
                                    MINER_ADDRESS_MISMATCH.with_label_values(&[&address]).inc();
                                    self.write_error_response(i, 23, "Unknown worker".into()).await?;
                                    continue;
                                }
                                let job_id = match u8::from_str_radix(&id_str, 16) {
                                    Ok(id) => id,
                                    Err(e) => {
                                        MINER_INVALID_SHARES.with_label_values(&[&address]).inc();
                                        self.write_error_response(i, 21, format!("Invalid job ID: {}", e).into()).await?;
                                        continue;
                                    }
                                };
                                let nonce = match u64::from_str_radix(nonce_str.trim_start_matches("0x"), 16) {
                                    Ok(n) => n,
                                    Err(e) => {
                                        MINER_INVALID_SHARES.with_label_values(&[&address]).inc();
                                        self.write_error_response(i, 21, format!("Invalid nonce: {}", e).into()).await?;
                                        continue;
                                    }
                                };

                                let template = match self.jobs.get_job(job_id).await {
                                    Some(b) => b,
                                    None => {
                                        MINER_INVALID_SHARES.with_label_values(&[&address]).inc();
                                        self.write_error_response(i, 21, "Stale job".into()).await?;
                                        debug!("Stale job: job_id={} for worker={}", job_id, address);
                                        continue;
                                    }
                                };

                                // Call validate_and_submit_share directly
                                if let Err(e) = validate_and_submit_share(
                                    &self.writer,
                                    i,
                                    address,
                                    job_id,
                                    nonce,
                                    template,
                                    &self.jobs,
                                    &self.share_handler,
                                    &self.pending_send,
                                    self.extranonce.clone(),
                                    self.difficulty,
                                    &self.client,
                                    &self.mining_addr,
                                    &self.duplicate_share_count,
                                ).await {
                                    warn!("Share validation failed for worker={}: {:?}", self.payout_addr.as_ref().unwrap_or(&String::new()), e);
                                }
                            }
                            _ => {
                                debug!("Got unknown method: {} for worker: {:?}", msg.method, self.payout_addr);
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(e) => return Err(e),
                },
            }
        }
        Ok(())
    }
}

pub async fn read(r: &mut Lines<BufReader<ReadHalf<'_>>>) -> Result<Option<Request>> {
    let line = match r.next_line().await? {
        Some(l) => l,
        None => return Ok(None),
    };
    debug!("Received from miner: {}", line);
    Ok(Some(serde_json::from_str(&line)?))
}