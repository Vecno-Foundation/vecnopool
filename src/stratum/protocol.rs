use anyhow::{Context, Result, anyhow};
use log::{debug, info, warn};
use serde::Serialize;
use serde_json::json;
use tokio::io::{AsyncWriteExt, BufReader, Lines};
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::sync::{mpsc, watch, RwLock, Mutex};
use hex;
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use crate::stratum::{Id, Request, Response};
use crate::stratum::jobs::{Jobs, JobParams, PendingResult};
use crate::treasury::sharehandler::{Contribution, Sharehandler};
use crate::treasury::share_validator::validate_share;
use crate::pow;
use crate::vecnod::RpcBlock;
use crate::uint::{U256, u256_to_hex};

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
    pub duplicate_share_count: Arc<AtomicU64>,
}

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
    duplicate_share_count: &Arc<AtomicU64>,
) -> Result<()> {
    let block_hash = {
        let mut header = template.header.clone().unwrap();
        header.nonce = nonce;
        let pow_hash = header.hash(false)?;
        hex::encode(pow_hash.as_bytes())
    };

    let nonce_hex = format!("{:016x}", nonce);
    let contribution = Contribution {
        address: address.clone(),
        difficulty: difficulty as i64,
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

    let is_valid = validate_share(&contribution, jobs, &nonce_hex).await?;
    let is_duplicate = if is_valid {
        share_handler.db
            .check_duplicate_share(&block_hash, &nonce_hex)
            .await
            .context("Failed to check for duplicate share")? > 0
    } else {
        false
    };

    if is_duplicate {
        let count = duplicate_share_count.fetch_add(1, Ordering::SeqCst) + 1;
        write_error_response(writer, i, 22, "Duplicate share".into()).await?;
        debug!(
            "Share rejected (duplicate): job_id={}, block_hash={}, nonce={} for worker={}",
            job_id, block_hash, nonce_hex, address
        );
        if count > 100 {
            info!(
                "Excessive duplicate shares: count={} for worker={}",
                count, address
            );
            return Err(anyhow!("Excessive duplicate shares from {}", address));
        }
        return Ok(());
    }

    if !is_valid {
        write_error_response(writer, i, 22, "Invalid share".into()).await?;
        debug!(
            "Share rejected (invalid): job_id={}, block_hash={}, nonce={} for worker={}",
            job_id, block_hash, nonce_hex, address
        );
        return Ok(());
    }

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
        return Ok(());
    }

    debug!("Submitting share to jobs: job_id={}, nonce={:016x}, block_hash={}", job_id, nonce, block_hash);
    if jobs.submit(i.clone(), job_id, nonce, address.clone(), pending_send.clone()).await {
        info!("Share accepted: job_id={} for worker={}", job_id, address);
        if let Err(e) = share_handler.record_share(&contribution, difficulty, is_valid, is_duplicate).await {
            info!("Failed to record share for worker={}: {:?}", address, e);
        }

        let result = PendingResult {
            id: i,
            error: None,
            block_hash: Some(block_hash),
        };
        let _ = pending_send.send(result);
    } else {
        debug!("Unable to submit share: job_id={}, block_hash={}", job_id, block_hash);
        write_error_response(writer, i, 20, "Unable to submit share".into()).await?;
    }

    Ok(())
}

async fn write_error_response(writer: &Arc<Mutex<WriteHalf<'_>>>, id: Id, code: u64, message: Box<str>) -> Result<()> {
    let res = Response::err(id, code, message)?;
    write(writer, &res).await
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
        let params = {
            let borrow = self.recv.borrow();
            match borrow.as_ref() {
                Some(j) => j.to_value(),
                None => {
                    debug!("No job template available for worker: {:?}", self.payout_addr);
                    return Ok(());
                }
            }
        };
        self.write_request("mining.notify", Some(params)).await?;

        let address = self.payout_addr.as_ref().unwrap_or(&String::new()).clone();
        if !address.is_empty() && self.authorized {
            let job_id = self.recv.borrow().as_ref().map(|j| j.id.to_string()).unwrap_or_default();
            let network_difficulty = match job_id.parse::<u8>() {
                Ok(job_id_u8) => self.jobs
                    .get_job_params(job_id_u8)
                    .await
                    .map(|params| params.difficulty())
                    .ok_or_else(|| anyhow!("No job params for job_id={}", job_id_u8))?,
                Err(_) => {
                    warn!(target: "stratum::protocol",
                        "Invalid job_id format for worker={}: job_id={}", address, job_id
                    );
                    return Ok(());
                }
            };
            if self.difficulty != network_difficulty {
                self.difficulty = network_difficulty;
                let difficulty_f64 = (network_difficulty as f64) / ((1u64 << 32) as f64);
                debug!(
                    "Sending network difficulty {} (raw: {}) to worker: {} (source: network, job_id: {})",
                    difficulty_f64, network_difficulty, address, job_id
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

    async fn write<T: Serialize>(&mut self, data: &T) -> Result<()> {
        let data = serde_json::to_vec(data)?;
        debug!("Writing to miner: {} for worker: {:?}", String::from_utf8_lossy(&data), self.payout_addr);
        let mut writer = self.writer.lock().await;
        writer.write_all(&data).await?;
        writer.write_all(NEW_LINE.as_bytes()).await?;
        Ok(())
    }

    pub async fn run(&mut self) -> Result<()> {
        debug!("Initialized connection with sharehandler for worker: {:?}", self.payout_addr);
        debug!("Last template status: has_template={:?} for worker: {:?}", self.last_template.read().await.is_some(), self.payout_addr);

        // Check initial connection state
        if self.recv.borrow().is_none() {
            debug!("No valid template available, rejecting connection for worker: {:?}", self.payout_addr);
            self.write_error_response(Id::Number(0), 25, "Node disconnected, please try again later".into()).await?;
            return Ok(());
        }

        loop {
            tokio::select! {
                res = self.recv.changed() => match res {
                    Err(_) => {
                        debug!("Template channel closed, closing connection for worker: {:?}", self.payout_addr);
                        self.write_error_response(Id::Number(self.id + 1), 25, "Node disconnected, please try again later".into()).await?;
                        return Ok(());
                    }
                    Ok(_) => {
                        if self.recv.borrow().is_none() {
                            debug!("No valid template available, closing connection for worker: {:?}", self.payout_addr);
                            self.write_error_response(Id::Number(self.id + 1), 25, "Node disconnected, please try again later".into()).await?;
                            return Ok(());
                        }
                        if self.subscribed {
                            self.write_template().await?;
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
                                // Check template availability before authorizing
                                if self.recv.borrow().is_none() {
                                    self.write_error_response(id, 25, "Node disconnected, please try again later".into()).await?;
                                    debug!("No valid template available, rejecting authorization for worker: {:?}", self.payout_addr);
                                    return Ok(());
                                }
                                self.payout_addr = Some(params[0].clone());
                                self.authorized = true;
                                let address = self.payout_addr.as_ref().unwrap();
                                let job_id = self.recv.borrow().as_ref().map(|j| j.id.to_string()).unwrap_or_default();
                                let network_difficulty = match job_id.parse::<u8>() {
                                    Ok(job_id_u8) => match self.jobs.get_job_params(job_id_u8).await {
                                        Some(params) => params.difficulty(),
                                        None => {
                                            warn!(target: "stratum::protocol",
                                                "No valid job params for worker={}: job_id={}", address, job_id
                                            );
                                            continue;
                                        }
                                    },
                                    Err(_) => {
                                        warn!(target: "stratum::protocol",
                                            "Invalid job_id format for worker={}: job_id={}", address, job_id
                                        );
                                        continue;
                                    }
                                };
                                self.difficulty = network_difficulty;
                                let difficulty_f64 = (network_difficulty as f64) / ((1u64 << 32) as f64);
                                debug!(
                                    "Sending network difficulty {} (raw: {}) to worker: {} (source: network)",
                                    difficulty_f64, network_difficulty, address
                                );
                                self.write_request("mining.set_difficulty", Some(json!([difficulty_f64])))
                                    .await?;
                                self.write_response(id, Some(true)).await?;
                                self.write_template().await?;
                            }
                            (Some(i), "mining.submit", Some(p)) => {
                                if !self.authorized {
                                    self.write_error_response(i, 24, "Unauthorized worker".into()).await?;
                                    continue;
                                }
                                let (address, id_str, nonce_str): (String, String, String) = serde_json::from_value(p)?;
                                if address != *self.payout_addr.as_ref().unwrap_or(&String::new()) {
                                    self.write_error_response(i, 23, "Unknown worker".into()).await?;
                                    continue;
                                }
                                // Check if a valid template exists (indicating Vecnod connection)
                                if self.recv.borrow().is_none() {
                                    self.write_error_response(i, 25, "Node disconnected, please try again later".into()).await?;
                                    debug!("Share rejected due to no valid template for worker={}", address);
                                    return Ok(());
                                }
                                let job_id = match u8::from_str_radix(&id_str, 16) {
                                    Ok(id) => id,
                                    Err(e) => {
                                        self.write_error_response(i, 21, format!("Invalid job ID: {}", e).into()).await?;
                                        continue;
                                    }
                                };
                                let nonce = match u64::from_str_radix(nonce_str.trim_start_matches("0x"), 16) {
                                    Ok(n) => n,
                                    Err(e) => {
                                        self.write_error_response(i, 21, format!("Invalid nonce: {}", e).into()).await?;
                                        continue;
                                    }
                                };

                                let template = match self.jobs.get_job(job_id).await {
                                    Some(b) => b,
                                    None => {
                                        self.write_error_response(i, 21, "Stale job".into()).await?;
                                        debug!("Stale job: job_id={} for worker={}", job_id, address);
                                        continue;
                                    }
                                };

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