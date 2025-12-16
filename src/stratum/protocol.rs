// src/stratum/protocol.rs

use anyhow::Result;
use serde_json::json;
use tokio::io::{AsyncWriteExt, BufReader, Lines};
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::sync::{mpsc, watch, Mutex};
use std::collections::HashSet;
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use std::time::Instant;
use crate::stratum::{Id, Request, Response};
use crate::stratum::jobs::{Jobs, JobParams, PendingResult};
use crate::treasury::sharehandler::Sharehandler;
use crate::config::DifficultyConfig;
use crate::stratum::variable_difficulty::VariableDifficulty;
use crate::stratum::worker_stats::WorkerStats;
use crate::pow::{u256_from_compact_target_bits, difficulty};
use log::{info, warn};
use once_cell::sync::Lazy;
use dashmap::DashSet;
use hex;

const NEW_LINE: &'static str = "\n";
static SUBMITTED_BLOCK_HASHES: Lazy<Arc<DashSet<String>>> = Lazy::new(|| Arc::new(DashSet::new()));

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
    pub extranonce: String,
    pub diff_config: Arc<DifficultyConfig>,
    pub last_share_time: Instant,
    pub window_start: Instant,
    pub current_network_diff: Arc<AtomicU64>,
    pub var_diff: Arc<VariableDifficulty>,
    pub worker_stats: Option<Arc<tokio::sync::RwLock<WorkerStats>>>,
    pub submitted_nonces: Arc<Mutex<HashSet<(u8, u64)>>>,
}

impl<'a> StratumConn<'a> {
    async fn maybe_update_difficulty(&mut self) -> Result<()> {
        let new_diff = if let Some(stats) = &self.worker_stats {
            let stats_read = stats.read().await;
            if stats_read.current_diff != self.difficulty {
                Some(stats_read.current_diff)
            } else {
                None
            }
        } else {
            None
        };

        if let Some(diff) = new_diff {
            self.difficulty = diff;
            let diff_f64 = self.difficulty as f64 / 4_294_967_296.0;
            let _ = self.write_request("mining.set_difficulty", Some(json!([diff_f64]))).await?;
            info!(
                "Difficulty updated → {} | new_diff={}",
                self.payout_addr.as_deref().unwrap_or("?"),
                self.difficulty
            );
        }
        Ok(())
    }

    pub async fn write_template(&mut self) -> Result<()> {
        self.maybe_update_difficulty().await?;
        let current_job = self.recv.borrow().clone();
        if let Some(job) = current_job {
            self.write_request("mining.notify", Some(job.to_value())).await
        } else {
            Ok(())
        }
    }

    async fn write_request(&mut self, method: &'static str, params: Option<serde_json::Value>) -> Result<()> {
        self.id += 1;
        let req = Request {
            id: Some(self.id.into()),
            method: method.into(),
            params,
        };
        self.write(&req).await
    }

    async fn write_response<T: serde::Serialize>(&mut self, id: Id, result: Option<T>) -> Result<()> {
        let res = Response::ok(id, result)?;
        self.write(&res).await
    }

    async fn write_error_response(&mut self, id: Id, code: u64, message: &str) -> Result<()> {
        let res = Response::err(id, code, message.into())?;
        self.write(&res).await
    }

    async fn write<T: serde::Serialize>(&mut self, data: &T) -> Result<()> {
        let data = serde_json::to_vec(data)?;
        let mut writer = self.writer.lock().await;
        writer.write_all(&data).await?;
        writer.write_all(NEW_LINE.as_bytes()).await?;
        writer.flush().await?;
        Ok(())
    }

    pub async fn run(&mut self) -> Result<()> {
        let now = Instant::now();
        self.last_share_time = now;
        self.window_start = now;

        if self.recv.borrow().is_none() {
            self.write_error_response(Id::Number(0), 25, "Node disconnected").await?;
            return Ok(());
        }

        loop {
            tokio::select! {
                _ = self.recv.changed() => {
                    if self.subscribed {
                        let _ = self.write_template().await;
                    }
                }

                item = self.pending_recv.recv() => {
                    if let Some(res) = item {
                        let _ = self.write(&res.into_response()?);
                    }
                }

                line = read(&mut self.reader) => {
                    match line? {
                        Some(msg) => {
                            match (msg.id, &*msg.method, msg.params) {
                                (Some(id), "mining.subscribe", Some(_)) => {
                                    self.subscribed = true;
                                    self.extranonce = format!("{:08x}", u32::from_be_bytes(self.worker));
                                    self.write_response(id, Some(true)).await?;
                                    self.write_request("mining.set_extranonce", Some(json!([self.extranonce, 4]))).await?;
                                }

                                (Some(id), "mining.authorize", Some(p)) => {
                                    let params: Vec<String> = match serde_json::from_value(p) {
                                        Ok(v) => v,
                                        Err(_) => {
                                            self.write_error_response(id, 21, "Bad params").await?;
                                            continue;
                                        }
                                    };

                                    if params.is_empty() || !params[0].starts_with("vecno:") {
                                        self.write_error_response(id, 21, "Invalid address").await?;
                                        continue;
                                    }

                                    let address = params[0].clone();
                                    self.payout_addr = Some(address.clone());
                                    self.authorized = true;

                                    let network_diff = self.current_network_diff.load(Ordering::Relaxed);

                                    if network_diff > 0 {
                                        let scaled = (network_diff as f64 / self.diff_config.scale_factor as f64).round() as u64;
                                        self.difficulty = scaled.max(network_diff);
                                        info!(
                                            "Dynamic initial difficulty → {} | diff={} (net={} / scale={})",
                                            address, self.difficulty, network_diff, self.diff_config.scale_factor
                                        );
                                    } else {
                                        self.difficulty = self.diff_config.default;
                                        info!("Fallback default difficulty → {} | diff={}", address, self.difficulty);
                                    }

                                    let stats = self.var_diff.get_or_create_stats(&address, &address, self.difficulty);
                                    self.worker_stats = Some(stats);
                                    self.submitted_nonces = Arc::new(Mutex::new(HashSet::new()));

                                    if self.diff_config.enabled {
                                        let diff_f64 = self.difficulty as f64 / 4_294_967_296.0;
                                        let _ = self.write_request("mining.set_difficulty", Some(json!([diff_f64]))).await;
                                    }

                                    self.write_response(id, Some(true)).await?;
                                    self.write_template().await?;
                                }

                                (Some(request_id), "mining.submit", Some(p)) => {
                                    if !self.authorized {
                                        self.write_error_response(request_id, 24, "Unauthorized").await?;
                                        continue;
                                    }

                                    let params: Vec<String> = match serde_json::from_value(p) {
                                        Ok(v) => v,
                                        Err(_) => {
                                            self.write_error_response(request_id, 21, "Invalid params").await?;
                                            continue;
                                        }
                                    };

                                    if params.len() < 3 {
                                        self.write_error_response(request_id, 21, "Missing params").await?;
                                        continue;
                                    }

                                    let worker_name = &params[0];
                                    let job_id_hex = &params[1];
                                    let nonce_hex = params[2].trim_start_matches("0x");

                                    if worker_name != self.payout_addr.as_ref().unwrap() {
                                        self.write_error_response(request_id, 23, "Worker name mismatch").await?;
                                        continue;
                                    }

                                    let job_id = match u8::from_str_radix(job_id_hex, 16) {
                                        Ok(id) => id,
                                        Err(_) => {
                                            self.write_error_response(request_id, 21, "Invalid job id").await?;
                                            continue;
                                        }
                                    };

                                    let nonce = match u64::from_str_radix(nonce_hex, 16) {
                                        Ok(n) => n,
                                        Err(_) => {
                                            self.write_error_response(request_id, 21, "Invalid nonce").await?;
                                            continue;
                                        }
                                    };

                                    let is_duplicate = {
                                        let mut nonces = self.submitted_nonces.lock().await;
                                        !nonces.insert((job_id, nonce))
                                    };

                                    if is_duplicate {
                                        warn!("Duplicate nonce: job={} nonce={:016x}", job_id, nonce);
                                        self.write_error_response(request_id, 22, "Duplicate nonce").await?;
                                        continue;
                                    }

                                    let template = match self.jobs.get_job(job_id).await {
                                        Some(t) => t,
                                        None => {
                                            self.write_error_response(request_id, 21, "Stale job").await?;
                                            continue;
                                        }
                                    };

                                    let job_params = match self.jobs.get_job_params(job_id).await {
                                        Some(p) => p,
                                        None => {
                                            self.write_error_response(request_id, 21, "Job not found").await?;
                                            continue;
                                        }
                                    };

                                    let network_diff = job_params.network_difficulty();

                                    let mut header = template.header.clone().expect("Missing header");
                                    header.nonce = nonce;

                                    let pow_hash = match header.hash(false) {
                                        Ok(h) => h,
                                        Err(_) => {
                                            self.write_error_response(request_id, 22, "Hash error").await?;
                                            continue;
                                        }
                                    };

                                    let block_hash_hex = hex::encode(pow_hash.as_bytes());
                                    let nonce_hex_full = format!("{:016x}", nonce);

                                    if SUBMITTED_BLOCK_HASHES.contains(&block_hash_hex) {
                                        warn!("Duplicate block hash: {}", block_hash_hex);
                                        self.write_error_response(request_id, 22, "Duplicate block").await?;
                                        continue;
                                    }

                                    let actual_diff = difficulty(u256_from_compact_target_bits(header.bits));
                                    let share_threshold = ((network_diff as f64) * 0.95) as u64;
                                    let is_valid_share = actual_diff >= share_threshold;
                                    let is_block_candidate = actual_diff >= network_diff && self.difficulty >= network_diff;

                                    info!(
                                        "Share → {} | job={} | actual={} | pool={} | net={} | threshold={} | valid_share={} | block_candidate={}",
                                        worker_name, job_id, actual_diff, self.difficulty, network_diff,
                                        share_threshold, is_valid_share, is_block_candidate
                                    );

                                    if !is_valid_share {
                                        if let Some(stats) = &self.worker_stats {
                                            let _ = self.var_diff.on_invalid_share(stats.clone()).await;
                                        }
                                        self.write_error_response(request_id, 22, "Low difficulty share").await?;
                                        continue;
                                    }

                                    if let Some(stats) = &self.worker_stats {
                                        let _ = self.var_diff.on_valid_share(stats.clone(), actual_diff).await;
                                    }

                                    let contribution = crate::treasury::sharehandler::Contribution {
                                        address: worker_name.clone(),
                                        difficulty: actual_diff as i64,
                                        timestamp: std::time::SystemTime::now()
                                            .duration_since(std::time::UNIX_EPOCH)
                                            .unwrap()
                                            .as_secs() as i64,
                                        job_id: job_id.to_string(),
                                        daa_score: header.daa_score as i64,
                                        extranonce: self.extranonce.clone(),
                                        nonce: nonce_hex_full.clone(),
                                        reward_block_hash: is_block_candidate.then(|| block_hash_hex.clone()),
                                        pool_difficulty: self.difficulty,
                                    };

                                    let _ = self.share_handler.record_share(&contribution, actual_diff, true).await;

                                    if is_block_candidate {
                                        info!(
                                            "BLOCK FOUND → {} | diff={} ≥ net={} | pool={} ≥ net={} | hash={}",
                                            worker_name, actual_diff, network_diff, self.difficulty, network_diff, block_hash_hex
                                        );
                                        SUBMITTED_BLOCK_HASHES.insert(block_hash_hex.clone());

                                        let submitted = self.jobs.submit(
                                            request_id.clone(),
                                            job_id,
                                            nonce,
                                            worker_name.clone(),
                                            self.pending_send.clone(),
                                        ).await;

                                        if !submitted {
                                            warn!("Block submission failed: stale or invalid template");
                                        }

                                        // Notify miner of block (some clients expect block_hash on success)
                                        let _ = self.pending_send.send(PendingResult {
                                            id: request_id,
                                            error: None,
                                            block_hash: Some(block_hash_hex),
                                        });
                                    } else {
                                        // Normal valid share (or lucky high-diff share below pool target)
                                        if actual_diff >= network_diff {
                                            info!(
                                                "LUCKY HIGH-DIFF SHARE → {} | actual={} ≥ net={} but pool={} < net | recorded as high-value share, NOT submitted",
                                                worker_name, actual_diff, network_diff, self.difficulty
                                            );
                                        } else {
                                            info!(
                                                "Valid share → {} | actual={} ({:.1}% of net) | recorded for rewards",
                                                worker_name, actual_diff, (actual_diff as f64 / network_diff as f64 * 100.0)
                                            );
                                        }

                                        let _ = self.pending_send.send(PendingResult {
                                            id: request_id,
                                            error: None,
                                            block_hash: None,
                                        });
                                    }
                                }

                                _ => {}
                            }
                        }
                        None => break,
                    }
                }
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
    Ok(Some(serde_json::from_str(&line)?))
}