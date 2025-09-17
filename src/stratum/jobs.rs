//src/stratum/jobs.rs

use crate::stratum::{Id, Response};
use crate::vecnod::{VecnodHandle, RpcBlock};
use crate::uint::U256;
use anyhow::Result;
use log::{debug, info, warn};
use serde_json::json;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};
use crate::database::db::Db;
use crate::treasury::reward_table::get_block_reward;
use hex;
use dashmap::DashMap;
use std::time::{Instant, Duration};

#[derive(Clone, Debug)]
pub struct Jobs {
    inner: Arc<RwLock<JobsInner>>,
    pending: Arc<Mutex<VecDeque<Pending>>>,
    db: Arc<Db>,
    pool_address: String,
    pool_fee: f64,
    submitted_hashes: Arc<DashMap<String, Instant>>,
}

impl Jobs {
    pub fn new(handle: VecnodHandle, db: Arc<Db>, pool_address: String, pool_fee: f64) -> Self {
        Self {
            inner: Arc::new(RwLock::new(JobsInner {
                next: 0,
                jobs: Vec::with_capacity(256),
                handle,
            })),
            pending: Arc::new(Mutex::new(VecDeque::with_capacity(64))),
            db,
            pool_address,
            pool_fee,
            submitted_hashes: Arc::new(DashMap::new()),
        }
    }

    pub async fn insert(&self, template: RpcBlock) -> Option<JobParams> {
        let header = template.header.as_ref()?;
        let pre_pow = header.pre_pow().ok()?;
        let difficulty = header.difficulty();
        let timestamp = header.timestamp as u64;

        let mut w = self.inner.write().await;
        let len = w.jobs.len();
        let id = if len < 256 {
            w.jobs.push(template);
            len as u8
        } else {
            let id = w.next;
            w.jobs[id as usize] = template;
            id
        };
        w.next = id.wrapping_add(1);

        debug!(target: "stratum::jobs", "Inserted job: id={}, difficulty={}", id, difficulty);

        Some(JobParams {
            id,
            pre_pow,
            difficulty,
            timestamp,
        })
    }

    pub async fn submit(
        &self,
        rpc_id: Id,
        job_id: u8,
        nonce: u64,
        miner_address: String,
        extranonce: String,
        send: mpsc::UnboundedSender<PendingResult>,
    ) -> bool {
        let (mut block, handle, network_difficulty) = {
            let r = self.inner.read().await;
            let block = match r.jobs.get(job_id as usize) {
                Some(b) => b.clone(),
                None => {
                    debug!(target: "stratum::jobs", "No job found for job_id={}", job_id);
                    return false;
                }
            };
            let difficulty = block.header.as_ref().map(|h| h.difficulty()).unwrap_or(0);
            (block, r.handle.clone(), difficulty)
        };

        if let Some(header) = &block.header {
            let block_difficulty = header.difficulty();
            if block_difficulty < network_difficulty {
                warn!(target: "stratum::jobs",
                    "Block rejected: job_id={}, block_hash=pending, miner={}, difficulty={} below network_difficulty={}",
                    job_id, miner_address, block_difficulty, network_difficulty
                );
                let pending = Pending {
                    id: rpc_id,
                    send,
                    block_hash: "rejected_pre_hash".to_string(),
                };
                pending.resolve(Some(Box::from(format!(
                    "Difficulty {} below network minimum {}",
                    block_difficulty, network_difficulty
                ))));
                return false;
            }
        }

        if let Some(header) = &mut block.header {
            header.nonce = nonce;
            let reward_block_hash = match header.hash(false) {
                Ok(hash) => hex::encode(hash.as_bytes()),
                Err(e) => {
                    debug!(target: "stratum::jobs", "Failed to compute block hash for job_id={}: {}", job_id, e);
                    return false;
                }
            };

            // Check for duplicate block submission *before* adding to database
            let now = Instant::now();
            if let Some(entry) = self.submitted_hashes.get(&reward_block_hash) {
                if now.duration_since(*entry.value()) < Duration::from_secs(10) {
                    warn!(target: "stratum::jobs",
                        "Duplicate block submission detected: job_id={}, block_hash={}, miner={}",
                        job_id, reward_block_hash, miner_address
                    );
                    let pending = Pending {
                        id: rpc_id,
                        send,
                        block_hash: reward_block_hash.clone(),
                    };
                    pending.resolve(Some(Box::from("Duplicate block submission")));
                    return false;
                }
            }

            // Insert into submitted_hashes *before* database operation
            self.submitted_hashes.insert(reward_block_hash.clone(), now);

            // Clean up old entries to prevent memory growth
            self.submitted_hashes.retain(|_, v| now.duration_since(*v) < Duration::from_secs(60));

            let daa_score = header.daa_score;
            let reward = get_block_reward(daa_score)
                .map(|r| ((r as f64) * (1.0 - self.pool_fee / 100.0)) as u64)
                .unwrap_or(0);

            // Add block details to database only if not a duplicate
            if let Err(e) = self.db.add_block_details(
                &reward_block_hash,
                &miner_address,
                &reward_block_hash,
                job_id,
                &extranonce,
                &format!("{:016x}", nonce),
                daa_score,
                &self.pool_address,
                reward,
            ).await {
                debug!(target: "stratum::jobs", "Failed to store block submission for job_id={}: {}", job_id, e);
                // Remove from submitted_hashes to allow retry
                self.submitted_hashes.remove(&reward_block_hash);
                return false;
            }

            let mut pending = self.pending.lock().await;
            pending.push_back(Pending {
                id: rpc_id,
                send,
                block_hash: reward_block_hash.clone(),
            });
            info!(target: "stratum::jobs",
                "Submitted block: job_id={}, block_hash={}, miner={}, difficulty={}",
                job_id, reward_block_hash, miner_address, network_difficulty
            );
            handle.submit_block(block);
            true
        } else {
            debug!(target: "stratum::jobs", "No header found for job_id={}", job_id);
            false
        }
    }

    pub async fn resolve_pending(&self, error: Option<Box<str>>) {
        let pending = self.pending.lock().await.pop_front();
        if let Some(pending) = pending {
            if error.is_some() {
                if let Err(e) = self.db.update_block_status(&pending.block_hash, false).await {
                    debug!(target: "stratum::jobs", "Failed to update block status for hash={}: {}", pending.block_hash, e);
                }
            } else {
                if let Err(e) = self.db.update_block_status(&pending.block_hash, true).await {
                    debug!(target: "stratum::jobs", "Failed to update block status for hash={}: {}", pending.block_hash, e);
                }
            }
            pending.resolve(error);
        } else {
            debug!(target: "stratum::jobs", "Resolve: nothing is pending");
        }
    }

    pub async fn get_job(&self, job_id: u8) -> Option<RpcBlock> {
        let r = self.inner.read().await;
        r.jobs.get(job_id as usize).cloned()
    }

    pub async fn get_job_params(&self, job_id: u8) -> Option<JobParams> {
        let r = self.inner.read().await;
        r.jobs.get(job_id as usize).and_then(|job| {
            let header = job.header.as_ref()?;
            let pre_pow = header.pre_pow().ok()?;
            Some(JobParams {
                id: job_id,
                pre_pow,
                difficulty: header.difficulty(),
                timestamp: header.timestamp as u64,
            })
        })
    }
}

#[derive(Debug)]
struct JobsInner {
    next: u8,
    handle: VecnodHandle,
    jobs: Vec<RpcBlock>,
}

#[derive(Debug)]
pub struct JobParams {
    pub id: u8,
    pub pre_pow: U256,
    pub difficulty: u64,
    pub timestamp: u64,
}

impl JobParams {
    pub fn difficulty(&self) -> u64 {
        self.difficulty
    }

    pub fn to_value(&self) -> serde_json::Value {
        json!([
            hex::encode([self.id]),
            self.pre_pow.as_slice(),
            self.timestamp
        ])
    }
}

#[derive(Debug)]
pub struct Pending {
    id: Id,
    send: mpsc::UnboundedSender<PendingResult>,
    block_hash: String,
}

impl Pending {
    pub fn resolve(self, error: Option<Box<str>>) {
        let result = PendingResult { id: self.id, error };
        let _ = self.send.send(result);
    }
}

pub struct PendingResult {
    pub id: Id,
    pub error: Option<Box<str>>,
}

impl PendingResult {
    pub fn into_response(self) -> Result<Response> {
        match self.error {
            Some(e) => Response::err(self.id, 20, e),
            None => Response::ok(self.id, true),
        }
    }
}