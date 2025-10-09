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
    submission_lock: Arc<Mutex<()>>,
}

impl Jobs {
    pub fn new(handle: VecnodHandle, db: Arc<Db>, pool_address: String, pool_fee: f64) -> Self {
        let jobs = Self {
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
            submission_lock: Arc::new(Mutex::new(())),
        };
        // Start background task for cleaning up submitted_hashes
        tokio::spawn({
            let submitted_hashes = Arc::clone(&jobs.submitted_hashes);
            async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    let now = Instant::now();
                    submitted_hashes.retain(|_, v| now.duration_since(*v) < Duration::from_secs(60));
                }
            }
        });
        jobs
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
        let start = Instant::now();
        // Acquire lock with timeout
        let _lock = match tokio::time::timeout(Duration::from_millis(100), self.submission_lock.lock()).await {
            Ok(lock) => lock,
            Err(_) => {
                debug!(target: "stratum::jobs", "Submission lock timeout for job_id={}", job_id);
                let pending = Pending { id: rpc_id, send, block_hash: "timeout".to_string() };
                pending.resolve(Some(Box::from("Submission lock timeout")));
                return false;
            }
        };

        // Critical section: validate job and check for duplicates
        let (submission_key, reward_block_hash, block, handle, network_difficulty) = {
            let r = self.inner.read().await;
            let block = match r.jobs.get(job_id as usize) {
                Some(b) => b.clone(),
                None => {
                    debug!(target: "stratum::jobs", "No job found for job_id={}", job_id);
                    return false;
                }
            };
            let difficulty = block.header.as_ref().map(|h| h.difficulty()).unwrap_or(0);
            let mut block = block;
            let reward_block_hash = if let Some(header) = &mut block.header {
                header.nonce = nonce;
                match header.hash(false) {
                    Ok(hash) => hex::encode(hash.as_bytes()),
                    Err(e) => {
                        debug!(target: "stratum::jobs", "Failed to compute block hash for job_id={}: {}", job_id, e);
                        return false;
                    }
                }
            } else {
                debug!(target: "stratum::jobs", "No header found for job_id={}", job_id);
                return false;
            };
            let nonce_hex = format!("{:016x}", nonce);
            let submission_key = format!("{}:{}", reward_block_hash, nonce_hex);
            let now = Instant::now();
            if let Some(entry) = self.submitted_hashes.get(&submission_key) {
                if now.duration_since(*entry.value()) < Duration::from_secs(10) {
                    warn!(target: "stratum::jobs",
                        "Duplicate block submission detected: job_id={}, block_hash={}, nonce={}, miner={}",
                        job_id, reward_block_hash, nonce_hex, miner_address
                    );
                    let pending = Pending { id: rpc_id, send, block_hash: reward_block_hash };
                    pending.resolve(Some(Box::from("Duplicate block submission")));
                    return false;
                }
            }
            self.submitted_hashes.insert(submission_key.clone(), now);
            (submission_key, reward_block_hash, block, r.handle.clone(), difficulty)
        };

        // Perform slow operations outside the lock
        if let Some(header) = &block.header {
            let block_difficulty = header.difficulty();
            if block_difficulty < network_difficulty {
                warn!(target: "stratum::jobs",
                    "Block rejected: job_id={}, block_hash=pending, miner={}, difficulty={} below network_difficulty={}",
                    job_id, miner_address, block_difficulty, network_difficulty
                );
                let pending = Pending { id: rpc_id, send, block_hash: reward_block_hash.clone() };
                pending.resolve(Some(Box::from(format!(
                    "Difficulty {} below network minimum {}",
                    block_difficulty, network_difficulty
                ))));
                self.submitted_hashes.remove(&submission_key);
                return false;
            }
        }

        // Calculate reward from the first output of the coinbase transaction
        let reward = match block.transactions.get(0) {
            Some(coinbase_tx) if coinbase_tx.inputs.is_empty() => {
                match coinbase_tx.outputs.get(0) {
                    Some(output) => ((output.amount as f64) * (1.0 - self.pool_fee / 100.0)) as u64,
                    None => {
                        debug!(target: "stratum::jobs", "No outputs in coinbase transaction for job_id={}", job_id);
                        self.submitted_hashes.remove(&submission_key);
                        return false;
                    }
                }
            }
            _ => {
                info!(target: "stratum::jobs", "No valid coinbase transaction found for job_id={}", job_id);
                self.submitted_hashes.remove(&submission_key);
                return false;
            }
        };

        let daa_score = block.header.as_ref().map(|h| h.daa_score).unwrap_or(0);
        // Offload database write to a background task
        let db = Arc::clone(&self.db);
        let reward_block_hash_clone = reward_block_hash.clone();
        let miner_address_clone = miner_address.clone();
        let extranonce_clone = extranonce.clone();
        let pool_address_clone = self.pool_address.clone();
        tokio::spawn(async move {
            if let Err(e) = db.add_block_details(
                &reward_block_hash_clone,
                &miner_address_clone,
                &reward_block_hash_clone,
                job_id,
                &extranonce_clone,
                &format!("{:016x}", nonce),
                daa_score,
                &pool_address_clone,
                reward,
            ).await {
                debug!(target: "stratum::jobs", "Failed to store block submission for job_id={}: {}", job_id, e);
            }
        });

        let mut pending = self.pending.lock().await;
        pending.push_back(Pending { id: rpc_id, send, block_hash: reward_block_hash.clone() });
        info!(target: "stratum::jobs",
            "Submitted block: job_id={}, block_hash={}, nonce={}, miner={}, difficulty={}",
            job_id, reward_block_hash, format!("{:016x}", nonce), miner_address, network_difficulty
        );

        // Submit block asynchronously
        tokio::spawn({
            let handle = handle.clone();
            let block = block.clone();
            async move { handle.submit_block(block); }
        });

        info!(target: "stratum::jobs", "Submission processed in {:?}", start.elapsed());
        true
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