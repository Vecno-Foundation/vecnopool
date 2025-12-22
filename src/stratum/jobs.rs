//src/stratum/jobs.rs

use crate::stratum::{Id, Response};
use crate::vecnod::{VecnodHandle, RpcBlock};
use primitive_types::U256;
use anyhow::Result;
use log::{debug, info, warn};
use serde_json::json;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};
use hex;
use dashmap::DashMap;
use std::time::{Instant, Duration};
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};

const MAX_DAA_LAG: u64 = 3;

#[derive(Clone, Debug)]
pub struct Jobs {
    inner: Arc<RwLock<JobsInner>>,
    pending: Arc<Mutex<VecDeque<Pending>>>,
    submitted_hashes: Arc<DashMap<String, Instant>>,
    submission_lock: Arc<Mutex<()>>,
    current_daa_score: Arc<AtomicU64>,
    pub is_synced: Arc<AtomicBool>,
}

impl Jobs {
    pub fn new(
        handle: VecnodHandle,
        current_daa_score: Arc<AtomicU64>,
        is_synced: Arc<AtomicBool>,
    ) -> Self {
        let jobs = Self {
            inner: Arc::new(RwLock::new(JobsInner {
                next: 0,
                handle,
                jobs: Vec::with_capacity(256),
            })),
            pending: Arc::new(Mutex::new(VecDeque::with_capacity(64))),
            submitted_hashes: Arc::new(DashMap::new()),
            submission_lock: Arc::new(Mutex::new(())),
            current_daa_score,
            is_synced,
        };

        {
            let submitted_hashes = jobs.submitted_hashes.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    let now = Instant::now();
                    submitted_hashes.retain(|_, v| now.duration_since(*v) < Duration::from_secs(60));
                }
            });
        }

        jobs
    }

    pub async fn insert(&self, template: RpcBlock) -> Option<JobParams> {
        let header = template.header.as_ref()?.clone();
        let pre_pow = header.pre_pow().ok()?;
        let network_difficulty = header.difficulty();
        let timestamp = header.timestamp as u64;

        let mut w = self.inner.write().await;
        let len = w.jobs.len();
        let id = if len < 256 {
            w.jobs.push(template);
            len as u8
        } else {
            let id = w.next;
            w.jobs[id as usize] = template;
            w.next = id.wrapping_add(1);
            id
        };

        debug!(target: "stratum::jobs",
            "New job broadcast → id={} | network_difficulty={} | daa_score={}",
            id, network_difficulty, header.daa_score
        );

        Some(JobParams {
            id,
            pre_pow,
            network_difficulty,
            timestamp,
        })
    }

    pub async fn submit(
        &self,
        rpc_id: Id,
        job_id: u8,
        nonce: u64,
        miner_address: String,
        send: mpsc::UnboundedSender<PendingResult>,
    ) -> bool {
        let start = Instant::now();
        if !self.is_synced.load(Ordering::Relaxed) {
            warn!(
                target: "stratum::jobs",
                "BLOCK SUBMISSION REJECTED: Node not synced → miner={} job_id={} nonce={:016x}",
                miner_address, job_id, nonce
            );
            Pending { id: rpc_id, send }.resolve(Some("node not synced".into()));
            return false;
        }

        let _lock = match tokio::time::timeout(Duration::from_millis(100), self.submission_lock.lock()).await {
            Ok(lock) => lock,
            Err(_) => {
                debug!(target: "stratum::jobs", "Submission lock timeout job_id={}", job_id);
                Pending { id: rpc_id, send }.resolve(Some("timeout".into()));
                return false;
            }
        };

        let (block, handle, block_hash_hex, template_daa_score) = {
            let r = self.inner.read().await;
            let Some(template) = r.jobs.get(job_id as usize) else {
                Pending { id: rpc_id, send }.resolve(Some("stale job".into()));
                return false;
            };

            let template_daa_score = template.header.as_ref().map(|h| h.daa_score).unwrap_or(0);
            let current_daa_approx = self.current_daa_score.load(Ordering::Relaxed);

            if template_daa_score + MAX_DAA_LAG < current_daa_approx {
                info!(
                    target: "stratum::jobs",
                    "Rejected potentially stale block: job_id={} daa={} (current ~{}) lag={} miner={}",
                    job_id,
                    template_daa_score,
                    current_daa_approx,
                    current_daa_approx.saturating_sub(template_daa_score),
                    miner_address
                );
                Pending { id: rpc_id, send }.resolve(Some("stale block".into()));
                return false;
            }

            let mut block = template.clone();
            let block_hash_hex = match block.header.as_mut() {
                Some(header) => {
                    header.nonce = nonce;
                    match header.hash(false) {
                        Ok(h) => hex::encode(h.as_bytes()),
                        Err(_) => {
                            Pending { id: rpc_id, send }.resolve(Some("hash error".into()));
                            return false;
                        }
                    }
                }
                None => {
                    Pending { id: rpc_id, send }.resolve(Some("no header".into()));
                    return false;
                }
            };

            let nonce_hex = format!("{:016x}", nonce);
            let key = format!("{}:{}", block_hash_hex, nonce_hex);

            let now = Instant::now();
            if let Some(entry) = self.submitted_hashes.get(&key) {
                if now.duration_since(*entry.value()) < Duration::from_secs(10) {
                    warn!(target: "stratum::jobs", "Duplicate block submission blocked: {}", miner_address);
                    Pending { id: rpc_id, send }.resolve(Some("duplicate".into()));
                    return false;
                }
            }
            self.submitted_hashes.insert(key, now);

            (block, r.handle.clone(), block_hash_hex, template_daa_score)
        };

        self.pending.lock().await.push_back(Pending { id: rpc_id, send });

        debug!(
            target: "stratum::jobs",
            "BLOCK FOUND & SUBMITTED → miner={} | job_id={} | nonce={:016x} | hash={} | daa_score={}",
            miner_address, job_id, nonce, block_hash_hex, template_daa_score
        );

        let handle2 = handle.clone();
        let block2 = block.clone();
        let miner = miner_address.clone();
        tokio::spawn(async move {
            handle2.submit_block(block2);
            debug!("BLOCK SUCCESSFULLY SUBMITTED TO NODE by {}", miner);
        });

        debug!(target: "stratum::jobs", "Block submission queued in {:?}", start.elapsed());
        true
    }

    pub async fn resolve_pending(&self, error: Option<Box<str>>) {
        let mut pending = self.pending.lock().await;
        if let Some(p) = pending.pop_front() {
            p.resolve(error);
        }
    }

    pub async fn get_job(&self, job_id: u8) -> Option<RpcBlock> {
        let r = self.inner.read().await;
        r.jobs.get(job_id as usize).cloned()
    }

    pub async fn get_job_params(&self, job_id: u8) -> Option<JobParams> {
        let r = self.inner.read().await;
        let job = r.jobs.get(job_id as usize)?;
        let header = job.header.as_ref()?.clone();
        let pre_pow = header.pre_pow().ok()?;
        Some(JobParams {
            id: job_id,
            pre_pow,
            network_difficulty: header.difficulty(),
            timestamp: header.timestamp as u64,
        })
    }
}

#[derive(Debug)]
struct JobsInner {
    next: u8,
    handle: VecnodHandle,
    jobs: Vec<RpcBlock>,
}

#[derive(Debug, Clone)]
pub struct JobParams {
    pub id: u8,
    pub pre_pow: U256,
    pub network_difficulty: u64,
    pub timestamp: u64,
}

impl JobParams {
    pub fn network_difficulty(&self) -> u64 { self.network_difficulty }

    pub fn to_value(&self) -> serde_json::Value {
        json!([
            hex::encode([self.id]),
            self.pre_pow.0.as_slice(),
            self.timestamp
        ])
    }
}

#[derive(Debug)]
struct Pending {
    id: Id,
    send: mpsc::UnboundedSender<PendingResult>,
}

impl Pending {
    fn resolve(self, error: Option<Box<str>>) {
        let _ = self.send.send(PendingResult {
            id: self.id,
            error,
            block_hash: None,
        });
    }
}

#[derive(Debug)]
pub struct PendingResult {
    pub id: Id,
    pub error: Option<Box<str>>,
    pub block_hash: Option<String>,
}

impl PendingResult {
    pub fn into_response(self) -> Result<Response> {
        match self.error {
            Some(e) => Response::err(self.id, 20, e),
            None => Response::ok(self.id, json!({"accepted": true, "block_hash": self.block_hash})),
        }
    }
}