use log::{debug, info, warn};
use std::sync::atomic::{AtomicU16, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::path::Path;
use hex;
use tokio::io::AsyncWriteExt;
use serde::Serialize;
use serde_json::json;
use tokio::io::{AsyncBufReadExt, BufReader, Lines};
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch, RwLock};
use anyhow::Result;
use crate::stratum::jobs::{PendingResult, Jobs, JobParams};
use crate::treasury::sharehandler::{Contribution, Sharehandler};
use crate::pow;
use crate::stratum::{Id, Request, Response};
use crate::vecnod::{VecnodHandle, RpcBlock};
use prometheus::{IntCounterVec, register_int_counter_vec};
use lazy_static::lazy_static;
use crate::uint::{u256_to_hex, U256};
use crate::api::fetch_block_details;

const NEW_LINE: &'static str = "\n";

lazy_static! {
    static ref MINER_ADDED_SHARES: IntCounterVec = register_int_counter_vec!(
        "miner_added_shares",
        "Number of valid shares added per miner",
        &["address"]
    ).unwrap();
    static ref MINER_INVALID_SHARES: IntCounterVec = register_int_counter_vec!(
        "miner_invalid_shares",
        "Number of invalid shares per miner",
        &["address"]
    ).unwrap();
    static ref MINER_DUPLICATED_SHARES: IntCounterVec = register_int_counter_vec!(
        "miner_duplicated_shares",
        "Number of duplicated shares per miner",
        &["address"]
    ).unwrap();
}

struct StratumTask {
    listener: TcpListener,
    recv: watch::Receiver<Option<JobParams>>,
    jobs: Jobs,
    share_handler: Arc<Sharehandler>,
    last_template: Arc<RwLock<Option<RpcBlock>>>,
    worker_counter: Arc<AtomicU16>,
}

impl StratumTask {
    async fn run(self) {
        loop {
            let worker_id = self.worker_counter.fetch_add(1, AtomicOrdering::SeqCst);
            if worker_id == 0 {
                self.worker_counter.fetch_add(1, AtomicOrdering::SeqCst);
            }
            let worker = worker_id.to_be_bytes();

            match self.listener.accept().await {
                Ok((mut conn, addr)) => {
                    info!("New connection from {addr}");
                    let recv = self.recv.clone();
                    let jobs = self.jobs.clone();
                    let (pending_send, pending_recv) = mpsc::unbounded_channel();
                    let share_handler = self.share_handler.clone();
                    let last_template = self.last_template.clone();

                    tokio::spawn(async move {
                        let (reader, writer) = conn.split();
                        let conn = StratumConn {
                            reader: BufReader::new(reader).lines(),
                            writer,
                            recv,
                            jobs,
                            pending_send,
                            pending_recv,
                            worker,
                            id: 0,
                            subscribed: false,
                            difficulty: 0,
                            authorized: false,
                            payout_addr: None,
                            share_handler,
                            last_template,
                            extranonce: String::new(),
                        };

                        match conn.run().await {
                            Ok(_) => info!("Connection {addr} closed"),
                            Err(e) => warn!("Connection {addr} closed: {e}"),
                        }
                    });
                }
                Err(e) => {
                    warn!("Error: {e}");
                }
            }
        }
    }
}

pub struct Stratum {
    pub send: watch::Sender<Option<JobParams>>,
    pub jobs: Jobs,
    pub share_handler: Arc<Sharehandler>,
    pub last_template: Arc<RwLock<Option<RpcBlock>>>,
}

impl Stratum {
    pub async fn new(host: &str, handle: VecnodHandle, _pool_address: &str, _network_id: &str) -> Result<Self> {
        let (send, recv) = watch::channel(None);
        let listener = TcpListener::bind(host).await?;
        info!("Listening on {host}");

        let jobs = Jobs::new(handle);
        let db = Arc::new(crate::database::db::Db::new(Path::new("pool.db")).await?);
        let share_handler = Arc::new(Sharehandler::new(db, 10000, 60_000).await?);
        let last_template = Arc::new(RwLock::new(None));
        let worker_counter = Arc::new(AtomicU16::new(0));

        let task = StratumTask {
            listener,
            recv,
            jobs: jobs.clone(),
            share_handler: share_handler.clone(),
            last_template: last_template.clone(),
            worker_counter,
        };
        tokio::spawn(task.run());

        Ok(Stratum {
            send,
            jobs,
            share_handler,
            last_template,
        })
    }

    pub async fn broadcast(&self, template: RpcBlock) {
        if let Some(job) = self.jobs.insert(template.clone()).await {
            *self.last_template.write().await = Some(template);
            debug!("Broadcasting new job: {:?}", job);
            let _ = self.send.send(Some(job));
        }
    }

    pub async fn resolve_pending_job(&self, error: Option<Box<str>>) {
        self.jobs.resolve_pending(error).await
    }
}

struct StratumConn<'a> {
    reader: Lines<BufReader<ReadHalf<'a>>>,
    writer: WriteHalf<'a>,
    recv: watch::Receiver<Option<JobParams>>,
    jobs: Jobs,
    pending_send: mpsc::UnboundedSender<PendingResult>,
    pending_recv: mpsc::UnboundedReceiver<PendingResult>,
    worker: [u8; 2],
    id: u64,
    subscribed: bool,
    difficulty: u64,
    authorized: bool,
    payout_addr: Option<String>,
    share_handler: Arc<Sharehandler>,
    last_template: Arc<RwLock<Option<RpcBlock>>>,
    extranonce: String,
}

impl<'a> StratumConn<'a> {
    async fn write_template(&mut self) -> Result<()> {
        debug!("Sending template to worker {:?}", self.payout_addr);
        let (difficulty, params) = {
            let borrow = self.recv.borrow();
            match borrow.as_ref() {
                Some(j) => (j.difficulty(), j.to_value()),
                None => {
                    debug!("No job template available for worker {:?}", self.payout_addr);
                    return Ok(());
                }
            }
        };
        self.write_request("mining.notify", Some(params)).await?;

        if self.difficulty != difficulty {
            self.difficulty = difficulty;
            let difficulty_f64 = (self.difficulty as f64) / ((1u64 << 32) as f64);
            debug!("Sending difficulty {} to worker {:?}", difficulty_f64, self.payout_addr);
            self.write_request("mining.set_difficulty", Some(json!([difficulty_f64])))
                .await?;
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
        debug!("Writing to miner {:?}: {}", self.payout_addr, String::from_utf8_lossy(&data));
        self.writer.write_all(&data).await?;
        self.writer.write_all(NEW_LINE.as_bytes()).await?;
        Ok(())
    }

    async fn run(mut self) -> Result<()> {
        debug!("Initialized connection with sharehandler");
        debug!("Last template: {:?}", self.last_template.read().await.is_some());

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
                item = self.pending_recv.recv() => {
                    let res = item.expect("channel is always open").into_response()?;
                    self.write(&res).await?;
                },
                res = read(&mut self.reader) => match res {
                    Ok(Some(msg)) => {
                        debug!("Processing message for worker {:?}", self.payout_addr);
                        match (msg.id, &*msg.method, msg.params) {
                            (Some(id), "mining.subscribe", _) => {
                                debug!("Worker subscribed: {:?}", self.payout_addr);
                                self.subscribed = true;
                                self.extranonce = hex::encode(&self.worker);
                                self.write_response(id, Some(true)).await?;
                                self.write_request(
                                    "set_extranonce",
                                    Some(json!([self.extranonce, 6u64])),
                                ).await?;
                                self.write_template().await?;
                            }
                            (Some(id), "mining.authorize", Some(p)) => {
                                let params: Vec<String> = serde_json::from_value(p)?;
                                if params.len() < 1 || !params[0].starts_with("vecno:") || params[0].len() < 10 {
                                    self.write_error_response(id, 21, "Invalid address format".into()).await?;
                                    continue;
                                }
                                self.payout_addr = Some(params[0].clone());
                                self.authorized = true;
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
                                    self.write_error_response(id, 23, "Unknown worker".into()).await?;
                                    continue;
                                }
                                let (total_submissions, window_submissions) = self.share_handler.get_share_counts(&address).await
                                    .map_err(|e| anyhow::anyhow!("Failed to get share counts: {}", e))?;
                                debug!("Sending share counts for {}: total={}, window={}", address, total_submissions, window_submissions);
                                self.write_response(id, Some(json!([total_submissions, window_submissions]))).await?;
                            }
                            (Some(id), "mining.configure", Some(_)) => {
                                debug!("Received mining.configure for worker {:?}", self.payout_addr);
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
                                        debug!("Stale job_id={} for worker {}", job_id, address);
                                        continue;
                                    }
                                };

                                let contribution = Contribution {
                                    address: address.clone(),
                                    difficulty: self.difficulty as i64,
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
                                    extranonce: self.extranonce.clone(),
                                    nonce: format!("{:016x}", nonce),
                                };
                                if !self.share_handler.validate_share(&contribution, &self.jobs, &self.extranonce, &format!("{:016x}", nonce)).await? {
                                    MINER_DUPLICATED_SHARES.with_label_values(&[&address]).inc();
                                    self.write_error_response(i, 22, "Invalid or duplicate share".into()).await?;
                                    debug!("Share rejected for worker {}: job_id={}, extranonce={}, nonce={}", address, job_id, self.extranonce, nonce);
                                    continue;
                                }

                                let log_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    let mut header = template.header.clone().unwrap();
                                    header.nonce = nonce;
                                    let pow_hash = header.hash(false)?;
                                    let pow_u256 = U256::from_little_endian(pow_hash.as_bytes());
                                    let network_target = pow::u256_from_compact_target(header.bits);
                                    let difficulty_f64 = (self.difficulty as f64) / ((1u64 << 32) as f64);

                                    debug!(
                                        "Share submitted: job_id={}, nonce={:016x}, pow_u256={}, network_target={}, pool_difficulty={}, difficulty_f64={}",
                                        job_id, nonce, u256_to_hex(&pow_u256), u256_to_hex(&network_target), self.difficulty, difficulty_f64
                                    );

                                    Ok::<(), anyhow::Error>(())
                                }));

                                if let Err(_) = log_result {
                                    warn!("Diagnostic logging panicked for job_id={}", job_id);
                                    self.write_error_response(i, 23, "Internal server error".into()).await?;
                                    continue;
                                }

                                let block_hash = {
                                    let mut header = template.header.clone().unwrap();
                                    header.nonce = nonce;
                                    let pow_hash = header.hash(false)?;
                                    hex::encode(pow_hash.as_bytes())
                                };

                                if self.jobs.submit(i.clone(), job_id, nonce, block_hash.clone(), self.pending_send.clone()).await {
                                    MINER_ADDED_SHARES.with_label_values(&[&address]).inc();
                                    info!("Share accepted: job_id={} for worker {}", job_id, address);
                                    if let Err(e) = self.share_handler.record_share(contribution).await {
                                        warn!("Failed to record share: {}", e);
                                    }

                                    // Fetch block details and distribute rewards
                                    let (reward_block_hash, daa_score) = match fetch_block_details(&block_hash).await {
                                        Ok(result) => result,
                                        Err(e) => {
                                            warn!("Failed to fetch block details: {}", e);
                                            (block_hash.clone(), 0)
                                        }
                                    };

                                    if let Err(e) = self.share_handler
                                        .distribute_rewards(
                                            block_hash.clone(),
                                            reward_block_hash,
                                            daa_score,
                                            template.transactions.get(0).and_then(|tx| tx.outputs.get(0)).map(|output| output.amount),
                                        )
                                        .await
                                    {
                                        warn!("Failed to distribute rewards: {}", e);
                                    }

                                    let (total_submissions, window_submissions) = self.share_handler.get_share_counts(&address).await
                                        .map_err(|e| anyhow::anyhow!("Failed to get share counts: {}", e))?;
                                    if self.share_handler.should_log_share(&address, total_submissions).await {
                                        info!(
                                            "Share accepted: job_id={} for worker {}, total submissions: {} (in window: {})",
                                            job_id, address, total_submissions, window_submissions
                                        );
                                        self.share_handler.update_log_time(&address).await;
                                        let params = json!([address.clone(), total_submissions, window_submissions]);
                                        if let Err(e) = self.write_notification("mining.share_update", Some(params)).await {
                                            warn!("Failed to send share_update notification to worker {}: {}", address, e);
                                        }
                                    }
                                    self.write_response(i, Some(true)).await?;
                                } else {
                                    MINER_INVALID_SHARES.with_label_values(&[&address]).inc();
                                    debug!("Unable to submit share for job_id={}: hash={}", job_id, &block_hash);
                                    self.write_error_response(i, 20, "Unable to submit share".into()).await?;
                                    continue;
                                }
                            }
                            _ => {
                                debug!("Got unknown method: {} for worker {:?}", msg.method, self.payout_addr);
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

async fn read(r: &mut Lines<BufReader<ReadHalf<'_>>>) -> Result<Option<Request>> {
    let line = match r.next_line().await? {
        Some(l) => l,
        None => return Ok(None),
    };
    debug!("Received from miner: {}", line);
    Ok(Some(serde_json::from_str(&line)?))
}