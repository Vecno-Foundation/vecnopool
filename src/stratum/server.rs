use anyhow::Result;
use log::{debug, error, info, warn};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::io::AsyncBufReadExt;
use crate::stratum::protocol::StratumConn;
use crate::stratum::jobs::{Jobs, JobParams, PendingResult};
use crate::treasury::sharehandler::Sharehandler;
use crate::vecnod::{VecnodHandle, RpcBlock};
use crate::database::db::Db;

pub struct Stratum {
    pub last_template: Arc<tokio::sync::RwLock<Option<RpcBlock>>>,
    pub jobs: Arc<Jobs>,
    pub share_handler: Arc<Sharehandler>,
    pub send: watch::Sender<Option<JobParams>>,
}

impl Stratum {
    pub async fn new(
        addr: &str,
        handle: VecnodHandle,
        pool_fee: f64,
        window_time_ms: u64,
    ) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        info!(
            "Listening on {}, pool fee: {}%, window_time_ms: {}ms ({}s)",
            addr,
            pool_fee,
            window_time_ms,
            window_time_ms / 1000
        );

        let last_template = Arc::new(tokio::sync::RwLock::new(None));
        let db = Arc::new(Db::new().await?);
        let jobs = Arc::new(Jobs::new(handle));
        let (pending, _) = mpsc::unbounded_channel();
        let (send, recv) = watch::channel(None);
        let share_handler = Arc::new(
            Sharehandler::new(db, pool_fee, jobs.clone(), window_time_ms)
                .await?,
        );
        let worker_counter = Arc::new(AtomicU32::new(1));

        let jobs_clone = jobs.clone();
        let share_handler_clone = share_handler.clone();
        let last_template_clone = last_template.clone();
        let pending_clone = pending.clone();

        tokio::spawn(async move {
            loop {
                let worker_id = worker_counter.fetch_add(1, AtomicOrdering::SeqCst);
                if worker_id == 0 {
                    worker_counter.fetch_add(1, AtomicOrdering::SeqCst);
                    debug!("Skipped worker_id=0 to avoid zeroed extranonce");
                }
                let worker = worker_id.to_be_bytes();
                let worker_hex = format!("{:08x}", worker_id);
                debug!(
                    "Assigned worker ID: raw={:?}, hex={}, worker_id={} for new connection",
                    worker, worker_hex, worker_id
                );

                match listener.accept().await {
                    Ok((mut stream, addr)) => {
                        info!("New connection from {}", addr);
                        let jobs = jobs_clone.clone();
                        let share_handler = share_handler_clone.clone();
                        let last_template = last_template_clone.clone();
                        let pending_send = pending_clone.clone();
                        let (pending_send_inner, pending_recv) = mpsc::unbounded_channel();
                        let recv_clone = recv.clone();
                        tokio::spawn(async move {
                            let (reader, writer) = stream.split();
                            let mut conn = StratumConn {
                                reader: tokio::io::BufReader::new(reader).lines(),
                                writer: Arc::new(Mutex::new(writer)),
                                recv: recv_clone,
                                jobs,
                                pending_send: pending_send_inner,
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
                                duplicate_share_count: Arc::new(AtomicU64::new(0)),
                            };
                            debug!(
                                "Initialized StratumConn for worker: addr={}, worker_id={}, worker_bytes={:?}, worker_hex={}",
                                addr, worker_id, conn.worker, worker_hex
                            );
                            if let Err(e) = conn.run().await {
                                error!("Stratum connection error for {}: {}", addr, e);
                                let result = PendingResult {
                                    id: conn.id.into(),
                                    error: Some(format!("Connection closed: {}", e).into_boxed_str()),
                                    block_hash: None,
                                };
                                let _ = pending_send.send(result);
                            }
                        });
                    }
                    Err(e) => error!("Failed to accept connection: {}", e),
                }
            }
        });

        Ok(Stratum {
            last_template,
            jobs,
            share_handler,
            send,
        })
    }

    pub async fn broadcast(&self, template: RpcBlock) {
        if let Some(job) = self.jobs.insert(template.clone()).await {
            *self.last_template.write().await = Some(template);
            debug!("Broadcasting new job: id={}", job.id);
            let _ = self.send.send(Some(job));
        }
    }

    pub async fn resolve_pending_job(&self, error: Option<anyhow::Error>) {
        if let Some(e) = &error {
            warn!("Error resolving pending job: {}", e);
        }
        let error_str = error.map(|e| e.to_string().into_boxed_str());
        self.jobs.resolve_pending(error_str).await;
    }
}