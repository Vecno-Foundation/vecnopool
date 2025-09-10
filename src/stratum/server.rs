//src/stratum/server.rs

use anyhow::Result;
use log::{debug, error, info, warn};
use std::sync::atomic::{AtomicU16, Ordering as AtomicOrdering};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::io::AsyncBufReadExt;
use crate::stratum::protocol::{StratumConn, PayoutNotification};
use crate::stratum::jobs::{Jobs, JobParams, PendingResult};
use crate::treasury::sharehandler::Sharehandler;
use crate::vecnod::{VecnodHandle, RpcBlock};
use crate::database::db::Db;

pub struct Stratum {
    pub last_template: Arc<tokio::sync::RwLock<Option<RpcBlock>>>,
    pub jobs: Arc<Jobs>,
    pub share_handler: Arc<Sharehandler>,
    pub send: watch::Sender<Option<JobParams>>,
    pub payout_notify: broadcast::Sender<PayoutNotification>,
}

impl Stratum {
    pub async fn new(
        addr: &str,
        handle: VecnodHandle,
        pool_address: &str,
        _network_id: &str,
        pool_fee: f64,
    ) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        info!("Listening on {}, pool fee: {}%", addr, pool_fee);
        let last_template = Arc::new(tokio::sync::RwLock::new(None));
        let jobs = Arc::new(Jobs::new(handle));
        let (pending, _) = mpsc::unbounded_channel();
        let (send, recv) = watch::channel(None);
        let (payout_notify, _) = broadcast::channel(100);
        let db = Arc::new(Db::new(std::path::Path::new("pool.db")).await?);
        let share_handler = Arc::new(Sharehandler::new(db, 10000, 600_000, pool_address.to_string(), pool_fee).await?);
        let worker_counter = Arc::new(AtomicU16::new(0));
        
        let jobs_clone = jobs.clone();
        let share_handler_clone = share_handler.clone();
        let last_template_clone = last_template.clone();
        let pending_clone = pending.clone();
        let payout_notify_clone = payout_notify.clone();
        let pool_address_clone = pool_address.to_string();
        
        tokio::spawn(async move {
            loop {
                let worker_id = worker_counter.fetch_add(1, AtomicOrdering::SeqCst);
                if worker_id == 0 {
                    worker_counter.fetch_add(1, AtomicOrdering::SeqCst);
                }
                let worker = worker_id.to_be_bytes();
                debug!("Assigned worker ID: {:?}", worker);

                match listener.accept().await {
                    Ok((mut stream, addr)) => {
                        info!("New connection from {}", addr);
                        let jobs = jobs_clone.clone();
                        let share_handler = share_handler_clone.clone();
                        let last_template = last_template_clone.clone();
                        let pending_send = pending_clone.clone();
                        let payout_notify_recv = payout_notify_clone.subscribe();
                        let (pending_send_inner, pending_recv) = mpsc::unbounded_channel();
                        let mining_addr = pool_address_clone.clone();
                        let client = reqwest::Client::new();
                        let recv_clone = recv.clone();
                        tokio::spawn(async move {
                            let (reader, writer) = stream.split();
                            let mut conn = StratumConn {
                                reader: tokio::io::BufReader::new(reader).lines(),
                                writer,
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
                                mining_addr,
                                client,
                                duplicate_share_count: 0,
                                payout_notify_recv,
                            };
                            debug!("Initialized StratumConn for worker {:?}", addr);
                            if let Err(e) = conn.run().await {
                                error!("Stratum connection error for {}: {}", addr, e);
                                let result = PendingResult {
                                    id: conn.id.into(),
                                    error: Some(format!("Connection closed: {}", e).into_boxed_str()),
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
            payout_notify,
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