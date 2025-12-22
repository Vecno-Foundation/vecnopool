// src/stratum/server.rs

use anyhow::Result;
use log::{debug, error, info, warn};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicBool, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc, watch, Mutex};
use tokio::time::{sleep, Duration};
use tokio::io::BufReader;
use crate::stratum::protocol::StratumConn;
use crate::stratum::jobs::{Jobs, JobParams, PendingResult};
use crate::treasury::sharehandler::Sharehandler;
use crate::vecnod::{VecnodHandle, RpcBlock};
use crate::database::db::Db;
use crate::config::DifficultyConfig;
use crate::stratum::variable_difficulty::VariableDifficulty;
use std::collections::HashSet;
use tokio::io::AsyncBufReadExt;

pub struct Stratum {
    pub last_template: Arc<tokio::sync::RwLock<Option<RpcBlock>>>,
    pub jobs: Arc<Jobs>,
    pub share_handler: Arc<Sharehandler>,
    pub send: watch::Sender<Option<JobParams>>,
    pub current_network_diff: Arc<AtomicU64>,
}

impl Stratum {
    pub async fn new(
        addr: &str,
        handle: VecnodHandle,
        pool_fee: f64,
        window_time_ms: u64,
        diff_config: DifficultyConfig,
        current_daa_score: Arc<AtomicU64>,
        is_synced: Arc<AtomicBool>,
    ) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        info!(
            "Stratum server listening on {} | Pool fee: {}% | PPLNS window: {}s",
            addr, pool_fee, window_time_ms / 1000
        );

        let last_template = Arc::new(tokio::sync::RwLock::new(None));
        let db = Arc::new(Db::new().await?);

        let current_network_diff = Arc::new(AtomicU64::new(0));

        let jobs = Arc::new(Jobs::new(
            handle.clone(),
            current_daa_score.clone(),
            is_synced.clone(),
        ));

        let (pending, _) = mpsc::unbounded_channel::<PendingResult>();
        let (send, recv) = watch::channel(None);
        let (shutdown_tx, _) = broadcast::channel(16);

        let share_handler = Arc::new(
            Sharehandler::new(
                db.clone(),
                pool_fee,
                jobs.clone(),
                window_time_ms,
                is_synced.clone(),
            )
            .await?,
        );

        let diff_config = Arc::new(diff_config);

        let var_diff = Arc::new(VariableDifficulty::new(
            (*diff_config).clone(),
            current_network_diff.clone(),
        ));
        var_diff.clone().start_thread();

        let worker_counter = Arc::new(AtomicU32::new(1));
        let jobs_clone = jobs.clone();
        let share_handler_clone = share_handler.clone();
        let pending_clone = pending.clone();
        let recv_clone = recv.clone();
        let current_network_diff_for_connections = current_network_diff.clone();
        let diff_config_shared = diff_config.clone();
        let var_diff_shared = var_diff.clone();
        let is_synced_accept = is_synced.clone();
        let shutdown_tx_clone = shutdown_tx.clone();

        tokio::spawn(async move {
            loop {
                if !is_synced_accept.load(AtomicOrdering::Relaxed) {
                    debug!("Node not synced — pausing acceptance of new connections");
                    sleep(Duration::from_secs(5)).await;
                    continue;
                }
                let worker_id = worker_counter.fetch_add(1, AtomicOrdering::SeqCst);
                if worker_id == 0 {
                    worker_counter.fetch_add(1, AtomicOrdering::SeqCst);
                }
                let worker = worker_id.to_be_bytes();

                match listener.accept().await {
                    Ok((stream, addr)) => {
                        info!("New miner connected: {}", addr);
                        let (read_half, write_half) = stream.into_split();

                        let jobs = jobs_clone.clone();
                        let share_handler = share_handler_clone.clone();
                        let pending_send = pending_clone.clone();
                        let (pending_send_inner, pending_recv) = mpsc::unbounded_channel();
                        let recv_conn = recv_clone.clone();
                        let network_diff_conn = current_network_diff_for_connections.clone();
                        let diff_config_conn = diff_config_shared.clone();
                        let var_diff_conn = var_diff_shared.clone();
                        let shutdown_rx = shutdown_tx_clone.subscribe();

                        tokio::spawn(async move {
                            let writer_arc = Arc::new(Mutex::new(write_half));

                            let mut conn = StratumConn {
                                reader: BufReader::new(read_half).lines(),
                                writer: writer_arc,
                                recv: recv_conn,
                                jobs,
                                pending_send: pending_send_inner,
                                pending_recv,
                                worker,
                                id: 0,
                                subscribed: false,
                                difficulty: diff_config_conn.default,
                                authorized: false,
                                payout_addr: None,
                                share_handler,
                                extranonce: String::new(),
                                diff_config: diff_config_conn.clone(),
                                last_share_time: Instant::now(),
                                window_start: Instant::now(),
                                current_network_diff: network_diff_conn,
                                var_diff: var_diff_conn,
                                worker_stats: None,
                                submitted_nonces: Arc::new(Mutex::new(HashSet::new())),
                                shutdown_rx,
                            };

                            if let Err(e) = conn.run().await {
                                error!("Stratum connection closed for {}: {}", addr, e);
                                let _ = pending_send.send(PendingResult {
                                    id: conn.id.into(),
                                    error: Some(format!("Connection error: {}", e).into_boxed_str()),
                                    block_hash: None,
                                });
                            }
                        });
                    }
                    Err(e) => error!("Failed to accept connection: {}", e),
                }
            }
        });
        let is_synced_monitor = is_synced.clone();
        let shutdown_tx_monitor = shutdown_tx.clone();
        tokio::spawn(async move {
            let mut was_synced = true;

            loop {
                sleep(Duration::from_secs(5)).await;

                let now_synced = is_synced_monitor.load(AtomicOrdering::Relaxed);

                if !now_synced && was_synced {
                    warn!("Node lost sync — closing all miner connections and pausing operations");
                    let _ = shutdown_tx_monitor.send(());
                } else if now_synced && !was_synced {
                    info!("Node re-synced — resuming full mining operations");
                }

                was_synced = now_synced;
            }
        });

        Ok(Stratum {
            last_template,
            jobs,
            share_handler,
            send,
            current_network_diff,
        })
    }

    pub async fn broadcast(&self, template: RpcBlock) {
        if !self.jobs.is_synced.load(AtomicOrdering::Relaxed) {
            debug!("Node not synced — skipping job broadcast");
            return;
        }
        if let Some(header) = template.header.as_ref() {
            let network_diff = header.difficulty();
            self.current_network_diff.store(network_diff, AtomicOrdering::Relaxed);
            debug!("Updated current network difficulty: {}", network_diff);
        }

        if let Some(job) = self.jobs.insert(template.clone()).await {
            *self.last_template.write().await = Some(template);
            debug!("Broadcasting new job: id={}", job.id);
            let _ = self.send.send(Some(job));
        }
    }

    pub async fn resolve_pending_job(&self, error: Option<anyhow::Error>) {
        if let Some(e) = &error {
            warn!("Block submission failed: {}", e);
        }
        let error_str = error.map(|e| e.to_string().into_boxed_str());
        self.jobs.resolve_pending(error_str).await;
    }
}