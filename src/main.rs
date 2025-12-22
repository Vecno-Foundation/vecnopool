//src/main.rs

use anyhow::{Context, Result};
use log::{debug, info, warn, LevelFilter};
use std::env;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::time::{self, Duration, sleep};
use tokio::sync::watch;
use dashmap::DashSet;
use crate::treasury::payout::check_confirmations;
use crate::vecnod::{Client, Message, VecnodHandle};
use crate::stratum::Stratum;
use crate::vecnod::proto::vecnod_message::Payload;
use crate::vecnod::proto::GetInfoRequestMessage;
use crate::config::DifficultyConfig;
use crate::database::db::Db;

mod vecnod;
mod pow;
mod stratum;
mod treasury;
mod database;
mod config;

#[tokio::main]
async fn main() -> Result<()> {
    match dotenv::dotenv() {
        Ok(_) => debug!(".env file loaded successfully"),
        Err(e) => warn!("Failed to load .env file: {}", e),
    }

    let rpc_url = env::var("RPC_URL").context("RPC_URL must be set in .env")?;
    let stratum_addr = env::var("STRATUM_ADDR").unwrap_or("localhost:6969".to_string());
    let extra_data = env::var("EXTRA_DATA").unwrap_or("Vecno Mining Pool".to_string());
    let pool_address = env::var("POOL_ADDR").context("POOL_ADDR must be set in .env")?;
    let debug = env::var("DEBUG").map(|v| v.to_lowercase() == "true").unwrap_or(false);
    let pool_fee: f64 = env::var("POOL_FEE_PERCENT")
        .context("POOL_FEE_PERCENT must be set in .env")?
        .parse()
        .context("POOL_FEE_PERCENT must be a valid float")?;

    if pool_fee < 0.0 || pool_fee > 100.0 {
        return Err(anyhow::anyhow!("POOL_FEE_PERCENT must be between 0 and 100"));
    }

    info!("Loaded pool fee: {}%", pool_fee);

    env_logger::Builder::new()
        .filter_level(if debug { LevelFilter::Debug } else { LevelFilter::Info })
        .init();

    // Shared state
    let is_synced = Arc::new(AtomicBool::new(false));
    let current_daa_score = Arc::new(AtomicU64::new(0));
    let recorded_rewards = Arc::new(DashSet::<String>::new());

    info!("Waiting for vecnod node to sync... (will retry indefinitely)");

    let (sync_handle, sync_recv_cmd) = VecnodHandle::new();
    let temp_db = Arc::new(Db::new().await.context("Failed to create temporary DB")?);

    let (temp_client, mut temp_msgs) = Client::new(
        &rpc_url,
        &pool_address,
        &extra_data,
        sync_handle.clone(),
        sync_recv_cmd,
        temp_db,
        pool_address.clone(),
        is_synced.clone(),
        recorded_rewards.clone(),
    );

    let mut waited_seconds = 0;

    loop {
        sync_handle.send_cmd(Payload::GetInfoRequest(GetInfoRequestMessage {}));
        let _ = temp_client.request_template();

        if let Ok(Some(msg)) = tokio::time::timeout(Duration::from_secs(5), temp_msgs.recv()).await {
            if matches!(msg, Message::Info { .. } | Message::Template(_)) && is_synced.load(Ordering::Relaxed) {
                info!("✓ Node is fully synced — starting mining pool");
                break;
            }
        }

        waited_seconds += 10;
        let minutes = waited_seconds / 60;
        let seconds = waited_seconds % 60;
        info!("Still waiting for sync... ({:02}m:{:02}s elapsed)", minutes, seconds);
        sleep(Duration::from_secs(10)).await;
    }

    let diff_config = DifficultyConfig::load();

    let window_time_ms: u64 = env::var("WINDOW_TIME_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(300_000);

    if window_time_ms < 30000 {
        return Err(anyhow::anyhow!("WINDOW_TIME_MS must be at least 30000 milliseconds"));
    }

    let (handle, recv_cmd) = VecnodHandle::new();

    info!("Starting Stratum server on {}", stratum_addr);
    let stratum = Stratum::new(
        &stratum_addr,
        handle.clone(),
        pool_fee,
        window_time_ms,
        diff_config,
        current_daa_score.clone(),
        is_synced.clone(),
    )
    .await
    .context("Failed to initialize Stratum")?;

    let (daa_score_tx, daa_score_rx) = watch::channel::<Option<u64>>(None);

    // Cleanup task
    tokio::spawn({
        let db = stratum.share_handler.db.clone();
        async move {
            let mut interval = time::interval(Duration::from_secs(6000));
            interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                let _ = db.cleanup_old_shares(86_400).await;
                let _ = db.cleanup_processed_blocks().await;
            }
        }
    });

    tokio::spawn({
        let db = stratum.share_handler.db.clone();
        let daa_score_rx = daa_score_rx.clone();
        let window_time_ms = window_time_ms;
        let handle = handle.clone();
        async move {
            let mut interval = time::interval(Duration::from_secs(150));
            interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                let _ = check_confirmations(db.clone(), daa_score_rx.clone(), window_time_ms, handle.clone()).await;
            }
        }
    });

    // Permanent client
    let (client, mut msgs) = Client::new(
        &rpc_url,
        &pool_address,
        &extra_data,
        handle.clone(),
        recv_cmd,
        stratum.share_handler.db.clone(),
        pool_address.clone(),
        is_synced.clone(),
        recorded_rewards.clone(),
    );

    handle.send_cmd(Payload::get_block_dag_info());

    info!("Vecno Mining Pool is now fully operational and accepting miners!");

    // Main loop
    while let Some(msg) = msgs.recv().await {
        match msg {
            Message::Info { version } => {
                info!("Connected to Vecnod {}", version);
            }
            Message::NewTemplate => {
                let _ = client.request_template();
            }
            Message::Template(template) => {
                *stratum.last_template.write().await = Some(template.clone());
                stratum.broadcast(template).await;
            }
            Message::SubmitBlockResult(error) => {
                let error_any = error.map(|e| anyhow::anyhow!(e.to_string()));
                stratum.resolve_pending_job(error_any).await;
            }
            Message::BlockDagInfo { virtual_daa_score } => {
                current_daa_score.store(virtual_daa_score, Ordering::Relaxed);
                let _ = daa_score_tx.send(Some(virtual_daa_score));
            }
            Message::NewBlock => {
                let _ = client.request_template();
                handle.send_cmd(Payload::get_block_dag_info());
            }
            Message::BlockStatus => {}
        }
    }

    warn!("Main loop exited — shutting down");
    Ok(())
}