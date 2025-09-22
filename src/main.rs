//src/main.rs

use anyhow::{Context, Result};
use log::{debug, info, warn, LevelFilter};
use std::env;
use tokio::time::{self, Duration};
use tokio::sync::watch;
use crate::treasury::payout::check_confirmations;
use crate::vecnod::{Client, Message, VecnodHandle};
use crate::stratum::Stratum;
use crate::vecnod::proto::vecnod_message::Payload;

mod vecnod;
mod pow;
mod stratum;
mod treasury;
mod database;
mod uint;

#[tokio::main]
async fn main() -> Result<()> {
    debug!("Starting main function");
    dotenv::dotenv().context("Failed to load .env file")?;

    let window_time_ms: u64 = env::var("WINDOW_TIME_MS")
        .map(|val| {
            val.parse::<u64>()
                .map_err(|e| anyhow::anyhow!("Invalid WINDOW_TIME_MS: {}", e))
        })
        .unwrap_or(Ok(300_000))
        .context("Failed to parse WINDOW_TIME_MS")?;

    // Validate window_time_ms
    if window_time_ms < 30000 {
        return Err(anyhow::anyhow!("WINDOW_TIME_MS must be at least 3000 milliseconds"));
    }

    debug!("Loaded WINDOW_TIME_MS: {}ms ({}s)", window_time_ms, window_time_ms / 1000);

    let rpc_url = env::var("RPC_URL").context("RPC_URL must be set in .env")?;
    let stratum_addr = env::var("STRATUM_ADDR").unwrap_or("localhost:6969".to_string());
    let extra_data = env::var("EXTRA_DATA").unwrap_or("Vecno Mining Pool".to_string());
    let pool_address = env::var("MINING_ADDR").context("MINING_ADDR must be set in .env")?;
    let debug = env::var("DEBUG")
        .map(|v| v.to_lowercase() == "true")
        .unwrap_or(false);
    let pool_fee: f64 = env::var("POOL_FEE_PERCENT")
        .context("POOL_FEE_PERCENT must be set in .env")?
        .parse()
        .context("POOL_FEE_PERCENT must be a valid float")?;
    if pool_fee < 0.0 || pool_fee > 100.0 {
        return Err(anyhow::anyhow!("POOL_FEE_PERCENT must be between 0 and 100"));
    }

    info!("Loaded pool fee: {}%", pool_fee);

    env_logger::Builder::new()
        .filter_level(LevelFilter::Info)
        .filter_module("vecnod_stratum", if debug { LevelFilter::Debug } else { LevelFilter::Info })
        .init();

    debug!("Creating VecnodHandle");
    let (handle, recv_cmd) = VecnodHandle::new();
    debug!("Initializing database");

    let stratum = Stratum::new(&stratum_addr, handle.clone(), &pool_address, pool_fee, window_time_ms)
        .await
        .context("Failed to initialize Stratum")?;

    // Create a watch channel for sharing the latest DAA score
    let (daa_score_tx, daa_score_rx) = watch::channel::<Option<u64>>(None);

    // Start cleanup task
    debug!("Spawning cleanup task");
    tokio::spawn({
        let db = stratum.share_handler.db.clone();
        async move {
            let mut cleanup_interval = time::interval(Duration::from_secs(6000)); // Run every 60 minutes
            cleanup_interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
            loop {
                debug!("Cleanup task loop iteration");
                cleanup_interval.tick().await;
                // Clean up shares older than 24 hours (86400 seconds)
                if let Err(e) = db.cleanup_old_shares(86_400).await {
                    warn!("Failed to clean up old shares: {:?}", e);
                } else {
                    debug!("Successfully cleaned up old shares (retention: 86400s)");
                }
                // Clean up processed blocks
                if let Err(e) = db.cleanup_processed_blocks().await {
                    warn!("Failed to clean up processed blocks: {:?}", e);
                } else {
                    debug!("Successfully cleaned up processed blocks");
                }
            }
        }
    });

    // Start confirmation task
    debug!("Spawning confirmation task");
    tokio::spawn({
        let db = stratum.share_handler.db.clone();
        let daa_score_rx = daa_score_rx.clone();
        async move {
            let mut confirmations_interval = time::interval(Duration::from_secs(60));
            confirmations_interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
            loop {
                debug!("Confirmation task loop iteration");
                confirmations_interval.tick().await;
                debug!("Triggering check_confirmations");
                if let Err(e) = check_confirmations(db.clone(), daa_score_rx.clone(), window_time_ms).await {
                    warn!("Failed to check confirmations: {:?}", e);
                } else {
                    debug!("check_confirmations completed successfully");
                }
            }
        }
    });

    debug!("Creating Vecnod client");
    let (client, mut msgs) = Client::new(&rpc_url, &pool_address, &extra_data, handle.clone(), recv_cmd);

    // Request initial DAA score
    handle.send_cmd(Payload::get_block_dag_info());

    debug!("Entering main message loop");
    while let Some(msg) = msgs.recv().await {
        debug!("Received message: {:?}", msg);
        match msg {
            Message::Info { version, .. } => {
                info!("Connected to Vecnod {version}");
            }
            Message::NewTemplate => {
                debug!("New block template available, requesting new template");
                if !client.request_template() {
                    warn!("Failed to request template: channel closed");
                    break;
                }
            }
            Message::Template(template) => {
                debug!("Received new template, broadcasting");
                *stratum.last_template.write().await = Some(template.clone());
                stratum.broadcast(template).await;
            }
            Message::SubmitBlockResult(error) => {
                debug!("SubmitBlockResult: error={:?}", error);
                if let Some(ref e) = error {
                    debug!("Submitted invalid block: {}", e);
                }
                stratum.resolve_pending_job(error.map(|e| anyhow::anyhow!(e.to_string()))).await;
            }
            Message::BlockDagInfo { virtual_daa_score } => {
                debug!("Received BlockDagInfo: virtual_daa_score={}", virtual_daa_score);
                // Update the shared DAA score
                let _ = daa_score_tx.send(Some(virtual_daa_score));
            }
            Message::NewBlock => {
                debug!("New block detected in blockchain, requesting new template and DAA score");
                if !client.request_template() {
                    warn!("Failed to request template after new block: channel closed");
                    break;
                }
                // Request updated DAA score
                handle.send_cmd(Payload::get_block_dag_info());
            }
        }
    }

    warn!("Main loop exited, shutting down...");
    Ok(())
}