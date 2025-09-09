// src/main.rs

use anyhow::{Context, Result};
use log::{debug, info, LevelFilter};
use std::env;
use tokio::time::{self, Duration};
use crate::metrics::start_metrics_server;
use crate::treasury::payout::{check_confirmations, process_payouts};
use crate::vecnod::{Client, Message, VecnodHandle};
use crate::stratum::Stratum;
use log::warn;

mod vecnod;
mod pow;
mod stratum;
mod wasm;
mod treasury;
mod database;
mod uint;
mod api;
mod metrics;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().context("Failed to load .env file")?;

    let rpc_url = env::var("RPC_URL").context("RPC_URL must be set in .env")?;
    let stratum_addr = env::var("STRATUM_ADDR").unwrap_or("localhost:6969".to_string());
    let extra_data = env::var("EXTRA_DATA").unwrap_or("vecnod-stratum".to_string());
    let pool_address = env::var("MINING_ADDR").context("MINING_ADDR must be set in .env")?;
    let network_id = env::var("NETWORK_ID").unwrap_or("mainnet".to_string());
    let debug = env::var("DEBUG")
        .map(|v| v.to_lowercase() == "true")
        .unwrap_or(false);
    let mnemonic = env::var("MNEMONIC").context("MNEMONIC must be set in .env")?;
    let pool_fee: f64 = env::var("POOL_FEE_PERCENT")
        .context("POOL_FEE_PERCENT must be set in .env")?
        .parse()
        .context("POOL_FEE_PERCENT must be a valid float")?;

    let words = mnemonic.trim().split_whitespace().count();
    if words != 12 && words != 24 {
        return Err(anyhow::anyhow!("MNEMONIC must be a 12 or 24-word phrase"));
    }

    if pool_fee < 0.0 || pool_fee > 100.0 {
        return Err(anyhow::anyhow!("POOL_FEE_PERCENT must be between 0 and 100"));
    }

    info!("Loaded pool fee: {}%", pool_fee); // Log to verify

    env_logger::Builder::new()
        .filter_level(LevelFilter::Info)
        .filter_module("vecnod_stratum", if debug { LevelFilter::Debug } else { LevelFilter::Info })
        .init();

    let client = reqwest::Client::new();
    wasm::initialize_wasm().await.context("Failed to initialize WASM module")?;

    tokio::spawn(start_metrics_server());

    let (handle, recv_cmd) = VecnodHandle::new();
    let stratum = Stratum::new(&stratum_addr, handle.clone(), &pool_address, &network_id, pool_fee)
        .await
        .context("Failed to initialize Stratum")?;

    // Start payout task
    tokio::spawn({
        let db = stratum.share_handler.db.clone();
        let client = client.clone();
        let payout_notify = stratum.payout_notify.clone();
        let pool_fee = pool_fee; // Capture pool_fee for the task
        async move {
            let mut confirmations_interval = time::interval(Duration::from_secs(30));
            let mut payouts_interval = time::interval(Duration::from_secs(600));
            loop {
                tokio::select! {
                    _ = confirmations_interval.tick() => {
                        if let Err(e) = check_confirmations(db.clone(), &client).await {
                            log::warn!("Failed to check confirmations: {:?}", e);
                        }
                    }
                    _ = payouts_interval.tick() => {
                        if let Err(e) = process_payouts(db.clone(), &client, payout_notify.clone(), pool_fee).await {
                            log::warn!("Failed to process payouts: {:?}", e);
                        }
                    }
                }
            }
        }
    });

    let (client, mut msgs) = Client::new(&rpc_url, &pool_address, &extra_data, handle, recv_cmd);

    while let Some(msg) = msgs.recv().await {
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
        }
    }

    println!("Main loop exited, shutting down...");
    Ok(())
}