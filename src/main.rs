use anyhow::{Context, Result};
use dotenv::dotenv;
use log::{debug, info, warn, LevelFilter};
use std::env;
use tokio_retry::{Retry, strategy::ExponentialBackoff};
use serde_json::Value;
use warp::Filter;

use crate::vecnod::{Client, Message, VecnodHandle};
use vecnod_stratum::wasm;
pub use crate::uint::U256;

mod vecnod;
mod pow;
mod stratum;
mod database;
mod uint;

async fn fetch_block_details(block_hash: &str) -> Result<(String, u64)> {
    let url = format!(
        "{}/blocks/{}?includeColor=false",
        env::var("RPC_URL").context("RPC_URL must be set")?,
        block_hash
    );
    let retry_strategy = ExponentialBackoff::from_millis(100).take(3);
    let response: Value = Retry::spawn(retry_strategy, || async {
        reqwest::get(&url).await?.json().await
    })
    .await
    .context("Failed to fetch block details")?;
    let reward_block_hash = response["verboseData"]["mergeSetBluesHashes"]
        .as_array()
        .and_then(|arr| arr.get(0).and_then(|v| v.as_str()))
        .map(|s| s.to_string())
        .unwrap_or_else(|| "reward_block_hash_placeholder".to_string());
    let daa_score = response["header"]["daaScore"]
        .as_u64()
        .unwrap_or(0);
    Ok((reward_block_hash, daa_score))
}

async fn start_metrics_server() {
    let metrics_route = warp::path("metrics").map(|| {
        use prometheus::{Encoder, TextEncoder};
        let encoder = TextEncoder::new();
        let metric_families = prometheus::gather();
        let mut buffer = vec![];
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    });
    warp::serve(metrics_route)
        .run(([0, 0, 0, 0], 9090))
        .await;
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().context("Failed to load .env file")?;

    let rpc_url = env::var("RPC_URL").context("RPC_URL must be set in .env")?;
    let stratum_addr = env::var("STRATUM_ADDR").unwrap_or("localhost:6969".to_string());
    let extra_data = env::var("EXTRA_DATA").unwrap_or("vecnod-stratum".to_string());
    let pool_address = env::var("MINING_ADDR").context("MINING_ADDR must be set in .env")?;
    let network_id = env::var("NETWORK_ID").unwrap_or("mainnet".to_string());
    let debug = env::var("DEBUG")
        .map(|v| v.to_lowercase() == "true")
        .unwrap_or(false);

    let level = if debug { LevelFilter::Debug } else { LevelFilter::Info };

    env_logger::Builder::new()
        .filter_level(LevelFilter::Info)
        .filter_module("vecnod_stratum", level)
        .init();

    // Initialize WASM module
    wasm::initialize_wasm().await.context("Failed to initialize WASM module")?;

    tokio::spawn(start_metrics_server());

    let (handle, recv_cmd) = VecnodHandle::new();
    let stratum = stratum::Stratum::new(&stratum_addr, handle.clone(), &pool_address, &network_id)
        .await
        .context("Failed to initialize Stratum")?;

    let (client, mut msgs) = Client::new(&rpc_url, &pool_address, &extra_data, handle, recv_cmd);
    while let Some(msg) = msgs.recv().await {
        match msg {
            Message::Info { version, .. } => {
                info!("Connected to Vecnod {version}");
            }
            Message::NewTemplate => {
                debug!("Requesting new template");
                if !client.request_template() {
                    debug!("Channel closed");
                    break;
                }
            }
            Message::Template(template) => {
                debug!("Received block template");
                *stratum.last_template.write().await = Some(template.clone());
                stratum.broadcast(template).await;
            }
            Message::SubmitBlockResult(error) => {
                debug!("Resolve pending job");
                match &error {
                    Some(e) => debug!("Submitted invalid block: {e}"),
                    None => {
                        info!("Found a block!");
                        let last_template = stratum.last_template.read().await;
                        if let Some(template) = &*last_template {
                            let block_hash = template
                                .verbose_data
                                .as_ref()
                                .map(|v| v.hash.clone())
                                .unwrap_or_else(|| "block_hash_placeholder".to_string());
                            let (reward_block_hash, daa_score) = fetch_block_details(&block_hash)
                                .await
                                .unwrap_or_else(|e| {
                                    warn!("Failed to fetch block details: {}", e);
                                    ("reward_block_hash_placeholder".to_string(), 0)
                                });
                            stratum
                                .distribute_rewards(block_hash, reward_block_hash, daa_score)
                                .await;
                        } else {
                            warn!("No template available for reward distribution");
                        }
                    }
                }
                stratum.resolve_pending_job(error).await;
            }
        }
    }

    Ok(())
}