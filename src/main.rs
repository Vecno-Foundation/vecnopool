use anyhow::{Context, Result};
use log::{debug, info, warn, LevelFilter};
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
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

async fn fetch_block_details(block_hashes: Vec<String>, miner_info: &str) -> Result<(String, u64)> {
    let client = reqwest::Client::new();
    let retry_strategy = ExponentialBackoff::from_millis(100).take(3);
    
    for hash in block_hashes {
        let url = format!("https://api.vecnoscan.org/blocks/{}?includeColor=false", hash);
        let response: Value = Retry::spawn(retry_strategy.clone(), || async {
            let response = client.get(&url).send().await?.json().await;
            debug!("Vecnoscan response for block {}: {:?}", hash, response);
            response
        })
        .await
        .context(format!("Failed to fetch block details for hash {}", hash))?;

        if response.get("extra").and_then(|e| e.get("minerInfo")).and_then(|m| m.as_str()).map_or(false, |m| m.contains(miner_info)) {
            let reward_block_hash = response["verboseData"]["mergeSetBluesHashes"]
                .as_array()
                .and_then(|arr| arr.get(0).and_then(|v| v.as_str()))
                .map(|s| s.to_string())
                .unwrap_or_else(|| {
                    warn!("No mergeSetBluesHashes found for block {}, using placeholder", hash);
                    "reward_block_hash_placeholder".to_string()
                });
            let daa_score = response["header"]["daaScore"]
                .as_str()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or_else(|| {
                    warn!("Invalid or missing daaScore for block {}, defaulting to current timestamp", hash);
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0)
                });
            return Ok((reward_block_hash, daa_score));
        }
    }

    warn!("No matching block found for minerInfo {}, using placeholder", miner_info);
    Ok(("reward_block_hash_placeholder".to_string(), SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)))
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
    // Initialize dotenv
    dotenv::dotenv().context("Failed to load .env file")?;

    // Load environment variables
    let _wrpc_url = env::var("WRPC_URL").context("WRPC_URL must be set in .env")?;
    let rpc_url = env::var("RPC_URL").context("RPC_URL must be set in .env")?;
    let stratum_addr = env::var("STRATUM_ADDR").unwrap_or("localhost:6969".to_string());
    let extra_data = env::var("EXTRA_DATA").unwrap_or("vecnod-stratum".to_string());
    let pool_address = env::var("MINING_ADDR").context("MINING_ADDR must be set in .env")?;
    let network_id = env::var("NETWORK_ID").unwrap_or("mainnet".to_string());
    let debug = env::var("DEBUG")
        .map(|v| v.to_lowercase() == "true")
        .unwrap_or(false);
    let mnemonic = env::var("MNEMONIC").context("MNEMONIC must be set in .env")?;
    let miner_info = env::var("MINER_INFO").unwrap_or("vecnod-stratum".to_string());

    // Validate MNEMONIC
    let words = mnemonic.trim().split_whitespace().count();
    if words != 12 && words != 24 {
        return Err(anyhow::anyhow!("MNEMONIC must be a 12 or 24-word phrase"));
    }

    // Set up logging
    env_logger::Builder::new()
        .filter_level(LevelFilter::Info)
        .filter_module("vecnod_stratum", if debug { LevelFilter::Debug } else { LevelFilter::Info })
        .init();

    // Initialize WASM module
    wasm::initialize_wasm().await.context("Failed to initialize WASM module")?;

    // Start metrics server
    tokio::spawn(start_metrics_server());

    // Initialize stratum server
    let (handle, recv_cmd) = VecnodHandle::new();
    let stratum = stratum::Stratum::new(&stratum_addr, handle.clone(), &pool_address, &network_id)
        .await
        .context("Failed to initialize Stratum")?;

    // Initialize vecnod client
    let (client, mut msgs) = Client::new(&rpc_url, &pool_address, &extra_data, handle, recv_cmd);

    // Cooldown for block finds
    let last_processed_timestamp = Arc::new(AtomicU64::new(0));
    let duplicate_event_count = Arc::new(AtomicU64::new(0));

    // Main loop for handling messages
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
                debug!("Received block template: {:?}", template);
                *stratum.last_template.write().await = Some(template.clone());
                stratum.broadcast(template).await;
            }
            Message::SubmitBlockResult(error) => {
                debug!("SubmitBlockResult: error={:?}, template={:?}", error, stratum.last_template.read().await);
                match &error {
                    Some(e) => debug!("Submitted invalid block: {e}"),
                    None => {
                        let current_timestamp = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_secs())
                            .unwrap_or(0);
                        let last_timestamp = last_processed_timestamp.load(Ordering::Relaxed);
                        if current_timestamp - last_timestamp < 1 {
                            let count = duplicate_event_count.fetch_add(1, Ordering::Relaxed) + 1;
                            debug!("Skipping duplicate block find event. Last: {}, Current: {}, Count: {}", last_timestamp, current_timestamp, count);
                            continue;
                        }
                        last_processed_timestamp.store(current_timestamp, Ordering::Relaxed);
                        duplicate_event_count.store(0, Ordering::Relaxed);
                        info!("Found a block!");
                        let last_template = stratum.last_template.read().await;
                        if let Some(template) = &*last_template {
                            let block_hashes = template
                                .verbose_data
                                .as_ref()
                                .map(|v| v.merge_set_blues_hashes.clone())
                                .unwrap_or(vec!["block_hash_placeholder".to_string()]);
                            let (reward_block_hash, daa_score) = fetch_block_details(block_hashes.clone(), &miner_info)
                                .await
                                .unwrap_or_else(|e| {
                                    warn!("Failed to fetch block details: {}", e);
                                    ("reward_block_hash_placeholder".to_string(), 0)
                                });
                            stratum
                                .distribute_rewards(block_hashes[0].clone(), reward_block_hash, daa_score)
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

    println!("Main loop exited, shutting down...");
    Ok(())
}