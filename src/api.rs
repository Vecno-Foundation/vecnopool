use anyhow::{Context, Result};
use log::{debug, warn};
use serde_json::Value;
use tokio_retry::{Retry, strategy::ExponentialBackoff};

pub async fn fetch_block_details(block_hash: &str) -> Result<(String, u64)> {
    debug!("Fetching block details for block hash: {}", block_hash);
    let client = reqwest::Client::new();
    let retry_strategy = ExponentialBackoff::from_millis(100).take(3);

    let url = format!("https://api.vecnoscan.org/blocks/{}?includeColor=false", block_hash);
    let response: Value = Retry::spawn(retry_strategy.clone(), || async {
        let response = client.get(&url).send().await?;
        let status = response.status();
        let json = response.json().await;
        debug!("Vecnoscan block response for hash {}: status={}, response={:?}", block_hash, status, json);
        json
    })
    .await
    .context(format!("Failed to fetch block details for hash {}", block_hash))?;

    if response.get("detail").is_some() {
        warn!("Block not found for hash {}: {:?}", block_hash, response);
        return Err(anyhow::anyhow!("Block not found for hash {}: {:?}", block_hash, response));
    }

    let daa_score = response["header"]["daaScore"]
        .as_str()
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| {
            anyhow::anyhow!("Invalid or missing daaScore for block {}: {:?}", block_hash, response)
        })?;

    Ok((block_hash.to_string(), daa_score))
}