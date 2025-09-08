use anyhow::{Context, Result};
use log::debug;
use serde::Deserialize;
use std::sync::Arc;
use crate::database::db::Db;
use crate::metrics::{BLOCK_DETAILS_FETCH_SUCCESS, BLOCK_DETAILS_FETCH_FAILED};
use log::warn;

#[derive(Deserialize, Debug)]
struct BlockResponse {
    header: Header,
    transactions: Vec<Transaction>,
    #[serde(default)]
    detail: Option<String>,
}

#[derive(Deserialize, Debug)]
struct Header {
    #[serde(rename = "daaScore")]
    daa_score: String,
}

#[derive(Deserialize, Debug)]
struct Transaction {
    outputs: Vec<Output>,
}

#[derive(Deserialize, Debug)]
struct Output {
    #[serde(rename = "verboseData")]
    verbose_data: VerboseData,
}

#[derive(Deserialize, Debug)]
struct VerboseData {
    #[serde(rename = "scriptPublicKeyAddress")]
    script_public_key_address: String,
}

pub async fn fetch_block_details(
    db: Arc<Db>,
    client: &reqwest::Client,
    block_hash: &str,
    mining_addr: &str,
) -> Result<(String, u64)> {
    debug!("Fetching block details for hash {block_hash}", block_hash = block_hash);

    // Check database first
    let result = sqlx::query_as::<_, (String, i64)>(
        "SELECT reward_block_hash, daa_score FROM blocks WHERE reward_block_hash = ?"
    )
    .bind(block_hash)
    .fetch_optional(&db.pool)
    .await
    .context("Failed to check block details in database")?;

    if let Some((hash, daa_score)) = result {
        BLOCK_DETAILS_FETCH_SUCCESS.with_label_values(&[block_hash]).inc();
        return Ok((hash, daa_score as u64));
    }

    let url = format!("https://api.vecnoscan.org/blocks/{}?includeColor=false", block_hash);

    let response = client.get(&url).send().await
        .context(format!("Failed to send request for block hash {}", block_hash))?;
    let status = response.status();
    let response: BlockResponse = response.json().await
        .context(format!("Failed to parse response for block hash {}", block_hash))?;
    debug!("Vecnoscan block response for hash {block_hash}: status {status}", block_hash = block_hash, status = status.as_u16());

    if response.detail.is_some() {
        warn!("Block not found for hash {block_hash}", block_hash = block_hash);
        BLOCK_DETAILS_FETCH_FAILED.with_label_values(&[block_hash]).inc();
        return Err(anyhow::anyhow!("Block not found for hash {}", block_hash));
    }

    if response.transactions.is_empty() {
        BLOCK_DETAILS_FETCH_FAILED.with_label_values(&[block_hash]).inc();
        return Err(anyhow::anyhow!("No transactions found in block {}", block_hash));
    }

    let coinbase_output = response.transactions[0].outputs.get(0)
        .ok_or_else(|| {
            BLOCK_DETAILS_FETCH_FAILED.with_label_values(&[block_hash]).inc();
            anyhow::anyhow!("No outputs found in coinbase transaction for block {}", block_hash)
        })?;
    let script_public_key_address = &coinbase_output.verbose_data.script_public_key_address;

    if script_public_key_address != mining_addr {
        BLOCK_DETAILS_FETCH_FAILED.with_label_values(&[block_hash]).inc();
        return Err(anyhow::anyhow!(
            "Block {} mined to address {}, not pool's MINING_ADDR {}",
            block_hash,
            script_public_key_address,
            mining_addr
        ));
    }

    let daa_score_str = &response.header.daa_score;
    if daa_score_str.is_empty() {
        BLOCK_DETAILS_FETCH_FAILED.with_label_values(&[block_hash]).inc();
        return Err(anyhow::anyhow!("Empty daaScore for block {}", block_hash));
    }
    let daa_score = daa_score_str.parse::<u64>()
        .map_err(|_| {
            BLOCK_DETAILS_FETCH_FAILED.with_label_values(&[block_hash]).inc();
            anyhow::anyhow!("Invalid daaScore for block {}: {}", block_hash, daa_score_str)
        })?;

    // Store in database
    db.add_block_details(block_hash, "pool", block_hash, "", daa_score, mining_addr, 0)
        .await
        .context("Failed to store block details in database")?;

    BLOCK_DETAILS_FETCH_SUCCESS.with_label_values(&[block_hash]).inc();
    Ok((block_hash.to_string(), daa_score))
}