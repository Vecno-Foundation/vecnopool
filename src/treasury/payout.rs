//src/treasury/payout.rs

use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::Value;
use tokio_retry::{Retry, strategy::ExponentialBackoff};
use crate::database::db::Db;
use log::{debug, warn, info, error};
use std::env;
use std::sync::Arc;
use sqlx::Row;
use crate::metrics::{DB_QUERIES_SUCCESS, DB_QUERIES_FAILED, TRANSACTION_CREATION_FAILED, REWARDS_DISTRIBUTED};
use crate::stratum::protocol::PayoutNotification;
use tokio::sync::broadcast::Sender;
use std::time::{SystemTime, UNIX_EPOCH};

fn strip_worker_suffix(address: &str) -> &str {
    address.split('.').next().unwrap_or(address)
}

#[derive(Debug)]
#[allow(dead_code)]
struct Balance {
    address: String,
    available_balance: i64,
}

pub async fn check_confirmations(db: Arc<Db>, client: &Client) -> Result<()> {
    let vecnoscan_url = env::var("VECNOSCAN_URL").context("VECNOSCAN_URL must be set in .env")?;
    debug!("Checking confirmations with VECNOSCAN_URL: {}", vecnoscan_url);
    let retry_strategy = ExponentialBackoff::from_millis(200)
        .factor(2)
        .max_delay(std::time::Duration::from_secs(5))
        .take(5);

    let current_daa_score = Retry::spawn(retry_strategy.clone(), || async {
        debug!("Fetching network info from: {}/info/network", vecnoscan_url);
        let response = client
            .get(&format!("{}/info/network", vecnoscan_url))
            .send()
            .await
            .context("Failed to fetch Vecnoscan network info")?;
        
        if !response.status().is_success() {
            return Err(anyhow::anyhow!("Vecnoscan info request failed: {}", response.status()));
        }

        let info: Value = response.json().await.context("Failed to parse Vecnoscan info response")?;
        let daa_score = info["virtualDaaScore"]
            .as_str()
            .and_then(|s| s.parse::<u64>().ok())
            .context("Failed to parse virtualDaaScore")?;

        Ok(daa_score)
    })
    .await
    .context("Failed to get current DAA score after retries")?;

    debug!("Fetched current DAA score: {}", current_daa_score);

    let unconfirmed_blocks = db.get_unconfirmed_blocks().await
        .context("Failed to get unconfirmed blocks")?;
    
    if unconfirmed_blocks.is_empty() {
        debug!("No unconfirmed blocks to process");
        return Ok(());
    }

    for block in unconfirmed_blocks {
        let confirmations = current_daa_score.saturating_sub(block.daa_score as u64);

        let result = db.update_block_confirmations(&block.reward_block_hash, confirmations).await
            .context(format!("Failed to update confirmations for block {}", block.reward_block_hash));
        
        match result {
            Ok(_) => {
                DB_QUERIES_SUCCESS.with_label_values(&["update_block_confirmations"]).inc();
                debug!("Updated confirmations: block_hash={}, confirmations={}", block.reward_block_hash, confirmations);
            }
            Err(e) => {
                DB_QUERIES_FAILED.with_label_values(&["update_block_confirmations"]).inc();
                warn!("Failed to update confirmations for block_hash={}: {:?}", block.reward_block_hash, e);
                continue;
            }
        }

        if confirmations >= 100 {
            debug!("Block {} reached 100 confirmations, marked as valid, daa_score={}", block.reward_block_hash, block.daa_score);
            if let Err(e) = db.mark_block_processed(&block.reward_block_hash).await {
                warn!("Failed to mark block {} as processed: {:?}", block.reward_block_hash, e);
                DB_QUERIES_FAILED.with_label_values(&["mark_block_processed"]).inc();
                continue;
            } else {
                DB_QUERIES_SUCCESS.with_label_values(&["mark_block_processed"]).inc();
            }

            let share_counts = db.get_shares_in_window(block.daa_score as u64, 1000).await
                .context("Failed to get share counts for reward distribution")?;
            let total_shares: u64 = share_counts.iter().map(|entry| *entry.value()).sum();
            debug!("Share counts for block {}: {:?}", block.reward_block_hash, share_counts);
            
            if total_shares == 0 {
                warn!("No shares found for block {} in daa_score window {}", block.reward_block_hash, block.daa_score);
                continue;
            }

            let amount = block.amount as u64; // Fee already deducted in add_block_details

            for entry in share_counts.iter() {
                let address = entry.key();
                let miner_shares = *entry.value();
                let share_percentage = miner_shares as f64 / total_shares as f64;
                let miner_reward = ((amount as f64) * share_percentage) as u64;

                let result = db.add_balance(&block.miner_id, address, miner_reward).await
                    .context(format!("Failed to add balance for address {} in block {}", address, block.reward_block_hash));
                
                match result {
                    Ok(_) => {
                        DB_QUERIES_SUCCESS.with_label_values(&["add_balance"]).inc();
                        REWARDS_DISTRIBUTED.with_label_values(&[address, &block.reward_block_hash]).inc_by(miner_reward as f64 / 100_000_000.0);
                        info!(
                            "Added reward to balance: amount={} VE, address={}, block_hash={}, confirmations={}",
                            miner_reward as f64 / 100_000_000.0, address, block.reward_block_hash, confirmations
                        );
                    }
                    Err(e) => {
                        DB_QUERIES_FAILED.with_label_values(&["add_balance"]).inc();
                        warn!("Failed to add balance for address={}: {:?}", address, e);
                    }
                }
            }
        }
    }

    Ok(())
}

pub async fn process_payouts(db: Arc<Db>, client: &Client, payout_notify: Sender<PayoutNotification>, pool_fee: f64) -> Result<()> {
    let pool_address = env::var("MINING_ADDR").context("MINING_ADDR must be set in .env")?;
    let wasm_url = env::var("WASM_URL").unwrap_or("http://localhost:8181".to_string());
    debug!("Starting process_payouts with WASM_URL: {}, pool fee: {}%", wasm_url, pool_fee);

    let balances = sqlx::query("SELECT address, available_balance FROM balances WHERE available_balance >= 100000000")
        .fetch_all(&db.pool)
        .await
        .context("Failed to fetch balances")?
        .into_iter()
        .map(|row| {
            Ok::<_, sqlx::Error>(Balance {
                address: row.get::<String, _>("address"),
                available_balance: row.get::<i64, _>("available_balance"),
            })
        })
        .collect::<Result<Vec<Balance>, sqlx::Error>>()
        .context("Failed to parse balance rows")?;
    debug!("Available balances: {:?}", balances);

    debug!("Sending POST request to: {}/processPayouts", wasm_url);
    let response = client
        .post(&format!("{}/processPayouts", wasm_url))
        .send()
        .await;

    let payout_data: Value = match response {
        Ok(resp) => {
            debug!("Received response with status: {}", resp.status());
            if !resp.status().is_success() {
                let status = resp.status().as_u16();
                let body = resp.text().await.unwrap_or_default();
                warn!("Failed to process payouts: status={}, body={}", status, body);
                TRANSACTION_CREATION_FAILED.with_label_values(&["processPayouts"]).inc();
                return Err(anyhow::anyhow!("Failed to process payouts: status={}, body={}", status, body));
            }
            let data = resp.json().await
                .context("Failed to parse processPayouts response")?;
            debug!("Parsed payout data: {:?}", data);
            data
        }
        Err(e) => {
            error!("Failed to call processPayouts endpoint: {:?}", e);
            TRANSACTION_CREATION_FAILED.with_label_values(&["processPayouts"]).inc();
            return Err(e.into());
        }
    };

    let payouts = payout_data["result"]
        .as_array()
        .context("processPayouts response missing result array")?;
    debug!("Payouts received: {:?}", payouts);

    if payouts.is_empty() {
        warn!("No payouts processed by processPayouts endpoint. Available balances: {:?}", balances);
        return Ok(());
    }

    for payout in payouts {
        let address = payout["address"]
            .as_str()
            .context("Payout missing address")?;
        let amount = payout["amount"]
            .as_str()
            .context("Payout missing or invalid amount")?
            .parse::<u64>()
            .context("Failed to parse amount")?;
        let tx_id = payout["txId"]
            .as_str()
            .context("Payout missing txId")?;

        let base_address = strip_worker_suffix(address);
        if base_address == pool_address {
            debug!("Skipping database update for pool address: {}", address);
            continue;
        }

        info!("Payout processed by run_vecno.js: amount={} VE, address={}, tx_id={}", amount as f64 / 100_000_000.0, address, tx_id);

        let notification = PayoutNotification {
            address: address.to_string(),
            amount,
            tx_id: tx_id.to_string(),
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
        };
        if let Err(e) = payout_notify.send(notification) {
            warn!("Failed to send payout notification for address={}: {:?}", address, e);
        }

        let exists = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM payments WHERE tx_id = ?")
            .bind(tx_id)
            .fetch_one(&db.pool)
            .await
            .context("Failed to check existing payment")?;
        if exists == 0 {
            warn!("Payment not found in database for tx_id: {}", tx_id);
            let result = db.add_payment(address, amount, tx_id).await
                .context(format!("Failed to record payment for {}", address));
            match result {
                Ok(_) => {
                    DB_QUERIES_SUCCESS.with_label_values(&["add_payment"]).inc();
                    info!("Payout recorded: amount={} VE, address={}, tx_id={}", amount as f64 / 100_000_000.0, address, tx_id);
                }
                Err(e) => {
                    DB_QUERIES_FAILED.with_label_values(&["add_payment"]).inc();
                    warn!("Failed to record payment for address={}: {:?}", address, e);
                    continue;
                }
            }
        } else {
            debug!("Payment already recorded for tx_id: {}", tx_id);
        }

        let balance = sqlx::query_scalar::<_, i64>("SELECT available_balance FROM balances WHERE address = ?")
            .bind(address)
            .fetch_one(&db.pool)
            .await
            .context("Failed to check balance")?;
        if balance == 0 {
            debug!("Balance already reset for address: {}", address);
        } else {
            warn!("Balance not reset for address: {}, available_balance={} VE", address, balance as f64 / 100_000_000.0);
            let result = db.reset_available_balance(address).await
                .context(format!("Failed to reset available balance for {}", address));
            match result {
                Ok(_) => {
                    DB_QUERIES_SUCCESS.with_label_values(&["reset_available_balance"]).inc();
                    info!("Balance reset: address={}", address);
                }
                Err(e) => {
                    DB_QUERIES_FAILED.with_label_values(&["reset_available_balance"]).inc();
                    warn!("Failed to reset available balance for address={}: {:?}", address, e);
                }
            }
        }
    }

    info!("Processed {} payouts successfully", payouts.len());
    Ok(())
}