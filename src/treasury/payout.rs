use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::Value;
use tokio_retry::{Retry, strategy::ExponentialBackoff};
use crate::database::db::Db;
use log::{debug, warn, info};
use std::env;
use std::sync::Arc;
use sqlx::Row;
use crate::metrics::{DB_QUERIES_SUCCESS, DB_QUERIES_FAILED, REWARDS_DISTRIBUTED};
use crate::stratum::protocol::PayoutNotification;
use tokio::sync::broadcast::Sender;
use std::time::{SystemTime, UNIX_EPOCH};

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

    // PPLNS window: 5 minutes (300 seconds)
    let window_duration_secs = 300;
    let current_time_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as i64;

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

            // Use PPLNS with a 5-minute window around block timestamp
            let block_timestamp = block.timestamp.unwrap_or(current_time_secs);
            let share_counts = db.get_shares_in_time_window(block_timestamp, window_duration_secs).await
                .context("Failed to get share difficulties for reward distribution")?;
            let total_difficulty: u64 = share_counts.iter().map(|entry| *entry.value()).sum();
            debug!("Share difficulties for block {}: {:?}", block.reward_block_hash, share_counts);
            
            if total_difficulty == 0 {
                warn!("No valid shares found for block {} in time window {}", block.reward_block_hash, block_timestamp);
                continue;
            }

            let amount = block.amount as u64;

            for entry in share_counts.iter() {
                let address = entry.key();
                let miner_difficulty = *entry.value();
                let share_percentage = miner_difficulty as f64 / total_difficulty as f64;
                let miner_reward = ((amount as f64) * share_percentage) as u64;

                let result = db.add_balance(&block.miner_id, address, miner_reward).await
                    .context(format!("Failed to add balance for address {} in block {}", address, block.reward_block_hash));
                
                match result {
                    Ok(_) => {
                        DB_QUERIES_SUCCESS.with_label_values(&["add_balance"]).inc();
                        REWARDS_DISTRIBUTED.with_label_values(&[address, &block.reward_block_hash]).inc_by(miner_reward as f64 / 100_000_000.0);
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

pub async fn process_payouts(db: Arc<Db>, payout_notify: Sender<PayoutNotification>) -> Result<()> {
    debug!("Starting process_payouts to check for new payments");

    // Fetch payments where notified = false
    let payments = sqlx::query("SELECT address, amount, tx_id FROM payments WHERE notified = false")
        .fetch_all(&db.pool)
        .await
        .context("Failed to fetch new payments")?
        .into_iter()
        .map(|row| {
            Ok::<_, sqlx::Error>((
                row.get::<String, _>("address"),
                row.get::<i64, _>("amount"),
                row.get::<String, _>("tx_id"),
            ))
        })
        .collect::<Result<Vec<(String, i64, String)>, sqlx::Error>>()
        .context("Failed to parse payment rows")?;
    debug!("Found {} new payments in database", payments.len());

    if payments.is_empty() {
        debug!("No new payments found in database");
        return Ok(());
    }

    for (address, amount, tx_id) in &payments {
        // Send notification for new payment
        let notification = PayoutNotification {
            address: address.to_string(),
            amount: *amount as u64,
            tx_id: tx_id.to_string(),
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
        };

        if let Err(e) = payout_notify.send(notification) {
            warn!("Failed to send payout notification for address={}: {:?}", address, e);
        } else {
            info!(
                "Sent payout notification: amount={} VE, address={}, tx_id={}",
                *amount as f64 / 100_000_000.0,
                address,
                tx_id
            );

            // Update the payment to mark it as notified
            let result = sqlx::query("UPDATE payments SET notified = true WHERE tx_id = ?")
                .bind(tx_id)
                .execute(&db.pool)
                .await
                .context(format!("Failed to mark payment as notified for tx_id={}", tx_id));

            match result {
                Ok(_) => {
                    DB_QUERIES_SUCCESS.with_label_values(&["update_payment_notified"]).inc();
                    debug!("Marked payment as notified: tx_id={}", tx_id);
                }
                Err(e) => {
                    DB_QUERIES_FAILED.with_label_values(&["update_payment_notified"]).inc();
                    warn!("Failed to mark payment as notified for tx_id={}: {:?}", tx_id, e);
                }
            }
        }
    }

    info!("Processed {} new payout notifications successfully", payments.len());
    Ok(())
}