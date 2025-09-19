//src/treasury/payout.rs

use anyhow::{Context, Result};
use std::sync::Arc;
use sqlx::Row;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::watch;
use crate::database::db::{Db};
use crate::stratum::protocol::PayoutNotification;
use tokio::sync::broadcast::Sender;
use log::{debug, warn, info};

pub async fn check_confirmations(db: Arc<Db>, mut daa_score_rx: watch::Receiver<Option<u64>>, window_time_ms: u64) -> Result<()> {
    debug!("Starting check_confirmations task");

    let current_daa_score = match daa_score_rx.changed().await {
        Ok(()) => match daa_score_rx.borrow_and_update().as_ref() {
            Some(daa_score) => {
                debug!("Using latest DAA score: {}", daa_score);
                *daa_score
            }
            None => {
                warn!("No DAA score available after change, skipping confirmation check");
                return Err(anyhow::anyhow!("No DAA score available"));
            }
        },
        Err(e) => {
            warn!("DAA score channel closed: {:?}", e);
            return Err(anyhow::anyhow!("DAA score channel closed"));
        }
    };

    debug!("Fetched current virtual DAA score: {}", current_daa_score);

    let unconfirmed_blocks = match db.get_unconfirmed_blocks().await {
        Ok(blocks) => {
            debug!("Fetched {} unconfirmed blocks", blocks.len());
            blocks
        }
        Err(e) => {
            warn!("Failed to get unconfirmed blocks: {:?}", e);
            return Err(e).context("Failed to get unconfirmed blocks");
        }
    };

    if unconfirmed_blocks.is_empty() {
        debug!("No unconfirmed blocks to process");
        if let Err(e) = db.cleanup_unaccepted_blocks().await {
            warn!("Failed to clean up unaccepted blocks: {:?}", e);
        } else {
            debug!("Successfully cleaned up unaccepted blocks");
        }
        return Ok(());
    }

    debug!("Processing {} unconfirmed blocks", unconfirmed_blocks.len());

    let mut updates = Vec::new();
    for block in unconfirmed_blocks {
        debug!("Examining block: reward_block_hash={}, daa_score={}, accepted={}, confirmations={}", 
               block.reward_block_hash, block.daa_score, block.accepted, block.confirmations);
        if block.accepted == 0 {
            debug!("Skipping unaccepted block: {}", block.reward_block_hash);
            continue;
        }

        let confirmations = current_daa_score.saturating_sub(block.daa_score as u64);
        debug!("Processing block: {}, new_confirmations={}", block.reward_block_hash, confirmations);
        updates.push((block.reward_block_hash.clone(), confirmations as i64));
    }

    if !updates.is_empty() {
        let start_time = SystemTime::now();
        let mut transaction = db.pool.begin().await.context("Failed to start transaction for batch confirmations update")?;
        for (reward_block_hash, confirmations) in &updates {
            let result = sqlx::query(
                "UPDATE blocks SET confirmations = $1 WHERE reward_block_hash = $2"
            )
            .bind(*confirmations)
            .bind(&reward_block_hash)
            .execute(&mut *transaction)
            .await
            .context(format!("Failed to update confirmations for block {}", reward_block_hash));

            match result {
                Ok(_) => {
                    debug!("Updated confirmations: block_hash={}, confirmations={}", reward_block_hash, confirmations);
                }
                Err(e) => {
                    warn!("Failed to update confirmations for block_hash={}: {:?}", reward_block_hash, e);
                }
            }
        }
        transaction.commit().await.context("Failed to commit batch confirmations update")?;
        let elapsed = start_time.elapsed().unwrap_or_default().as_secs_f64();
        debug!("Batch update_block_confirmations took {} seconds for {} blocks", elapsed, updates.len());
    }

    if let Err(e) = db.cleanup_unaccepted_blocks().await {
        warn!("Failed to clean up unaccepted blocks: {:?}", e);
    } else {
        debug!("Successfully cleaned up unaccepted blocks");
    }

    if let Err(e) = process_rewards(db.clone(), window_time_ms).await {
        warn!("Failed to process rewards: {:?}", e);
    }

    debug!("Completed check_confirmations task");
    Ok(())
}

pub async fn process_rewards(db: Arc<Db>, window_time_ms: u64) -> Result<()> {
    debug!("Starting process_rewards task");

    let window_duration_secs = window_time_ms / 1000;

    let current_time_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as i64;

    let blocks = match db.get_blocks_for_rewards().await {
        Ok(blocks) => {
            debug!("Fetched {} blocks for reward processing", blocks.len());
            blocks
        }
        Err(e) => {
            warn!("Failed to fetch blocks for reward processing: {:?}", e);
            return Err(e).context("Failed to fetch blocks for reward processing");
        }
    };

    if blocks.is_empty() {
        debug!("No blocks eligible for reward processing");
        return Ok(());
    }

    let mut processed_blocks = Vec::new();

    for block in blocks {
        let block_timestamp = block.timestamp.unwrap_or(current_time_secs);
        if current_time_secs < block_timestamp + window_duration_secs as i64 {
            continue;
        }

        debug!("Block {} reached 100 confirmations and PPLNS window, processing: daa_score={}", 
               block.reward_block_hash, block.daa_score);
        processed_blocks.push(block.reward_block_hash.clone());

        let share_counts = match db.get_shares_in_time_window(block_timestamp, window_duration_secs).await {
            Ok(counts) => counts,
            Err(e) => {
                warn!("Failed to get share difficulties for block {}: {:?}", block.reward_block_hash, e);
                continue;
            }
        };
        let total_difficulty: u64 = share_counts.iter().map(|(_address, difficulty)| *difficulty).sum();
        debug!("Share difficulties for block {}: total_difficulty={}, shares={:?}", 
               block.reward_block_hash, total_difficulty, share_counts);

        if total_difficulty == 0 {
            warn!("No valid shares found for block {} in time window {}", block.reward_block_hash, block_timestamp);
            continue;
        }

        let amount = block.amount as u64;

        for (address, miner_difficulty) in share_counts.iter() {
            let base_address = address.split('.').next().unwrap_or(address);
            let share_percentage = *miner_difficulty as f64 / total_difficulty as f64;
            let miner_reward = ((amount as f64) * share_percentage) as u64;
            info!("Distributing reward for block {}: address={}, reward={} sompi, share_percentage={:.2}%", 
                  block.reward_block_hash, base_address, miner_reward, share_percentage * 100.0);

            let result = db.add_balance("", base_address, miner_reward).await
                .context(format!("Failed to add balance for address {} in block {}", base_address, block.reward_block_hash));

            match result {
                Ok(_) => {
                    info!("Added balance: address={}, reward={} VE, block={}", 
                         base_address, miner_reward as f64 / 100_000_000.0, block.reward_block_hash);
                }
                Err(e) => {
                    warn!("Failed to add balance for address={}: {:?}", base_address, e);
                }
            }
        }
    }

    if !processed_blocks.is_empty() {
        let start_time = SystemTime::now();
        let mut transaction = db.pool.begin().await.context("Failed to start transaction for batch mark processed")?;
        for reward_block_hash in &processed_blocks {
            let result = sqlx::query(
                "UPDATE blocks SET processed = $1 WHERE reward_block_hash = $2"
            )
            .bind(1_i64)
            .bind(&reward_block_hash)
            .execute(&mut *transaction)
            .await
            .context(format!("Failed to mark block {} as processed", reward_block_hash));

            match result {
                Ok(_) => {
                    debug!("Marked block as processed: {}", reward_block_hash);
                }
                Err(e) => {
                    warn!("Failed to mark block {} as processed: {:?}", reward_block_hash, e);
                }
            }
        }
        transaction.commit().await.context("Failed to commit batch mark processed")?;
        let elapsed = start_time.elapsed().unwrap_or_default().as_secs_f64();
        debug!("Batch mark_block_processed took {} seconds for {} blocks", elapsed, processed_blocks.len());
    }

    debug!("Completed process_rewards task");
    Ok(())
}

pub async fn process_payouts(db: Arc<Db>, payout_notify: Sender<PayoutNotification>) -> Result<()> {
    debug!("Starting process_payouts task");

    let payments = match sqlx::query("SELECT address, amount, tx_id FROM payments WHERE notified = $1")
        .bind(false)
        .fetch_all(&db.pool)
        .await
        .context("Failed to fetch new payments")
    {
        Ok(rows) => {
            debug!("Fetched {} payments from database", rows.len());
            rows
        }
        Err(e) => {
            warn!("Failed to fetch payments: {:?}", e);
            return Err(e);
        }
    };

    debug!("Found {} new payments in database", payments.len());

    if payments.is_empty() {
        debug!("No new payments found in database");
        return Ok(());
    }

    let payments = payments
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

    for (address, amount, tx_id) in &payments {
        debug!("Processing payout: address={}, amount={}, tx_id={}", address, amount, tx_id);
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

            let result = sqlx::query("UPDATE payments SET notified = $1 WHERE tx_id = $2")
                .bind(true)
                .bind(tx_id)
                .execute(&db.pool)
                .await
                .context(format!("Failed to mark payment as notified for tx_id={}", tx_id));

            match result {
                Ok(_) => {
                    debug!("Marked payment as notified: tx_id={}", tx_id);
                }
                Err(e) => {
                    warn!("Failed to mark payment as notified for tx_id={}: {:?}", tx_id, e);
                }
            }
        }
    }

    info!("Processed {} new payout notifications successfully", payments.len());
    Ok(())
}