// src/treasury/payout.rs

use anyhow::{Context, Result};
use std::sync::Arc;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::watch;
use crate::database::db::Db;
use crate::vecnod::VecnodHandle;
use log::{debug, warn, info};

pub async fn check_confirmations(
    db: Arc<Db>,
    mut daa_score_rx: watch::Receiver<Option<u64>>,
    window_time_ms: u64,
    vecnod_handle: VecnodHandle,
) -> Result<()> {
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

    debug!("Processing {} unconfirmed blocks", unconfirmed_blocks.len());

    let mut updates = Vec::new();
    for block in unconfirmed_blocks {
        let confirmations = current_daa_score.saturating_sub(block.daa_score as u64);
        updates.push((block.reward_block_hash.clone(), confirmations as i64));
    }

    if !updates.is_empty() {
        let mut transaction = db
            .pool
            .begin()
            .await
            .context("Failed to start transaction for batch confirmations update")?;
        for (reward_block_hash, confirmations) in &updates {
            sqlx::query(
                "UPDATE blocks SET confirmations = $1 WHERE reward_block_hash = $2"
            )
            .bind(*confirmations)
            .bind(&reward_block_hash)
            .execute(&mut *transaction)
            .await
            .ok();
        }
        transaction.commit().await.ok();
    }

    // Update chain status
    let blocks_for_rewards = db.get_blocks_for_rewards().await.unwrap_or_default();
    for block in blocks_for_rewards {
        db.update_block_chain_status(&block.reward_block_hash, &vecnod_handle)
            .await
            .ok();
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

    let pool_fee = std::env::var("POOL_FEE_PERCENT")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .map(|f| f / 100.0)
        .unwrap_or(0.0);

    let blocks = match db.get_blocks_for_rewards().await {
        Ok(blocks) => blocks.into_iter().filter(|b| b.is_chain_block).collect::<Vec<_>>(),
        Err(e) => {
            warn!("Failed to fetch blocks for reward processing: {:?}", e);
            return Err(e);
        }
    };

    if blocks.is_empty() {
        debug!("No matured chain blocks eligible for payout");
        return Ok(());
    }

    let mut total_full_amount = 0u64;
    let mut total_net_amount = 0u64;
    let mut total_pool_fee_amount = 0u64;
    let mut all_payouts = Vec::new();
    let mut processed_blocks = Vec::new();

    for block in blocks {
        let block_timestamp = block.timestamp.unwrap_or(current_time_secs);
        if current_time_secs < block_timestamp + window_duration_secs as i64 {
            continue;
        }

        let share_counts = match db.get_shares_in_time_window(block_timestamp, window_duration_secs).await {
            Ok(counts) => counts,
            Err(e) => {
                warn!("Failed to get shares for block {}: {:?}", block.reward_block_hash, e);
                continue;
            }
        };

        let total_difficulty: u64 = share_counts.iter().map(|(_, d)| *d).sum();
        if total_difficulty == 0 {
            debug!("No shares in window for block {}, skipping", block.reward_block_hash);
            processed_blocks.push(block.reward_block_hash.clone());
            continue;
        }

        let full_amount = block.amount as u64;
        let net_amount = ((full_amount as f64) * (1.0 - pool_fee)).round() as u64;
        let pool_fee_amount = full_amount - net_amount;

        total_full_amount += full_amount;
        total_net_amount += net_amount;
        total_pool_fee_amount += pool_fee_amount;

        let mut block_payouts = Vec::new();
        let mut block_distributed = 0u64;

        for (address, miner_difficulty) in &share_counts {
            let base_address = address.split('.').next().unwrap_or(address);
            let share_percentage = *miner_difficulty as f64 / total_difficulty as f64;
            let miner_reward = ((net_amount as f64) * share_percentage).round() as u64;
            block_distributed += miner_reward;

            block_payouts.push((base_address.to_owned(), miner_reward));

            db.add_balance("", base_address, miner_reward).await.ok();
        }

        all_payouts.push((block.reward_block_hash.clone(), block_payouts, block_distributed, net_amount));
        processed_blocks.push(block.reward_block_hash.clone());
    }

    if processed_blocks.is_empty() {
        debug!("No blocks processed for payout");
        return Ok(());
    }

    // === SINGLE GLOBAL PAYOUT SUMMARY ===
    info!(target: "payout", "=== PAYOUT SUMMARY | {} blocks processed ===", processed_blocks.len());
    info!(target: "payout", "Total reward: {:.8} VE | Pool fee: {:.8} VE ({:.2}%) | Net distributed: {:.8} VE",
        total_full_amount as f64 / 100_000_000.0,
        total_pool_fee_amount as f64 / 100_000_000.0,
        pool_fee * 100.0,
        total_net_amount as f64 / 100_000_000.0
    );

    // Group by miner for cleaner display
    let mut miner_totals: HashMap<String, u64> = HashMap::new();
    for (_, payouts, _, _) in &all_payouts {
        for (addr, reward) in payouts {
            *miner_totals.entry(addr.clone()).or_insert(0) += reward;
        }
    }

    let mut sorted_miners: Vec<_> = miner_totals.into_iter().collect();
    sorted_miners.sort_by(|a, b| b.1.cmp(&a.1));

    info!(target: "payout", "Miners paid: {}", sorted_miners.len());
    for (addr, total_reward) in sorted_miners {
        info!(target: "payout", "  • {} → {:.8} VE", addr, total_reward as f64 / 100_000_000.0);
    }

    let total_distributed: u64 = all_payouts.iter().map(|(_, _, dist, _)| *dist).sum();
    let dust = total_net_amount.saturating_sub(total_distributed);

    info!(target: "payout", "Total distributed: {:.8} VE | Remaining dust: {:.8} VE", 
        total_distributed as f64 / 100_000_000.0,
        dust as f64 / 100_000_000.0
    );
    info!(target: "payout", "==============================================");

    // Mark blocks as processed
    let mut transaction = db.pool.begin().await.context("Failed to start transaction")?;
    for hash in processed_blocks {
        sqlx::query("UPDATE blocks SET processed = 1 WHERE reward_block_hash = $1")
            .bind(&hash)
            .execute(&mut *transaction)
            .await
            .ok();
    }
    transaction.commit().await.ok();

    debug!("Completed process_rewards task");
    Ok(())
}