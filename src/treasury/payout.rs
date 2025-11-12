//src/treasury/payout.rs

use anyhow::{Context, Result};
use std::sync::Arc;
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
        debug!(
            "Examining block: reward_block_hash={}, daa_score={}, confirmations={}",
            block.reward_block_hash, block.daa_score, block.confirmations
        );

        let confirmations = current_daa_score.saturating_sub(block.daa_score as u64);
        debug!(
            "Processing block: {}, new_confirmations={}",
            block.reward_block_hash, confirmations
        );
        updates.push((block.reward_block_hash.clone(), confirmations as i64));
    }

    if !updates.is_empty() {
        let start_time = SystemTime::now();
        let mut transaction = db
            .pool
            .begin()
            .await
            .context("Failed to start transaction for batch confirmations update")?;
        for (reward_block_hash, confirmations) in &updates {
            let result = sqlx::query(
                "UPDATE blocks SET confirmations = $1 WHERE reward_block_hash = $2"
            )
            .bind(*confirmations)
            .bind(&reward_block_hash)
            .execute(&mut *transaction)
            .await
            .context(format!(
                "Failed to update confirmations for block {}",
                reward_block_hash
            ));

            match result {
                Ok(_) => {
                    debug!(
                        "Updated confirmations: block_hash={}, confirmations={}",
                        reward_block_hash, confirmations
                    );
                }
                Err(e) => {
                    warn!(
                        "Failed to update confirmations for block_hash={}: {:?}",
                        reward_block_hash, e
                    );
                }
            }
        }
        transaction
            .commit()
            .await
            .context("Failed to commit batch confirmations update")?;
        let elapsed = start_time.elapsed().unwrap_or_default().as_secs_f64();
        debug!(
            "Batch update_block_confirmations took {} seconds for {} blocks",
            elapsed,
            updates.len()
        );
    }

    // Update is_chain_block status for blocks with 101 or more confirmations
    let blocks_for_rewards = match db.get_blocks_for_rewards().await {
        Ok(blocks) => {
            debug!("Fetched {} blocks for reward processing", blocks.len());
            blocks
        }
        Err(e) => {
            warn!("Failed to fetch blocks for reward processing: {:?}", e);
            return Err(e).context("Failed to fetch blocks for reward processing");
        }
    };

    for block in blocks_for_rewards {
        if let Err(e) = db
            .update_block_chain_status(&block.reward_block_hash, &vecnod_handle)
            .await
        {
            warn!(
                "Failed to update chain status for block {}: {:?}",
                block.reward_block_hash, e
            );
        } else {
            debug!("Updated chain status for block {}", block.reward_block_hash);
        }
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
        Ok(blocks) => {
            // Filter blocks where is_chain_block is true
            let chain_blocks: Vec<_> = blocks.clone().into_iter().filter(|b| b.is_chain_block).collect();
            debug!(
                "Fetched {} blocks for reward processing, {} are chain blocks",
                blocks.len(),
                chain_blocks.len()
            );
            chain_blocks
        }
        Err(e) => {
            warn!("Failed to fetch blocks for reward processing: {:?}", e);
            return Err(e).context("Failed to fetch blocks for reward processing");
        }
    };

    if blocks.is_empty() {
        debug!("No chain blocks eligible for reward processing");
        return Ok(());
    }

    let mut processed_blocks = Vec::new();

    for block in blocks {
        let block_timestamp = block.timestamp.unwrap_or(current_time_secs);
        if current_time_secs < block_timestamp + window_duration_secs as i64 {
            debug!(
                "Block {} is within PPLNS window, skipping",
                block.reward_block_hash
            );
            continue;
        }

        debug!(
            "Block {} reached 101 confirmations, is_chain_block=true, and PPLNS window, processing: daa_score={}",
            block.reward_block_hash, block.daa_score
        );
        processed_blocks.push(block.reward_block_hash.clone());

        let share_counts = match db
            .get_shares_in_time_window(block_timestamp, window_duration_secs)
            .await
        {
            Ok(counts) => counts,
            Err(e) => {
                warn!(
                    "Failed to get share difficulties for block {}: {:?}",
                    block.reward_block_hash, e
                );
                continue;
            }
        };
        let total_difficulty: u64 = share_counts.iter().map(|(_address, difficulty)| *difficulty).sum();
        debug!(
            "Share difficulties for block {}: total_difficulty={}, shares={:?}",
            block.reward_block_hash, total_difficulty, share_counts
        );

        let full_amount = block.amount as u64;
        let net_amount = ((full_amount as f64) * (1.0 - pool_fee)) as u64;

        for (address, miner_difficulty) in share_counts.iter() {
            let base_address = address.split('.').next().unwrap_or(address);
            let share_percentage = *miner_difficulty as f64 / total_difficulty as f64;
            let miner_reward = ((net_amount as f64) * share_percentage) as u64;
            debug!(
                "Distributing reward for block {}: address={}, reward={} veni, share_percentage={:.2}%",
                block.reward_block_hash,
                base_address,
                miner_reward,
                share_percentage * 100.0
            );

            let result = db
                .add_balance("", base_address, miner_reward)
                .await
                .context(format!(
                    "Failed to add balance for address {} in block {}",
                    base_address, block.reward_block_hash
                ));

            match result {
                Ok(_) => {
                    info!(
                        "Added balance: address={}, reward={} VE, block={}",
                        base_address,
                        miner_reward as f64 / 100_000_000.0,
                        block.reward_block_hash
                    );
                }
                Err(e) => {
                    warn!("Failed to add balance for address={}: {:?}", base_address, e);
                }
            }
        }
    }

    if !processed_blocks.is_empty() {
        let start_time = SystemTime::now();
        let mut transaction = db
            .pool
            .begin()
            .await
            .context("Failed to start transaction for batch mark processed")?;
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
        transaction
            .commit()
            .await
            .context("Failed to commit batch mark processed")?;
        let elapsed = start_time.elapsed().unwrap_or_default().as_secs_f64();
        debug!(
            "Batch mark_block_processed took {} seconds for {} blocks",
            elapsed,
            processed_blocks.len()
        );
    }

    debug!("Completed process_rewards task");
    Ok(())
}