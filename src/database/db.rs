use anyhow::{Context, Result};
use dashmap::DashMap;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool};
use std::path::Path;
use sqlx::Row;
use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::treasury::sharehandler::Contribution;
use log::debug;
use crate::metrics::{DB_QUERIES_SUCCESS, DB_QUERIES_FAILED};
use crate::treasury::reward_table::get_block_reward;

#[derive(Debug)]
pub struct Db {
    pub pool: SqlitePool,
}

#[derive(Debug, sqlx::FromRow)]
pub struct BlockDetails {
    pub miner_id: String,
    pub reward_block_hash: String,
    pub daa_score: i64,
    pub amount: i64,
    #[allow(dead_code)]
    pub confirmations: i64,
}

impl Db {
    pub async fn new(path: &Path) -> Result<Self> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true);
        let pool = SqlitePool::connect_with(options).await?;

        debug!("Initializing database tables");
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS shares (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                address TEXT NOT NULL,
                difficulty INTEGER NOT NULL DEFAULT 1,
                timestamp INTEGER NOT NULL,
                job_id TEXT NOT NULL,
                daa_score INTEGER NOT NULL,
                extranonce TEXT NOT NULL,
                nonce TEXT NOT NULL,
                UNIQUE(job_id, address, extranonce, nonce)
            )"
        )
        .execute(&pool)
        .await
        .context("Failed to create shares table")?;
        DB_QUERIES_SUCCESS.with_label_values(&["create_shares_table"]).inc();

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS balances (
                id TEXT NOT NULL,
                address TEXT NOT NULL,
                available_balance INTEGER NOT NULL DEFAULT 0,
                total_earned_balance INTEGER NOT NULL DEFAULT 0,
                UNIQUE(id, address)
            )"
        )
        .execute(&pool)
        .await
        .context("Failed to create balances table")?;
        DB_QUERIES_SUCCESS.with_label_values(&["create_balances_table"]).inc();

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                address TEXT NOT NULL,
                amount INTEGER NOT NULL,
                tx_id TEXT NOT NULL,
                timestamp INTEGER NOT NULL
            )"
        )
        .execute(&pool)
        .await
        .context("Failed to create payments table")?;
        DB_QUERIES_SUCCESS.with_label_values(&["create_payments_table"]).inc();

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS blocks (
                reward_block_hash TEXT PRIMARY KEY,
                miner_id TEXT NOT NULL,
                daa_score INTEGER NOT NULL,
                pool_wallet TEXT NOT NULL,
                amount INTEGER NOT NULL,
                confirmations INTEGER NOT NULL DEFAULT 0,
                processed INTEGER NOT NULL DEFAULT 0
            )"
        )
        .execute(&pool)
        .await
        .context("Failed to create blocks table")?;
        DB_QUERIES_SUCCESS.with_label_values(&["create_blocks_table"]).inc();

        debug!("Database initialized successfully");
        Ok(Db { pool })
    }

    pub async fn record_share(
        &self,
        address: &str,
        difficulty: u64,
        timestamp: u64,
        job_id: &str,
        daa_score: u64,
        extranonce: &str,
        nonce: &str,
    ) -> Result<()> {
        let result = sqlx::query(
            "INSERT INTO shares (address, difficulty, timestamp, job_id, daa_score, extranonce, nonce)
             VALUES (?, ?, ?, ?, ?, ?, ?)"
        )
        .bind(address)
        .bind(difficulty as i64)
        .bind(timestamp as i64)
        .bind(job_id)
        .bind(daa_score as i64)
        .bind(extranonce)
        .bind(nonce)
        .execute(&self.pool)
        .await;

        match result {
            Ok(_) => {
                DB_QUERIES_SUCCESS.with_label_values(&["record_share"]).inc();
                Ok(())
            }
            Err(sqlx::Error::Database(e)) if e.code() == Some("2067".into()) => {
                debug!(
                    "Skipping duplicate share for job_id={job_id}, address={address}, extranonce={extranonce}, nonce={nonce}"
                );
                DB_QUERIES_SUCCESS.with_label_values(&["record_share"]).inc();
                Ok(())
            }
            Err(e) => {
                DB_QUERIES_FAILED.with_label_values(&["record_share"]).inc();
                Err(e.into())
            }
        }
    }

    pub async fn load_recent_shares(&self, share_window: &mut VecDeque<Contribution>, n: usize) -> Result<u64> {
        let result = sqlx::query_as::<_, Contribution>(
            "SELECT address, difficulty, timestamp, job_id, daa_score, extranonce, nonce
             FROM shares
             ORDER BY timestamp DESC
             LIMIT ?"
        )
        .bind(n as i64)
        .fetch_all(&self.pool)
        .await
        .context("Failed to load recent shares");
        match result {
            Ok(rows) => {
                let mut total_shares = 0;
                for contribution in rows {
                    total_shares += 1;
                    share_window.push_front(contribution);
                }
                DB_QUERIES_SUCCESS.with_label_values(&["load_recent_shares"]).inc();
                Ok(total_shares)
            }
            Err(e) => {
                DB_QUERIES_FAILED.with_label_values(&["load_recent_shares"]).inc();
                Err(e)
            }
        }
    }

    pub async fn get_share_counts(&self, address: Option<&str>) -> Result<DashMap<String, u64>> {
        let query = if let Some(addr) = address {
            sqlx::query("SELECT address, COUNT(*) as count FROM shares WHERE address = ? GROUP BY address")
                .bind(addr)
        } else {
            sqlx::query("SELECT address, COUNT(*) as count FROM shares GROUP BY address")
        };

        let result = query
            .fetch_all(&self.pool)
            .await
            .context("Failed to get share counts");
        match result {
            Ok(rows) => {
                let sums = DashMap::new();
                for row in rows {
                    let address: String = row.get(0);
                    let count: i64 = row.get(1);
                    sums.insert(address, count as u64);
                }
                DB_QUERIES_SUCCESS.with_label_values(&["get_share_counts"]).inc();
                Ok(sums)
            }
            Err(e) => {
                DB_QUERIES_FAILED.with_label_values(&["get_share_counts"]).inc();
                Err(e)
            }
        }
    }

    pub async fn get_shares_in_window(&self, daa_score: u64, window_size: u64) -> Result<DashMap<String, u64>> {
        let result = sqlx::query(
            "SELECT address, COUNT(*) as count 
             FROM shares 
             WHERE daa_score >= ? AND daa_score < ? 
             GROUP BY address"
        )
        .bind((daa_score.saturating_sub(window_size)) as i64)
        .bind(daa_score as i64)
        .fetch_all(&self.pool)
        .await
        .context("Failed to get shares in window");
        match result {
            Ok(rows) => {
                let sums = DashMap::new();
                for row in rows {
                    let address: String = row.get(0);
                    let count: i64 = row.get(1);
                    sums.insert(address, count as u64);
                }
                DB_QUERIES_SUCCESS.with_label_values(&["get_shares_in_window"]).inc();
                Ok(sums)
            }
            Err(e) => {
                DB_QUERIES_FAILED.with_label_values(&["get_shares_in_window"]).inc();
                Err(e)
            }
        }
    }

    pub async fn add_balance(&self, id: &str, address: &str, amount: u64) -> Result<()> {
        let result = sqlx::query(
            "INSERT OR REPLACE INTO balances (id, address, available_balance, total_earned_balance)
             VALUES (?, ?, COALESCE((SELECT available_balance FROM balances WHERE id = ? AND address = ?), 0) + ?,
                     COALESCE((SELECT total_earned_balance FROM balances WHERE id = ? AND address = ?), 0) + ?)"
        )
        .bind(id)
        .bind(address)
        .bind(id)
        .bind(address)
        .bind(amount as i64)
        .bind(id)
        .bind(address)
        .bind(amount as i64)
        .execute(&self.pool)
        .await
        .context("Failed to add balance");
        match result {
            Ok(_) => {
                DB_QUERIES_SUCCESS.with_label_values(&["add_balance"]).inc();
                Ok(())
            }
            Err(e) => {
                DB_QUERIES_FAILED.with_label_values(&["add_balance"]).inc();
                Err(e)
            }
        }
    }

    pub async fn reset_available_balance(&self, address: &str) -> Result<()> {
        let result = sqlx::query("UPDATE balances SET available_balance = 0 WHERE address = ?")
            .bind(address)
            .execute(&self.pool)
            .await
            .context("Failed to reset available balance");
        match result {
            Ok(_) => {
                DB_QUERIES_SUCCESS.with_label_values(&["reset_available_balance"]).inc();
                Ok(())
            }
            Err(e) => {
                DB_QUERIES_FAILED.with_label_values(&["reset_available_balance"]).inc();
                Err(e)
            }
        }
    }

    pub async fn add_payment(&self, address: &str, amount: u64, tx_id: &str) -> Result<()> {
        let result = sqlx::query(
            "INSERT INTO payments (address, amount, tx_id, timestamp) VALUES (?, ?, ?, ?)"
        )
        .bind(address)
        .bind(amount as i64)
        .bind(tx_id)
        .bind(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64)
        .execute(&self.pool)
        .await
        .context("Failed to add payment");
        match result {
            Ok(_) => {
                DB_QUERIES_SUCCESS.with_label_values(&["add_payment"]).inc();
                Ok(())
            }
            Err(e) => {
                DB_QUERIES_FAILED.with_label_values(&["add_payment"]).inc();
                Err(e)
            }
        }
    }

    pub async fn add_block_details(
        &self,
        _block_hash: &str,
        miner_id: &str,
        reward_block_hash: &str,
        _reward_txn_id: &str,
        daa_score: u64,
        pool_wallet: &str,
        _amount: u64,
    ) -> Result<()> {
        let mut amount = get_block_reward(daa_score)
            .context(format!("Failed to calculate block reward for daa_score {}", daa_score))?;
        let pool_fee: f64 = 2.0 / 100.0; // 2% pool fee
        amount = ((amount as f64) * (1.0 - pool_fee)) as u64;

        let result = sqlx::query(
            "INSERT OR REPLACE INTO blocks (reward_block_hash, miner_id, daa_score, pool_wallet, amount, confirmations, processed)
             VALUES (?, ?, ?, ?, ?, 0, 0)"
        )
        .bind(reward_block_hash)
        .bind(miner_id)
        .bind(daa_score as i64)
        .bind(pool_wallet)
        .bind(amount as i64)
        .execute(&self.pool)
        .await
        .context("Failed to add block details");
        match result {
            Ok(_) => {
                DB_QUERIES_SUCCESS.with_label_values(&["add_block_details"]).inc();
                Ok(())
            }
            Err(e) => {
                DB_QUERIES_FAILED.with_label_values(&["add_block_details"]).inc();
                Err(e)
            }
        }
    }

    pub async fn get_unconfirmed_blocks(&self) -> Result<Vec<BlockDetails>> {
        let result = sqlx::query_as::<_, BlockDetails>(
            "SELECT miner_id, reward_block_hash, daa_score, pool_wallet, amount, confirmations
             FROM blocks WHERE confirmations < 100 AND processed = 0"
        )
        .fetch_all(&self.pool)
        .await
        .context("Failed to get unconfirmed blocks");
        match result {
            Ok(rows) => {
                DB_QUERIES_SUCCESS.with_label_values(&["get_unconfirmed_blocks"]).inc();
                Ok(rows)
            }
            Err(e) => {
                DB_QUERIES_FAILED.with_label_values(&["get_unconfirmed_blocks"]).inc();
                Err(e)
            }
        }
    }

    pub async fn update_block_confirmations(&self, reward_block_hash: &str, confirmations: u64) -> Result<()> {
        let result = sqlx::query(
            "UPDATE blocks SET confirmations = ? WHERE reward_block_hash = ?"
        )
        .bind(confirmations as i64)
        .bind(reward_block_hash)
        .execute(&self.pool)
        .await
        .context("Failed to update block confirmations");
        match result {
            Ok(_) => {
                DB_QUERIES_SUCCESS.with_label_values(&["update_block_confirmations"]).inc();
                Ok(())
            }
            Err(e) => {
                DB_QUERIES_FAILED.with_label_values(&["update_block_confirmations"]).inc();
                Err(e)
            }
        }
    }

    pub async fn mark_block_processed(&self, reward_block_hash: &str) -> Result<()> {
        let result = sqlx::query(
            "UPDATE blocks SET processed = 1 WHERE reward_block_hash = ?"
        )
        .bind(reward_block_hash)
        .execute(&self.pool)
        .await
        .context("Failed to mark block as processed");
        match result {
            Ok(_) => {
                DB_QUERIES_SUCCESS.with_label_values(&["mark_block_processed"]).inc();
                Ok(())
            }
            Err(e) => {
                DB_QUERIES_FAILED.with_label_values(&["mark_block_processed"]).inc();
                Err(e)
            }
        }
    }

    pub async fn check_duplicate_share(&self, job_id: &str, address: &str, extranonce: &str, nonce: &str) -> Result<i64> {
        let result = sqlx::query_scalar(
            "SELECT COUNT(*) FROM shares WHERE job_id = ? AND address = ? AND extranonce = ? AND nonce = ?"
        )
        .bind(job_id)
        .bind(address)
        .bind(extranonce)
        .bind(nonce)
        .fetch_one(&self.pool)
        .await
        .context("Failed to check for duplicate share");
        match result {
            Ok(count) => {
                if count > 0 {
                    debug!(
                        "Duplicate share found for job_id={job_id}, address={address}, extranonce={extranonce}, nonce={nonce}"
                    );
                }
                DB_QUERIES_SUCCESS.with_label_values(&["check_duplicate_share"]).inc();
                Ok(count)
            }
            Err(e) => {
                DB_QUERIES_FAILED.with_label_values(&["check_duplicate_share"]).inc();
                Err(e)
            }
        }
    }
}