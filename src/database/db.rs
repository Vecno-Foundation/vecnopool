use anyhow::{Context, Result};
use dashmap::DashMap;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool};
use std::path::Path;
use sqlx::Row;
use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::treasury::sharehandler::Contribution;
use log::{debug, warn};
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
    pub timestamp: Option<i64>,
}

impl Db {
    pub async fn new(path: &Path) -> Result<Self> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true);
        let pool = SqlitePool::connect_with(options)
            .await
            .context("Failed to connect to SQLite database")?;

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
                reward_block_hash TEXT,
                UNIQUE(reward_block_hash, nonce)
            )"
        )
        .execute(&pool)
        .await
        .context("Failed to create shares table")?;
        DB_QUERIES_SUCCESS.with_label_values(&["create_shares_table"]).inc();

        // Create index on daa_score, address, and timestamp
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_shares_daa_score_address ON shares (daa_score, address)"
        )
        .execute(&pool)
        .await
        .context("Failed to create index on shares table (daa_score, address)")?;
        DB_QUERIES_SUCCESS.with_label_values(&["create_shares_index_daa_score"]).inc();

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_shares_timestamp ON shares (timestamp)"
        )
        .execute(&pool)
        .await
        .context("Failed to create index on shares table (timestamp)")?;
        DB_QUERIES_SUCCESS.with_label_values(&["create_shares_index_timestamp"]).inc();

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
                timestamp INTEGER NOT NULL,
                notified BOOLEAN NOT NULL DEFAULT FALSE
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
                processed INTEGER NOT NULL DEFAULT 0,
                timestamp INTEGER
            )"
        )
        .execute(&pool)
        .await
        .context("Failed to create blocks table")?;
        DB_QUERIES_SUCCESS.with_label_values(&["create_blocks_table"]).inc();

        // Verify schema integrity
        let tables: Vec<(String,)> = sqlx::query_as("SELECT name FROM sqlite_master WHERE type='table'")
            .fetch_all(&pool)
            .await
            .context("Failed to verify database schema")?;
        let expected_tables = vec!["shares", "balances", "payments", "blocks"];
        for table in expected_tables {
            if !tables.iter().any(|(name,)| name == table) {
                return Err(anyhow::anyhow!("Database schema missing table: {}", table));
            }
        }

        // Verify payments table has notified column
        let columns: Vec<(String,)> = sqlx::query_as(
            "SELECT name FROM pragma_table_info('payments')"
        )
        .fetch_all(&pool)
        .await
        .context("Failed to verify payments table schema")?;
        if !columns.iter().any(|(name,)| name == "notified") {
            return Err(anyhow::anyhow!("Payments table missing 'notified' column"));
        }

        // Verify blocks table has timestamp column
        let block_columns: Vec<(String,)> = sqlx::query_as(
            "SELECT name FROM pragma_table_info('blocks')"
        )
        .fetch_all(&pool)
        .await
        .context("Failed to verify blocks table schema")?;
        if !block_columns.iter().any(|(name,)| name == "timestamp") {
            sqlx::query("ALTER TABLE blocks ADD COLUMN timestamp INTEGER")
                .execute(&pool)
                .await
                .context("Failed to add timestamp column to blocks table")?;
            DB_QUERIES_SUCCESS.with_label_values(&["alter_blocks_table"]).inc();
        }

        // Verify index exists
        let indexes: Vec<(String,)> = sqlx::query_as(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_shares_daa_score_address'"
        )
        .fetch_all(&pool)
        .await
        .context("Failed to verify indexes")?;
        if indexes.is_empty() {
            return Err(anyhow::anyhow!("Database schema missing index: idx_shares_daa_score_address"));
        }

        debug!("Database initialized successfully");
        Ok(Db { pool })
    }

    pub async fn record_share(
        &self,
        address: &str,
        difficulty: i64,
        timestamp: u64,
        job_id: &str,
        daa_score: u64,
        extranonce: &str,
        nonce: &str,
    ) -> Result<()> {
        let start_time = SystemTime::now();
        let result = sqlx::query(
            "INSERT INTO shares (address, difficulty, timestamp, job_id, daa_score, extranonce, nonce, reward_block_hash)
             VALUES (?, ?, ?, ?, ?, ?, ?, NULL)"
        )
        .bind(address)
        .bind(difficulty)
        .bind(timestamp as i64)
        .bind(job_id)
        .bind(daa_score as i64)
        .bind(extranonce)
        .bind(nonce)
        .execute(&self.pool)
        .await;

        let elapsed = start_time.elapsed().unwrap_or_default().as_secs_f64();
        debug!("record_share query took {} seconds for address={}", elapsed, address);

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
        let start_time = SystemTime::now();
        let result = sqlx::query_as::<_, Contribution>(
            "SELECT address, difficulty, timestamp, job_id, daa_score, extranonce, nonce, reward_block_hash
             FROM shares
             ORDER BY timestamp DESC
             LIMIT ?"
        )
        .bind(n as i64)
        .fetch_all(&self.pool)
        .await
        .context("Failed to load recent shares");

        let elapsed = start_time.elapsed().unwrap_or_default().as_secs_f64();
        debug!("load_recent_shares query took {} seconds", elapsed);

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
        let start_time = SystemTime::now();
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

        let elapsed = start_time.elapsed().unwrap_or_default().as_secs_f64();
        debug!("get_share_counts query took {} seconds", elapsed);

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

    pub async fn get_shares_in_time_window(&self, timestamp: i64, window_duration_secs: u64) -> Result<DashMap<String, u64>> {
        let start_time = SystemTime::now();
        let start_timestamp = timestamp.saturating_sub(window_duration_secs as i64);
        let result = sqlx::query(
            "SELECT address, SUM(difficulty) as total_difficulty
             FROM shares
             WHERE timestamp >= ? AND timestamp < ?
             GROUP BY address"
        )
        .bind(start_timestamp)
        .bind(timestamp)
        .fetch_all(&self.pool)
        .await
        .context("Failed to get shares in time window");

        let elapsed = start_time.elapsed().unwrap_or_default().as_secs_f64();
        if elapsed > 1.0 {
            warn!(
                "get_shares_in_time_window query took {} seconds for timestamp={} and window_duration_secs={}",
                elapsed, timestamp, window_duration_secs
            );
        } else {
            debug!(
                "get_shares_in_time_window query took {} seconds for timestamp={} and window_duration_secs={}",
                elapsed, timestamp, window_duration_secs
            );
        }

        match result {
            Ok(rows) => {
                let sums = DashMap::new();
                for row in rows {
                    let address: String = row.get(0);
                    let total_difficulty: i64 = row.get(1);
                    sums.insert(address, total_difficulty as u64);
                }
                DB_QUERIES_SUCCESS.with_label_values(&["get_shares_in_time_window"]).inc();
                Ok(sums)
            }
            Err(e) => {
                DB_QUERIES_FAILED.with_label_values(&["get_shares_in_time_window"]).inc();
                Err(e)
            }
        }
    }

    pub async fn add_balance(&self, id: &str, address: &str, amount: u64) -> Result<()> {
        let start_time = SystemTime::now();
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

        let elapsed = start_time.elapsed().unwrap_or_default().as_secs_f64();
        debug!("add_balance query took {} seconds for address={}", elapsed, address);

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

    pub async fn add_block_details(
        &self,
        _block_hash: &str,
        miner_id: &str,
        reward_block_hash: &str,
        _reward_txn_id: &str,
        daa_score: u64,
        pool_wallet: &str,
        _amount: u64,
        pool_fee: f64,
    ) -> Result<()> {
        let start_time = SystemTime::now();
        let mut amount = get_block_reward(daa_score)
            .context(format!("Failed to calculate block reward for daa_score {}", daa_score))?;
        amount = ((amount as f64) * (1.0 - pool_fee / 100.0)) as u64;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs() as i64;

        let result = sqlx::query(
            "INSERT OR REPLACE INTO blocks (reward_block_hash, miner_id, daa_score, pool_wallet, amount, confirmations, processed, timestamp)
             VALUES (?, ?, ?, ?, ?, 0, 0, ?)"
        )
        .bind(reward_block_hash)
        .bind(miner_id)
        .bind(daa_score as i64)
        .bind(pool_wallet)
        .bind(amount as i64)
        .bind(timestamp)
        .execute(&self.pool)
        .await
        .context("Failed to add block details");

        let elapsed = start_time.elapsed().unwrap_or_default().as_secs_f64();
        debug!("add_block_details query took {} seconds for reward_block_hash={}", elapsed, reward_block_hash);

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
        let start_time = SystemTime::now();
        let result = sqlx::query_as::<_, BlockDetails>(
            "SELECT miner_id, reward_block_hash, daa_score, amount, confirmations, timestamp
             FROM blocks WHERE confirmations < 100 AND processed = 0"
        )
        .fetch_all(&self.pool)
        .await
        .context("Failed to get unconfirmed blocks");

        let elapsed = start_time.elapsed().unwrap_or_default().as_secs_f64();
        debug!("get_unconfirmed_blocks query took {} seconds", elapsed);

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
        let start_time = SystemTime::now();
        let result = sqlx::query(
            "UPDATE blocks SET confirmations = ? WHERE reward_block_hash = ?"
        )
        .bind(confirmations as i64)
        .bind(reward_block_hash)
        .execute(&self.pool)
        .await
        .context("Failed to update block confirmations");

        let elapsed = start_time.elapsed().unwrap_or_default().as_secs_f64();
        debug!("update_block_confirmations query took {} seconds for reward_block_hash={}", elapsed, reward_block_hash);

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
        let start_time = SystemTime::now();
        let result = sqlx::query(
            "UPDATE blocks SET processed = 1 WHERE reward_block_hash = ?"
        )
        .bind(reward_block_hash)
        .execute(&self.pool)
        .await
        .context("Failed to mark block as processed");

        let elapsed = start_time.elapsed().unwrap_or_default().as_secs_f64();
        debug!("mark_block_processed query took {} seconds for reward_block_hash={}", elapsed, reward_block_hash);

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

    pub async fn check_duplicate_share_by_hash(&self, reward_block_hash: &str, nonce: &str) -> Result<i64> {
        let start_time = SystemTime::now();
        let result = sqlx::query_scalar(
            "SELECT COUNT(*) FROM shares WHERE reward_block_hash = ? AND nonce = ?"
        )
        .bind(reward_block_hash)
        .bind(nonce)
        .fetch_one(&self.pool)
        .await
        .context("Failed to check for duplicate share by hash");

        let elapsed = start_time.elapsed().unwrap_or_default().as_secs_f64();
        debug!(
            "check_duplicate_share_by_hash query took {} seconds for reward_block_hash={}, nonce={}",
            elapsed, reward_block_hash, nonce
        );

        match result {
            Ok(count) => {
                if count > 0 {
                    debug!(
                        "Duplicate share found for reward_block_hash={reward_block_hash}, nonce={nonce}"
                    );
                }
                DB_QUERIES_SUCCESS.with_label_values(&["check_duplicate_share_by_hash"]).inc();
                Ok(count)
            }
            Err(e) => {
                DB_QUERIES_FAILED.with_label_values(&["check_duplicate_share_by_hash"]).inc();
                Err(e)
            }
        }
    }

    pub async fn cleanup_old_shares(&self, retention_period_secs: i64) -> Result<()> {
        let start_time = SystemTime::now();
        let cutoff_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64 - retention_period_secs;

        let result = sqlx::query(
            "DELETE FROM shares WHERE timestamp < ?"
        )
        .bind(cutoff_time)
        .execute(&self.pool)
        .await
        .context("Failed to clean up old shares");

        let elapsed = start_time.elapsed().unwrap_or_default().as_secs_f64();
        debug!("cleanup_old_shares query took {} seconds for cutoff_time={}", elapsed, cutoff_time);

        match result {
            Ok(res) => {
                let rows_affected = res.rows_affected();
                if rows_affected > 0 {
                    debug!("Cleaned up {} old shares older than {} seconds", rows_affected, retention_period_secs);
                }
                DB_QUERIES_SUCCESS.with_label_values(&["cleanup_old_shares"]).inc();
                Ok(())
            }
            Err(e) => {
                DB_QUERIES_FAILED.with_label_values(&["cleanup_old_shares"]).inc();
                Err(e)
            }
        }
    }

    pub async fn cleanup_processed_blocks(&self) -> Result<()> {
        let start_time = SystemTime::now();
        let result = sqlx::query(
            "DELETE FROM blocks WHERE processed = 1"
        )
        .execute(&self.pool)
        .await
        .context("Failed to clean up processed blocks");

        let elapsed = start_time.elapsed().unwrap_or_default().as_secs_f64();
        debug!("cleanup_processed_blocks query took {} seconds", elapsed);

        match result {
            Ok(res) => {
                let rows_affected = res.rows_affected();
                if rows_affected > 0 {
                    debug!("Cleaned up {} processed blocks", rows_affected);
                }
                DB_QUERIES_SUCCESS.with_label_values(&["cleanup_processed_blocks"]).inc();
                Ok(())
            }
            Err(e) => {
                DB_QUERIES_FAILED.with_label_values(&["cleanup_processed_blocks"]).inc();
                Err(e)
            }
        }
    }
}