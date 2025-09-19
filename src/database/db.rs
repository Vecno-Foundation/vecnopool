//src/database/db.rs

use anyhow::{Context, Result};
use sqlx::postgres::{PgPoolOptions, PgPool};
use sqlx::Row;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use log::{debug, warn};

#[derive(Debug)]
pub struct Db {
    pub pool: PgPool,
}

#[derive(Debug, sqlx::FromRow)]
pub struct Block {
    pub reward_block_hash: String,
    pub miner_id: String,
    pub daa_score: i64,
    pub amount: i64,
    pub confirmations: i64,
    pub accepted: i64,
    pub timestamp: Option<i64>,
}

impl Db {
    pub async fn new() -> Result<Self> {
        let sql_uri = std::env::var("SQL_URI").context("SQL_URI must be set in .env")?;
        let pool = PgPoolOptions::new()
            .max_connections(20)
            .min_connections(5)
            .idle_timeout(std::time::Duration::from_secs(600))
            .max_lifetime(std::time::Duration::from_secs(1800))
            .connect(&sql_uri)
            .await
            .context("Failed to connect to PostgreSQL database")?;

        debug!("Initializing database tables");

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS shares (
                id BIGSERIAL PRIMARY KEY,
                address TEXT NOT NULL,
                difficulty BIGINT NOT NULL DEFAULT 1,
                timestamp BIGINT NOT NULL,
                job_id TEXT NOT NULL,
                daa_score BIGINT NOT NULL,
                extranonce TEXT NOT NULL,
                nonce TEXT NOT NULL,
                reward_block_hash TEXT,
                CONSTRAINT unique_reward_block_hash_nonce UNIQUE (reward_block_hash, nonce)
            )
            "#,
        )
        .execute(&pool)
        .await
        .context("Failed to create shares table")?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_shares_daa_score_address ON shares (daa_score, address)"
        )
        .execute(&pool)
        .await
        .context("Failed to create index on shares table (daa_score, address)")?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_shares_timestamp ON shares (timestamp)"
        )
        .execute(&pool)
        .await
        .context("Failed to create index on shares table (timestamp)")?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_shares_timestamp_address ON shares (timestamp, address)"
        )
        .execute(&pool)
        .await
        .context("Failed to create index on shares table (timestamp, address)")?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_shares_timestamp_address_difficulty ON shares (timestamp, address, difficulty)"
        )
        .execute(&pool)
        .await
        .context("Failed to create index on shares table (timestamp, address, difficulty)")?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_shares_address ON shares (address)"
        )
        .execute(&pool)
        .await
        .context("Failed to create index on shares table (address)")?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_shares_job_id ON shares (job_id)"
        )
        .execute(&pool)
        .await
        .context("Failed to create index on shares table (job_id)")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS balances (
                address TEXT PRIMARY KEY,
                available_balance BIGINT NOT NULL DEFAULT 0,
                total_earned_balance BIGINT NOT NULL DEFAULT 0
            )
            "#,
        )
        .execute(&pool)
        .await
        .context("Failed to create balances table")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS payments (
                id BIGSERIAL PRIMARY KEY,
                address TEXT NOT NULL,
                amount BIGINT NOT NULL,
                tx_id TEXT NOT NULL,
                timestamp BIGINT NOT NULL,
                notified BOOLEAN NOT NULL DEFAULT FALSE
            )
            "#,
        )
        .execute(&pool)
        .await
        .context("Failed to create payments table")?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_payments_timestamp ON payments (timestamp)"
        )
        .execute(&pool)
        .await
        .context("Failed to create index on payments table (timestamp)")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS blocks (
                reward_block_hash TEXT PRIMARY KEY,
                miner_id TEXT NOT NULL,
                daa_score BIGINT NOT NULL,
                pool_wallet TEXT NOT NULL,
                amount BIGINT NOT NULL,
                confirmations BIGINT NOT NULL DEFAULT 0,
                processed BIGINT NOT NULL DEFAULT 0,
                accepted BIGINT NOT NULL DEFAULT 0,
                job_id TEXT NOT NULL,
                extranonce TEXT NOT NULL,
                nonce TEXT NOT NULL,
                timestamp BIGINT
            )
            "#,
        )
        .execute(&pool)
        .await
        .context("Failed to create blocks table")?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_blocks_confirmations_processed ON blocks (confirmations, processed)"
        )
        .execute(&pool)
        .await
        .context("Failed to create index on blocks table (confirmations, processed)")?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_blocks_processed ON blocks (processed)"
        )
        .execute(&pool)
        .await
        .context("Failed to create index on blocks table (processed)")?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_blocks_miner_id ON blocks (miner_id)"
        )
        .execute(&pool)
        .await
        .context("Failed to create index on blocks table (miner_id)")?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_blocks_accepted ON blocks (accepted)"
        )
        .execute(&pool)
        .await
        .context("Failed to create index on blocks table (accepted)")?;

        let tables: Vec<(String,)> = sqlx::query_as(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        )
        .fetch_all(&pool)
        .await
        .context("Failed to verify database schema")?;
        let expected_tables = vec!["shares", "balances", "payments", "blocks"];
        for table in expected_tables {
            if !tables.iter().any(|(name,)| name == table) {
                return Err(anyhow::anyhow!("Database schema missing table: {}", table));
            }
        }

        let columns: Vec<(String,)> = sqlx::query_as(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'payments'"
        )
        .fetch_all(&pool)
        .await
        .context("Failed to verify payments table schema")?;
        if !columns.iter().any(|(name,)| name == "notified") {
            return Err(anyhow::anyhow!("Payments table missing 'notified' column"));
        }

        let block_columns: Vec<(String,)> = sqlx::query_as(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'blocks'"
        )
        .fetch_all(&pool)
        .await
        .context("Failed to verify blocks table schema")?;
        let expected_columns = vec![
            "reward_block_hash", "miner_id", "daa_score", "pool_wallet", "amount",
            "confirmations", "processed", "accepted", "job_id", "extranonce", "nonce", "timestamp"
        ];
        for column in expected_columns {
            if !block_columns.iter().any(|(name,)| name == column) {
                return Err(anyhow::anyhow!("Blocks table missing '{}' column", column));
            }
        }

        let indexes: Vec<(String,)> = sqlx::query_as(
            r#"
            SELECT indexname FROM pg_indexes
            WHERE schemaname = 'public' AND indexname IN (
                'idx_shares_daa_score_address',
                'idx_shares_timestamp',
                'idx_shares_timestamp_address',
                'idx_shares_timestamp_address_difficulty',
                'idx_shares_address',
                'idx_shares_job_id',
                'idx_blocks_confirmations_processed',
                'idx_blocks_processed',
                'idx_blocks_miner_id',
                'idx_blocks_accepted',
                'idx_payments_timestamp'
            )
            "#,
        )
        .fetch_all(&pool)
        .await
        .context("Failed to verify indexes")?;
        let expected_indexes = vec![
            "idx_shares_daa_score_address",
            "idx_shares_timestamp",
            "idx_shares_timestamp_address",
            "idx_shares_timestamp_address_difficulty",
            "idx_shares_address",
            "idx_shares_job_id",
            "idx_blocks_confirmations_processed",
            "idx_blocks_processed",
            "idx_blocks_miner_id",
            "idx_blocks_accepted",
            "idx_payments_timestamp",
        ];
        for index in expected_indexes {
            if !indexes.iter().any(|(name,)| name == index) {
                return Err(anyhow::anyhow!("Database schema missing index: {}", index));
            }
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
            r#"
            INSERT INTO shares (address, difficulty, timestamp, job_id, daa_score, extranonce, nonce, reward_block_hash)
            VALUES ($1, $2, $3, $4, $5, $6, $7, NULL)
            ON CONFLICT ON CONSTRAINT unique_reward_block_hash_nonce DO NOTHING
            "#,
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
            Ok(res) => {
                if res.rows_affected() > 0 {
                    // No metrics
                } else {
                    debug!(
                        "Skipping duplicate share for job_id={job_id}, address={address}, extranonce={extranonce}, nonce={nonce}"
                    );
                }
                Ok(())
            }
            Err(e) => Err(e.into())
        }
    }

    pub async fn get_total_shares(&self, window_secs: u64) -> Result<u64> {
        let start_time = SystemTime::now();
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs() as i64;
        let result: Result<i64, _> = sqlx::query_scalar(
            "SELECT COUNT(*) FROM shares WHERE timestamp >= $1"
        )
        .bind(current_time - window_secs as i64)
        .fetch_one(&self.pool)
        .await
        .context("Failed to query total shares");

        let elapsed = start_time.elapsed().unwrap_or_default().as_secs_f64();
        debug!("get_total_shares query took {} seconds for window_secs={}", elapsed, window_secs);

        match result {
            Ok(count) => Ok(count as u64),
            Err(e) => Err(e)
        }
    }

    pub async fn get_share_counts(&self, window_secs: Option<u64>) -> Result<HashMap<String, u64>> {
        let start_time = SystemTime::now();
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs() as i64;
        let query = if let Some(secs) = window_secs {
            sqlx::query(
                r#"
                SELECT address, SUM(difficulty)::BIGINT as total_difficulty
                FROM shares
                WHERE timestamp >= $1
                GROUP BY address
                "#,
            )
            .bind(current_time - secs as i64)
        } else {
            sqlx::query(
                r#"
                SELECT address, SUM(difficulty)::BIGINT as total_difficulty
                FROM shares
                GROUP BY address
                "#,
            )
        };

        let result = query
            .fetch_all(&self.pool)
            .await
            .context("Failed to get share counts");

        let elapsed = start_time.elapsed().unwrap_or_default().as_secs_f64();
        debug!("get_share_counts query took {} seconds", elapsed);

        match result {
            Ok(rows) => {
                let mut sums = HashMap::new();
                for row in rows {
                    let address: String = row.get(0);
                    let total_difficulty: i64 = row.get(1);
                    sums.insert(address, total_difficulty as u64);
                }
                Ok(sums)
            }
            Err(e) => Err(e)
        }
    }

    pub async fn get_shares_in_time_window(&self, timestamp: i64, window_duration_secs: u64) -> Result<HashMap<String, u64>> {
        let start_time = SystemTime::now();
        let start_timestamp = timestamp.saturating_sub(window_duration_secs as i64);
        let result = sqlx::query(
            r#"
            SELECT address, SUM(difficulty)::BIGINT as total_difficulty
            FROM shares
            WHERE timestamp >= $1 AND timestamp < $2
            GROUP BY address
            "#,
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
                let mut sums = HashMap::new();
                for row in rows {
                    let address: String = row.get(0);
                    let total_difficulty: i64 = row.get(1);
                    sums.insert(address, total_difficulty as u64);
                }
                Ok(sums)
            }
            Err(e) => Err(e)
        }
    }

    pub async fn add_balance(&self, _id: &str, address: &str, amount: u64) -> Result<()> {
        let start_time = SystemTime::now();
        // Strip worker suffix from address
        let base_address = address.split('.').next().unwrap_or(address);
        let result = sqlx::query(
            r#"
            INSERT INTO balances (address, available_balance, total_earned_balance)
            VALUES ($1, $2, $3)
            ON CONFLICT (address)
            DO UPDATE SET
                available_balance = balances.available_balance + EXCLUDED.available_balance,
                total_earned_balance = balances.total_earned_balance + EXCLUDED.total_earned_balance
            "#,
        )
        .bind(base_address)
        .bind(amount as i64)
        .bind(amount as i64)
        .execute(&self.pool)
        .await
        .context("Failed to add balance");

        let elapsed = start_time.elapsed().unwrap_or_default().as_secs_f64();
        debug!("add_balance query took {} seconds for address={}", elapsed, base_address);

        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(e)
        }
    }

    pub async fn add_block_details(
        &self,
        _block_hash: &str,
        miner_id: &str,
        reward_block_hash: &str,
        job_id: u8,
        extranonce: &str,
        nonce: &str,
        daa_score: u64,
        pool_wallet: &str,
        amount: u64,
    ) -> Result<()> {
        let start_time = SystemTime::now();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs() as i64;

        debug!("Adding block details: reward_block_hash={}, miner_id={}, job_id={}, daa_score={}, amount={}", 
               reward_block_hash, miner_id, job_id, daa_score, amount);

        let result = sqlx::query(
            r#"
            INSERT INTO blocks (
                reward_block_hash, miner_id, daa_score, pool_wallet, amount, confirmations, processed, accepted,
                job_id, extranonce, nonce, timestamp
            ) VALUES ($1, $2, $3, $4, $5, 0, 0, 0, $6, $7, $8, $9)
            ON CONFLICT (reward_block_hash) DO UPDATE SET
                miner_id = EXCLUDED.miner_id,
                daa_score = EXCLUDED.daa_score,
                pool_wallet = EXCLUDED.pool_wallet,
                amount = EXCLUDED.amount,
                job_id = EXCLUDED.job_id,
                extranonce = EXCLUDED.extranonce,
                nonce = EXCLUDED.nonce,
                timestamp = EXCLUDED.timestamp
            "#,
        )
        .bind(reward_block_hash)
        .bind(miner_id)
        .bind(daa_score as i64)
        .bind(pool_wallet)
        .bind(amount as i64)
        .bind(job_id.to_string())
        .bind(extranonce)
        .bind(nonce)
        .bind(timestamp)
        .execute(&self.pool)
        .await
        .context("Failed to add block details");

        let elapsed = start_time.elapsed().unwrap_or_default().as_secs_f64();
        if elapsed > 1.0 {
            warn!("add_block_details query took {} seconds for reward_block_hash={}", elapsed, reward_block_hash);
        } else {
            debug!("add_block_details query took {} seconds for reward_block_hash={}", elapsed, reward_block_hash);
        }

        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(e)
        }
    }

    pub async fn get_unconfirmed_blocks(&self) -> Result<Vec<Block>> {
        let start_time = SystemTime::now();
        let result = sqlx::query_as::<_, Block>(
            r#"
            SELECT reward_block_hash, miner_id, daa_score, pool_wallet, amount, confirmations, processed, accepted,
                   job_id, extranonce, nonce, timestamp
            FROM blocks WHERE processed = 0
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .context("Failed to get unconfirmed blocks");

        let elapsed = start_time.elapsed().unwrap_or_default().as_secs_f64();
        debug!("get_unconfirmed_blocks query took {} seconds", elapsed);

        match result {
            Ok(rows) => {
                for block in &rows {
                    debug!("Unconfirmed block: reward_block_hash={}, miner_id={}", block.reward_block_hash, block.miner_id);
                }
                Ok(rows)
            }
            Err(e) => Err(e)
        }
    }

    pub async fn get_blocks_for_rewards(&self) -> Result<Vec<Block>> {
        let start_time = SystemTime::now();
        let result = sqlx::query_as::<_, Block>(
            r#"
            SELECT reward_block_hash, miner_id, daa_score, pool_wallet, amount, confirmations, processed, accepted,
                   job_id, extranonce, nonce, timestamp
            FROM blocks WHERE confirmations >= 100 AND accepted = 1 AND processed = 0
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .context("Failed to get blocks for rewards");

        let elapsed = start_time.elapsed().unwrap_or_default().as_secs_f64();
        debug!("get_blocks_for_rewards query took {} seconds", elapsed);

        match result {
            Ok(rows) => {
                for block in &rows {
                    debug!("Block for rewards: reward_block_hash={}, miner_id={}", block.reward_block_hash, block.miner_id);
                }
                Ok(rows)
            }
            Err(e) => Err(e)
        }
    }

    pub async fn update_block_status(&self, reward_block_hash: &str, accepted: bool) -> Result<()> {
        let start_time = SystemTime::now();
        let result = sqlx::query(
            r#"
            UPDATE blocks SET accepted = $1 WHERE reward_block_hash = $2
            "#,
        )
        .bind(if accepted { 1 } else { 0 })
        .bind(reward_block_hash)
        .execute(&self.pool)
        .await
        .context("Failed to update block status");

        let elapsed = start_time.elapsed().unwrap_or_default().as_secs_f64();
        debug!("update_block_status query took {} seconds for reward_block_hash={}", elapsed, reward_block_hash);

        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(e)
        }
    }

    pub async fn check_duplicate_share(&self, reward_block_hash: &str, nonce: &str) -> Result<i64> {
        let start_time = SystemTime::now();
        let result = sqlx::query_scalar(
            r#"
            SELECT COUNT(*) FROM shares WHERE reward_block_hash = $1 AND nonce = $2
            "#,
        )
        .bind(reward_block_hash)
        .bind(nonce)
        .fetch_one(&self.pool)
        .await
        .context("Failed to check for duplicate share by hash");

        let elapsed = start_time.elapsed().unwrap_or_default().as_secs_f64();
        debug!(
            "check_duplicate_share query took {} seconds for reward_block_hash={}, nonce={}",
            elapsed, reward_block_hash, nonce
        );

        match result {
            Ok(count) => {
                if count > 0 {
                    debug!(
                        "Duplicate share found for reward_block_hash={reward_block_hash}, nonce={nonce}"
                    );
                }
                Ok(count)
            }
            Err(e) => Err(e)
        }
    }

    pub async fn cleanup_old_shares(&self, retention_period_secs: i64) -> Result<()> {
        let start_time = SystemTime::now();
        let cutoff_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64 - retention_period_secs;

        let result = sqlx::query(
            r#"
            DELETE FROM shares WHERE timestamp < $1
            "#,
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
                Ok(())
            }
            Err(e) => Err(e)
        }
    }

    pub async fn cleanup_processed_blocks(&self) -> Result<()> {
        let start_time = SystemTime::now();
        let result = sqlx::query(
            r#"
            DELETE FROM blocks WHERE processed = 1
            "#,
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
                Ok(())
            }
            Err(e) => Err(e)
        }
    }

    pub async fn cleanup_unaccepted_blocks(&self) -> Result<()> {
        let start_time = SystemTime::now();
        let result = sqlx::query(
            r#"
            DELETE FROM blocks WHERE accepted = 0
            "#,
        )
        .execute(&self.pool)
        .await
        .context("Failed to clean up unaccepted blocks");

        let elapsed = start_time.elapsed().unwrap_or_default().as_secs_f64();
        debug!("cleanup_unaccepted_blocks query took {} seconds", elapsed);

        match result {
            Ok(res) => {
                let rows_affected = res.rows_affected();
                if rows_affected > 0 {
                    debug!("Cleaned up {} unaccepted blocks", rows_affected);
                }
                Ok(())
            }
            Err(e) => Err(e)
        }
    }
}