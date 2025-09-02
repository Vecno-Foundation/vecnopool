use anyhow::Result;
use dashmap::DashMap;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool};
use std::path::Path;
use sqlx::Row;
use std::collections::VecDeque;
use crate::treasury::sharehandler::Contribution;
use log::debug;
use anyhow::Context;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct Db {
    pub pool: SqlitePool,
}

#[derive(Debug, sqlx::FromRow)]
pub struct BlockDetails {
    pub miner_id: String,
    pub reward_block_hash: String,
    pub daa_score: i64,
    pub pool_wallet: String,
    pub amount: i64,
}

impl Db {
    pub async fn new(path: &Path) -> Result<Self> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true);
        let pool = SqlitePool::connect_with(options).await?;
        
        debug!("Creating 'shares' table");
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS shares (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                address TEXT NOT NULL,
                difficulty INTEGER NOT NULL,
                timestamp INTEGER NOT NULL,
                job_id TEXT NOT NULL,
                daa_score INTEGER NOT NULL,
                extranonce TEXT NOT NULL,
                nonce TEXT NOT NULL
            )"
        )
        .execute(&pool)
        .await
        .context("Failed to create shares table")?;

        debug!("Creating 'balances' table");
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS balances (
                id TEXT NOT NULL,
                address TEXT NOT NULL,
                pending_balance INTEGER NOT NULL DEFAULT 0,
                available_balance INTEGER NOT NULL DEFAULT 0,
                total_earned_balance INTEGER NOT NULL DEFAULT 0,
                UNIQUE(id, address)
            )"
        )
        .execute(&pool)
        .await
        .context("Failed to create balances table")?;

        debug!("Creating 'payments' table");
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

        debug!("Creating 'blocks' table");
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS blocks (
                block_hash TEXT NOT NULL,
                miner_id TEXT NOT NULL,
                reward_block_hash TEXT NOT NULL,
                reward_txn_id TEXT NOT NULL,
                daa_score INTEGER NOT NULL,
                pool_wallet TEXT NOT NULL,
                amount INTEGER NOT NULL,
                confirmations INTEGER NOT NULL DEFAULT 0,
                UNIQUE(block_hash, reward_block_hash)
            )"
        )
        .execute(&pool)
        .await
        .context("Failed to create blocks table")?;

        debug!("Database initialized successfully");
        Ok(Db { pool })
    }

    // Rest of the methods remain unchanged
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
        sqlx::query(
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
        .await?;
        Ok(())
    }

    pub async fn load_balances(&self, balances: &DashMap<String, u64>) -> Result<()> {
        let rows = sqlx::query("SELECT address, available_balance FROM balances")
            .fetch_all(&self.pool)
            .await?;

        for row in rows {
            let address: String = row.get(0);
            let balance: i64 = row.get(1);
            balances.insert(address, balance as u64);
        }
        Ok(())
    }

    pub async fn load_recent_shares(&self, share_window: &mut VecDeque<Contribution>, n: usize) -> Result<u64> {
        let rows = sqlx::query_as::<_, Contribution>(
            "SELECT address, difficulty, timestamp, job_id, daa_score, extranonce, nonce
             FROM shares
             ORDER BY timestamp DESC
             LIMIT ?"
        )
        .bind(n as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut total_difficulty = 0;
        for contribution in rows {
            total_difficulty += contribution.difficulty as u64;
            share_window.push_front(contribution);
        }
        Ok(total_difficulty)
    }

    pub async fn get_total_submissions_all(&self) -> Result<DashMap<String, u64>> {
        let sums = DashMap::new();
        let rows = sqlx::query("SELECT address, COUNT(*) as count FROM shares GROUP BY address")
            .fetch_all(&self.pool)
            .await?;

        for row in rows {
            let address: String = row.get(0);
            let count: i64 = row.get(1);
            sums.insert(address, count as u64);
        }
        Ok(sums)
    }

    pub async fn get_total_submissions(&self, address: &str) -> Result<u64> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM shares WHERE address = ?")
            .bind(address)
            .fetch_one(&self.pool)
            .await
            .unwrap_or(0);
        Ok(count as u64)
    }

    pub async fn add_pending_balance(&self, id: &str, address: &str, amount: u64) -> Result<()> {
        sqlx::query(
            "INSERT OR REPLACE INTO balances (id, address, pending_balance, available_balance, total_earned_balance)
             VALUES (?, ?, COALESCE((SELECT pending_balance FROM balances WHERE id = ? AND address = ?), 0) + ?,
                     COALESCE((SELECT available_balance FROM balances WHERE id = ? AND address = ?), 0),
                     COALESCE((SELECT total_earned_balance FROM balances WHERE id = ? AND address = ?), 0) + ?)"
        )
        .bind(id)
        .bind(address)
        .bind(amount as i64)
        .bind(id)
        .bind(address)
        .bind(id)
        .bind(address)
        .bind(amount as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn move_pending_to_available(&self, id: &str, address: &str, amount: u64) -> Result<()> {
        sqlx::query(
            "UPDATE balances SET pending_balance = pending_balance - ?, available_balance = available_balance + ?
             WHERE id = ? AND address = ?"
        )
        .bind(amount as i64)
        .bind(amount as i64)
        .bind(id)
        .bind(address)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn reset_available_balance(&self, address: &str) -> Result<()> {
        sqlx::query("UPDATE balances SET available_balance = 0 WHERE address = ?")
            .bind(address)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn get_balances_for_payout(&self, min_balance: u64) -> Result<Vec<(String, String, u64)>> {
        let rows = sqlx::query(
            "SELECT id, address, available_balance FROM balances WHERE available_balance >= ?"
        )
        .bind(min_balance as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut balances = Vec::new();
        for row in rows {
            let id: String = row.get(0);
            let address: String = row.get(1);
            let balance: i64 = row.get(2);
            balances.push((id, address, balance as u64));
        }
        Ok(balances)
    }

    pub async fn add_payment(&self, address: &str, amount: u64, tx_id: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO payments (address, amount, tx_id, timestamp) VALUES (?, ?, ?, ?)"
        )
        .bind(address)
        .bind(amount as i64)
        .bind(tx_id)
        .bind(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn add_block_details(
        &self,
        block_hash: &str,
        miner_id: &str,
        reward_block_hash: &str,
        reward_txn_id: &str,
        daa_score: u64,
        pool_wallet: &str,
        amount: u64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT OR REPLACE INTO blocks (block_hash, miner_id, reward_block_hash, reward_txn_id, daa_score, pool_wallet, amount, confirmations)
             VALUES (?, ?, ?, ?, ?, ?, ?, 0)"
        )
        .bind(block_hash)
        .bind(miner_id)
        .bind(reward_block_hash)
        .bind(reward_txn_id)
        .bind(daa_score as i64)
        .bind(pool_wallet)
        .bind(amount as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_unconfirmed_blocks(&self) -> Result<Vec<BlockDetails>> {
        let rows = sqlx::query_as::<_, BlockDetails>(
            "SELECT block_hash, miner_id, reward_block_hash, reward_txn_id, daa_score, pool_wallet, amount
             FROM blocks WHERE confirmations < 100"
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn update_block_confirmations(&self, reward_block_hash: &str, confirmations: u64) -> Result<()> {
        sqlx::query(
            "UPDATE blocks SET confirmations = ? WHERE reward_block_hash = ?"
        )
        .bind(confirmations as i64)
        .bind(reward_block_hash)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn check_duplicate_share(&self, job_id: &str, address: &str, extranonce: &str, nonce: &str) -> Result<i64> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM shares WHERE job_id = ? AND address = ? AND extranonce = ? AND nonce = ?"
        )
        .bind(job_id)
        .bind(address)
        .bind(extranonce)
        .bind(nonce)
        .fetch_one(&self.pool)
        .await
        .unwrap_or(0);
        Ok(count)
    }
}