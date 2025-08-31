use anyhow::Result;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use rusqlite::{params, Connection};
use std::collections::VecDeque;
use std::path::Path;
use std::sync::{Arc, Mutex};
use rusqlite::OptionalExtension;
use std::collections::HashMap;
use prometheus::{IntCounterVec, register_int_counter_vec};
use lazy_static::lazy_static;

use crate::database::Contribution;

lazy_static! {
    static ref SHARES_RECORDED: IntCounterVec = register_int_counter_vec!(
        "shares_recorded",
        "Number of shares recorded in the database",
        &["address"]
    ).unwrap();
}

#[derive(Debug)]
pub struct Db {
    conn: Arc<Mutex<Connection>>,
}

impl Db {
    pub fn new(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS balances (
                id TEXT PRIMARY KEY,
                address TEXT NOT NULL,
                balance INTEGER DEFAULT 0,
                rebate INTEGER DEFAULT 0
            )",
            [],
        )?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS shares (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                address TEXT NOT NULL,
                difficulty INTEGER NOT NULL,
                timestamp INTEGER NOT NULL,
                job_id TEXT NOT NULL,
                daa_score INTEGER NOT NULL,
                extranonce TEXT NOT NULL,
                nonce TEXT NOT NULL
            )",
            [],
        )?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS block_details (
                mined_block_hash TEXT PRIMARY KEY,
                miner_id TEXT NOT NULL,
                pool_address TEXT NOT NULL,
                reward_block_hash TEXT NOT NULL,
                wallet TEXT NOT NULL,
                daa_score INTEGER NOT NULL,
                miner_reward INTEGER NOT NULL,
                timestamp TEXT NOT NULL
            )",
            [],
        )?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS wallet_total (
                address TEXT PRIMARY KEY,
                total INTEGER NOT NULL
            )",
            [],
        )?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet_address TEXT NOT NULL,
                amount INTEGER NOT NULL,
                timestamp TEXT NOT NULL,
                transaction_hash TEXT NOT NULL
            )",
            [],
        )?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS reward_block_details (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                reward_block_hash TEXT UNIQUE NOT NULL,
                reward_txn_id TEXT UNIQUE NOT NULL
            )",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS shares_timestamp_idx ON shares (timestamp)",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS shares_daa_score_idx ON shares (daa_score)",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS shares_job_id_idx ON shares (job_id, address, extranonce, nonce)",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS block_details_daa_score_idx ON block_details (daa_score)",
            [],
        )?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn get_conn(&self) -> std::sync::MutexGuard<Connection> {
        self.conn.lock().unwrap()
    }

    pub fn load_balances(&self, balances: &DashMap<String, u64>) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT address, balance FROM balances")?;
        let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
        for row in rows {
            let (addr, bal): (String, i64) = row?;
            balances.insert(addr, bal as u64);
        }
        Ok(())
    }

    pub fn load_recent_shares(&self, share_window: &mut VecDeque<Contribution>, n: usize) -> Result<u64> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT address, difficulty, timestamp, job_id, daa_score, extranonce, nonce FROM shares ORDER BY id DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map([n as i32], |row| {
            Ok(Contribution {
                address: row.get(0)?,
                difficulty: row.get(1)?,
                timestamp: row.get(2)?,
                job_id: row.get(3)?,
                daa_score: row.get(4)?,
                extranonce: row.get(5)?,
                nonce: row.get(6)?,
            })
        })?;
        let mut total = 0u64;
        for row in rows {
            let contribution = row?;
            share_window.push_front(contribution.clone());
            total += contribution.difficulty;
        }
        Ok(total)
    }

    pub fn record_share(&self, address: &str, difficulty: u64, timestamp: u64, job_id: &str, daa_score: u64, extranonce: &str, nonce: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let result = conn.execute(
            "INSERT INTO shares (address, difficulty, timestamp, job_id, daa_score, extranonce, nonce) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![address, difficulty as i64, timestamp as i64, job_id, daa_score as i64, extranonce, nonce],
        );
        if let Err(e) = result {
            log::error!("Failed to record share for address {}: {}", address, e);
            return Err(e.into());
        }
        SHARES_RECORDED.with_label_values(&[address]).inc();
        Ok(())
    }

    pub fn update_balance(&self, addr: &str, delta: u64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO balances (id, address, balance, rebate)
             VALUES (?1, ?1, COALESCE((SELECT balance FROM balances WHERE id = ?1), 0) + ?2, COALESCE((SELECT rebate FROM balances WHERE id = ?1), 0))",
            params![addr, delta as i64],
        )?;
        Ok(())
    }

    pub fn update_balance_with_rebate(&self, addr: &str, delta: u64, rebate: u64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO balances (id, address, balance, rebate)
             VALUES (?1, ?1, COALESCE((SELECT balance FROM balances WHERE id = ?1), 0) + ?2, COALESCE((SELECT rebate FROM balances WHERE id = ?1), 0) + ?3)",
            params![addr, delta as i64, rebate as i64],
        )?;
        Ok(())
    }

    pub fn add_balance(&self, miner_id: &str, addr: &str, amount: u64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let id = format!("{}_{}", miner_id, addr);
        conn.execute(
            "INSERT OR REPLACE INTO balances (id, address, balance, rebate)
             VALUES (?1, ?2, COALESCE((SELECT balance FROM balances WHERE id = ?1), 0) + ?3, COALESCE((SELECT rebate FROM balances WHERE id = ?1), 0))",
            params![id, addr, amount as i64],
        )?;
        Ok(())
    }

    pub fn add_balance_with_wallet_total(&self, miner_id: &str, wallet: &str, balance: u64, rebate: u64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("BEGIN", [])?;

        let id = format!("{}_{}", miner_id, wallet);
        conn.execute(
            "INSERT OR REPLACE INTO balances (id, address, balance, rebate)
             VALUES (?1, ?2, COALESCE((SELECT balance FROM balances WHERE id = ?1), 0) + ?3, COALESCE((SELECT rebate FROM balances WHERE id = ?1), 0) + ?4)",
            params![id, wallet, balance as i64, rebate as i64],
        )?;

        conn.execute(
            "INSERT OR REPLACE INTO wallet_total (address, total)
             VALUES (?1, COALESCE((SELECT total FROM wallet_total WHERE address = ?1), 0) + ?2)",
            params![wallet, balance as i64],
        )?;

        conn.execute("COMMIT", [])?;
        Ok(())
    }

    pub fn reset_balance_by_address(&self, wallet: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE balances SET balance = 0 WHERE address = ?1",
            [wallet],
        )?;
        Ok(())
    }

    pub fn add_block_details(
        &self,
        mined_block_hash: &str,
        miner_id: &str,
        reward_block_hash: &str,
        wallet: &str,
        daa_score: u64,
        pool_address: &str,
        miner_reward: u64,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO block_details (mined_block_hash, miner_id, pool_address, reward_block_hash, wallet, daa_score, miner_reward, timestamp)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT (mined_block_hash) DO UPDATE SET
                reward_block_hash = EXCLUDED.reward_block_hash,
                miner_reward = EXCLUDED.miner_reward",
            params![
                mined_block_hash,
                miner_id,
                pool_address,
                reward_block_hash,
                wallet,
                daa_score as i64,
                miner_reward as i64,
                Utc::now().to_rfc3339()
            ],
        )?;
        Ok(())
    }

    pub fn add_reward_details(&self, reward_block_hash: &str, reward_txn_id: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO reward_block_details (reward_block_hash, reward_txn_id) VALUES (?1, ?2)
             ON CONFLICT (reward_txn_id) DO UPDATE SET reward_block_hash = EXCLUDED.reward_block_hash",
            params![reward_block_hash, reward_txn_id],
        )?;
        Ok(())
    }

    pub fn get_reward_block_hash(&self, reward_txn_id: &str) -> Result<Option<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT reward_block_hash FROM reward_block_details WHERE reward_txn_id = ?1")?;
        let result = stmt
            .query_row([reward_txn_id], |row| row.get(0))
            .optional()?;
        Ok(result)
    }

    pub fn add_payment(&self, wallet_address: &str, amount: u64, transaction_hash: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO payments (wallet_address, amount, timestamp, transaction_hash)
             VALUES (?1, ?2, ?3, ?4)",
            params![wallet_address, amount as i64, Utc::now().to_rfc3339(), transaction_hash],
        )?;
        Ok(())
    }

    pub fn get_payments_by_wallet(&self, wallet: &str) -> Result<Vec<(i64, String, u64, String, String)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, wallet_address, amount, timestamp, transaction_hash
             FROM payments WHERE wallet_address = ?1 ORDER BY timestamp DESC",
        )?;
        let rows = stmt.query_map([wallet], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })?;
        let mut payments = Vec::new();
        for row in rows {
            let (id, wallet_address, amount, timestamp, transaction_hash): (i64, String, i64, String, String) = row?;
            payments.push((id, wallet_address, amount as u64, timestamp, transaction_hash));
        }
        Ok(payments)
    }

    pub fn prune_old_shares(&self, hours: i64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM shares WHERE timestamp < ?1",
            [Utc::now().timestamp() - hours * 3600],
        )?;
        Ok(())
    }

    pub fn get_share_sums_in_window(&self, n: usize) -> Result<DashMap<String, u64>> {
        let conn = self.conn.lock().unwrap();
        let sums = DashMap::new();
        let mut stmt = conn.prepare(
            "SELECT address, SUM(difficulty) FROM (SELECT * FROM shares ORDER BY id DESC LIMIT ?1) GROUP BY address",
        )?;
        let rows = stmt.query_map([n as i32], |row| Ok((row.get(0)?, row.get(1)?)))?;
        for row in rows {
            let (addr, sum): (String, i64) = row?;
            sums.insert(addr, sum as u64);
        }
        Ok(sums)
    }

    pub fn get_total_shares_in_window(&self, n: usize) -> Result<u64> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT SUM(difficulty) FROM (SELECT * FROM shares ORDER BY id DESC LIMIT ?1)",
        )?;
        let total: Option<i64> = stmt.query_row([n as i32], |row| row.get(0)).optional()?;
        Ok(total.unwrap_or(0) as u64)
    }

    pub fn get_total_submissions_all(&self) -> Result<DashMap<String, u64>> {
        let conn = self.conn.lock().unwrap();
        let sums = DashMap::new();
        let mut stmt = conn.prepare("SELECT address, COUNT(*) FROM shares GROUP BY address")?;
        let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
        for row in rows {
            let (addr, count): (String, i64) = row?;
            sums.insert(addr, count as u64);
        }
        Ok(sums)
    }

    pub fn get_total_submissions(&self, addr: &str) -> Result<u64> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT COUNT(*) FROM shares WHERE address = ?1")?;
        let count: i64 = stmt.query_row([addr], |row| row.get(0)).optional()?.unwrap_or(0);
        Ok(count as u64)
    }

    pub fn get_miner_hashrates_in_window(&self, hours: i64) -> Result<DashMap<String, f64>> {
        let conn = self.conn.lock().unwrap();
        let hashrates = DashMap::new();
        let mut stmt = conn.prepare(
            "SELECT address, difficulty, timestamp FROM shares 
             WHERE timestamp >= ?1 
             ORDER BY address, timestamp",
        )?;
        let rows = stmt.query_map([Utc::now().timestamp() - hours * 3600], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;

        let mut miner_shares: HashMap<String, Vec<(u64, i64)>> = HashMap::new();
        for row in rows {
            let (addr, diff, ts): (String, i64, i64) = row?;
            miner_shares.entry(addr).or_insert(Vec::new()).push((diff as u64, ts));
        }

        for (addr, shares) in miner_shares {
            if shares.len() < 2 {
                continue;
            }
            let mut total_hashes = 0u64;
            let mut total_time_secs = 0.0;
            for window in shares.windows(2) {
                let (diff, ts1) = window[0];
                let (_, ts2) = window[1];
                let time_diff = (ts2 - ts1) as f64;
                if time_diff > 0.0 {
                    total_hashes += diff;
                    total_time_secs += time_diff;
                }
            }
            if total_time_secs > 0.0 {
                let hashrate = (total_hashes as f64 * 4_294_967_296.0) / total_time_secs;
                hashrates.insert(addr, hashrate);
            }
        }

        Ok(hashrates)
    }

    pub fn get_share_timestamp(&self, addr: &str, diff: u64) -> Result<DateTime<Utc>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT timestamp FROM shares WHERE address = ?1 AND difficulty = ?2 LIMIT 1",
        )?;
        let ts: i64 = stmt.query_row(params![addr, diff as i64], |row| row.get(0))?;
        Ok(DateTime::from_timestamp(ts, 0)
            .ok_or_else(|| anyhow::anyhow!("Invalid timestamp: {}", ts))?)
    }

    pub fn get_all_balances(&self) -> Result<Vec<(String, String, u64)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT id, address, balance FROM balances")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;
        let mut balances = Vec::new();
        for row in rows {
            let (id, address, balance): (String, String, i64) = row?;
            let miner_id = id.split('_').next().unwrap_or("unknown").to_string();
            balances.push((miner_id, address, balance as u64));
        }
        Ok(balances)
    }

    pub fn get_user(&self, miner_id: &str, wallet: &str) -> Result<u64> {
        let conn = self.conn.lock().unwrap();
        let id = format!("{}_{}", miner_id, wallet);
        let mut stmt = conn.prepare("SELECT balance FROM balances WHERE id = ?1")?;
        let balance: Option<i64> = stmt
            .query_row([id], |row| row.get(0))
            .optional()?;
        Ok(balance.unwrap_or(0) as u64)
    }
}