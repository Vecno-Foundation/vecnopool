//src/stratum/worker_stats.rs

use std::time::Instant;

#[derive(Debug)]
pub struct WorkerStats {
    #[allow(dead_code)]
    pub address: String,
    pub worker_name: String,
    pub shares_found: u64,
    pub invalid_shares: u64,
    pub var_diff_start_time: Option<Instant>,
    pub var_diff_shares_found: u64,
    pub var_diff_window: usize,
    pub current_diff: u64,
    pub last_share_time: Instant,
    pub recent_shares: Vec<(Instant, u64)>,
    pub var_diff_enabled: bool,
    pub last_adjust_time: Option<Instant>,
}