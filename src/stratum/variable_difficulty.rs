// src/stratum/variable_difficulty.rs

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, Duration};
use dashmap::DashMap;
use tokio::time::{sleep, Duration as TokioDuration};
use log::{info, debug, warn};
use crate::stratum::worker_stats::WorkerStats;
use crate::config::DifficultyConfig;

pub struct VariableDifficulty {
    config: DifficultyConfig,
    worker_stats: Arc<DashMap<String, Arc<tokio::sync::RwLock<WorkerStats>>>>,
    expected_share_rate: f64,
    current_network_diff: Arc<AtomicU64>,
}

impl VariableDifficulty {
    pub fn new(config: DifficultyConfig, current_network_diff: Arc<AtomicU64>) -> Self {
        let expected = config.target_shares_per_min;
        Self {
            config,
            worker_stats: Arc::new(DashMap::new()),
            expected_share_rate: expected,
            current_network_diff,
        }
    }

    pub fn get_or_create_stats(&self, address: &str, worker_name: &str, initial_diff: u64) -> Arc<tokio::sync::RwLock<WorkerStats>> {
        let key = format!("{}:{}", address, worker_name);
        if let Some(stats) = self.worker_stats.get(&key) {
            return stats.value().clone();
        }

        let stats = Arc::new(tokio::sync::RwLock::new(WorkerStats {
            address: address.to_owned(),
            worker_name: worker_name.to_owned(),
            shares_found: 0,
            invalid_shares: 0,
            var_diff_start_time: None,
            var_diff_shares_found: 0,
            var_diff_window: 0,
            current_diff: initial_diff,
            last_share_time: Instant::now(),
            recent_shares: Vec::new(),
            var_diff_enabled: true,
            last_adjust_time: None,
        }));

        self.worker_stats.insert(key, stats.clone());
        stats
    }

    /// Safely updates worker difficulty
    /// - Maximum increase capped at ×2.0 per adjustment
    /// - Gradual 10% drops if pool diff >> network diff
    /// - Never below network difficulty
    async fn update_worker_difficulty(&self, stats: &mut WorkerStats, proposed_diff: f64) {
        let current = stats.current_diff as f64;
        let mut target = proposed_diff;

        let network_diff_u64 = self.current_network_diff.load(Ordering::Relaxed);
        let network_diff = network_diff_u64 as f64;

        let min_allowed = network_diff.max(self.config.min as f64);

        // Gradual 5% drop if pool difficulty is significantly above network
        if current > network_diff * 1.3 {
            target = (current * 0.95).max(min_allowed);
            warn!(
                "VarDiff GRADUAL DROP → {} | pool too high vs net_diff | reducing 10% ({} → {})",
                stats.worker_name, current as u64, target as u64
            );
        }
        // Upward adjustment — capped at ×2.0 maximum
        else if target > current {
            // Hard cap: never more than double in one step
            target = target.min(current * 2.0);

            // Additional large step limit for safety
            let max_step = self.config.max as f64 * 6.0;
            let step = target - current;
            if step > max_step {
                target = current + max_step;
            }
        }

        // Final floor
        target = target.max(min_allowed);

        let clamped = target.clamp(self.config.min as f64, self.config.max as f64) as u64;

        if clamped != stats.current_diff {
            let multiplier = clamped as f64 / current.max(1.0) as f64;
            info!(
                "VarDiff UPDATE → {} | {} → {} (×{:.2}) | net_diff={} (window reset)",
                stats.worker_name,
                stats.current_diff,
                clamped,
                multiplier,
                network_diff_u64
            );

            stats.current_diff = clamped;
            stats.var_diff_start_time = None;
            stats.var_diff_window = 0;
            stats.var_diff_shares_found = 0;
            stats.last_adjust_time = Some(Instant::now());
        }
    }

    pub async fn on_valid_share(&self, stats_lock: Arc<tokio::sync::RwLock<WorkerStats>>, _difficulty: u64) {
        let mut stats = stats_lock.write().await;
        let now = Instant::now();

        let current_diff = stats.current_diff;

        stats.shares_found += 1;
        stats.last_share_time = now;
        stats.recent_shares.push((now, current_diff));
        stats.recent_shares.retain(|(t, _)| now.duration_since(*t) <= Duration::from_secs(3600));

        if stats.var_diff_start_time.is_none() {
            stats.var_diff_start_time = Some(now);
        }
        stats.var_diff_shares_found += 1;
    }

    pub async fn on_invalid_share(&self, stats_lock: Arc<tokio::sync::RwLock<WorkerStats>>) {
        let mut stats = stats_lock.write().await;
        stats.invalid_shares += 1;
    }

    pub fn start_thread(self: Arc<Self>) {
        let worker_stats = self.worker_stats.clone();
        let config = self.config.clone();
        let expected_share_rate = self.expected_share_rate;
        let current_network_diff = self.current_network_diff.clone();

        tokio::spawn(async move {
            loop {
                sleep(TokioDuration::from_secs(5)).await;

                for entry in worker_stats.iter() {
                    let stats_lock = entry.value().clone();
                    let mut stats = stats_lock.write().await;

                    if !stats.var_diff_enabled || stats.var_diff_start_time.is_none() {
                        continue;
                    }

                    if let Some(last_adj) = stats.last_adjust_time {
                        if Instant::now().duration_since(last_adj) < Duration::from_secs(30) {
                            continue;
                        }
                    }

                    let start_time = stats.var_diff_start_time.unwrap();
                    let duration_mins = start_time.elapsed().as_secs_f64() / 60.0;
                    if duration_mins < 0.1 {
                        continue;
                    }

                    let share_rate = stats.var_diff_shares_found as f64 / duration_mins;
                    let ratio = share_rate / expected_share_rate;

                    // Strong increase if submitting extremely fast — but capped in update_worker_difficulty
                    if ratio > 3.0 {
                        let target_diff = stats.current_diff as f64 * ratio.clamp(1.0, 2.5);
                        info!(
                            "VarDiff RAPID INCREASE → {} | rate={:.1}/min (>{:.0}× target) | proposing strong jump",
                            stats.worker_name, share_rate, ratio
                        );
                        self.update_worker_difficulty(&mut stats, target_diff).await;
                        continue;
                    }

                    // Fast drop on inactivity
                    let time_since_last_share = Instant::now().duration_since(stats.last_share_time).as_secs_f64();
                    if time_since_last_share > 120.0 && share_rate < expected_share_rate * 0.3 {
                        let network_diff = current_network_diff.load(Ordering::Relaxed) as f64;
                        let target_diff = (network_diff * 1.5).max(config.min as f64);
                        info!(
                            "VarDiff FAST DROP → {} | no_share={:.0}s | rate={:.1}/min | forcing ~1.5×net_diff",
                            stats.worker_name, time_since_last_share, share_rate
                        );
                        self.update_worker_difficulty(&mut stats, target_diff).await;
                        continue;
                    }

                    let window_idx = stats.var_diff_window.min(config.windows_mins.len() - 1);
                    let window_mins = config.windows_mins[window_idx];
                    let tolerance = config.tolerances[window_idx];

                    if window_mins == 0 {
                        if (1.0 - ratio).abs() >= tolerance {
                            let target_diff = stats.current_diff as f64 * ratio;
                            self.update_worker_difficulty(&mut stats, target_diff).await;
                        }
                        continue;
                    }

                    let mut breached = false;
                    for i in 1..=window_idx {
                        if (1.0 - ratio).abs() >= config.tolerances[i] {
                            breached = true;
                            break;
                        }
                    }
                    if breached {
                        let target_diff = stats.current_diff as f64 * ratio;
                        self.update_worker_difficulty(&mut stats, target_diff).await;
                        continue;
                    }

                    if stats.var_diff_shares_found as f64 >= window_mins as f64 * expected_share_rate * (1.0 + tolerance) {
                        let target_diff = stats.current_diff as f64 * ratio;
                        self.update_worker_difficulty(&mut stats, target_diff).await;
                        continue;
                    }

                    if duration_mins >= window_mins as f64 {
                        if stats.var_diff_shares_found as f64 <= window_mins as f64 * expected_share_rate * (1.0 - tolerance) {
                            let target_diff = stats.current_diff as f64 * ratio.max(0.1);
                            self.update_worker_difficulty(&mut stats, target_diff).await;
                        } else {
                            stats.var_diff_window += 1;
                            let next_window = config.windows_mins.get(stats.var_diff_window).copied().unwrap_or(0);
                            debug!("VarDiff ADVANCED → {} to window {}m", stats.worker_name, next_window);

                            stats.var_diff_shares_found = 0;
                            stats.var_diff_start_time = Some(Instant::now());
                        }
                    }
                }
            }
        });
    }
}