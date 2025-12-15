// src/config.rs

use log::info;
use std::env;

#[derive(Debug, Clone)]
pub struct DifficultyConfig {
    pub min: u64,
    pub default: u64,
    pub max: u64,
    pub enabled: bool,
    pub scale_factor: u64,

    // Staged VarDiff (progressive windows system)
    pub target_shares_per_min: f64,      // Desired average share submission rate
    pub windows_mins: Vec<u64>,          // Time windows in minutes for progressive evaluation
    pub tolerances: Vec<f64>,            // Allowed deviation per window (looser early, tighter later)
    pub high_rejection_threshold: f64,   // % of invalid shares that triggers protective low-diff fallback
}

impl DifficultyConfig {
    pub fn load() -> Self {
        let min = env::var("POOL_MIN_DIFFICULTY")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(50_000); // 50000

        let default = env::var("POOL_DEFAULT_DIFFICULTY")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(500_000) // 500000
            .max(min);

        let max = env::var("POOL_MAX_DIFFICULTY")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2_000_000_000) // 2000000000
            .max(default);

        let enabled = env::var("VARDIFF_ENABLED")
            .ok()
            .and_then(|s| s.to_lowercase().parse().ok())
            .unwrap_or(true);

        let scale_factor = env::var("VARDIFF_SCALE_FACTOR")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(200); // 200

        let target_shares_per_min = env::var("VARDIFF_TARGET_SHARES_PER_MIN")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30.0); // 30 (~1 share every 2 seconds)

        let windows_str = env::var("VARDIFF_WINDOWS")
            .unwrap_or("1,3,10,30,60,180,0".to_string());
        let windows_mins: Vec<u64> = windows_str
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();

        let tolerances_str = env::var("VARDIFF_TOLERANCES")
            .unwrap_or("1.5,1.2,0.8,0.6,0.4,0.3,0.2".to_string());
        let tolerances: Vec<f64> = tolerances_str
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();

        let high_rejection_threshold = env::var("VARDIFF_HIGH_REJECTION_THRESHOLD")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(25.0);

        let config = Self {
            min,
            default,
            max,
            enabled,
            scale_factor,
            target_shares_per_min,
            windows_mins,
            tolerances,
            high_rejection_threshold,
        };

        info!(
            "VarDiff {} | Range: {} → {} | Default: {} | Target: {} shares/min | Windows: {:?} | Tolerances: {:?} | Scale: {} | RejectionThreshold: {}%",
            if config.enabled { "ENABLED" } else { "DISABLED" },
            config.min,
            config.max,
            config.default,
            config.target_shares_per_min,
            config.windows_mins,
            config.tolerances,
            config.scale_factor,
            config.high_rejection_threshold
        );

        config
    }
}