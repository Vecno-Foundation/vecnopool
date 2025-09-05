// src/treasury/share_validator.rs
use anyhow::Result;
use crate::database::db::Db;
use crate::stratum::jobs::Jobs;
use crate::treasury::sharehandler::Contribution;
use log::{debug, warn};
use std::sync::Arc;

pub async fn validate_share(contribution: &Contribution, jobs: &Jobs, db: Arc<Db>, extranonce: &str, nonce: &str) -> Result<bool> {
    let is_duplicate = {
        let count = db.check_duplicate_share(&contribution.job_id, &contribution.address, extranonce, nonce).await?;
        count > 0
    };

    if is_duplicate {
        warn!("Duplicate share detected for job_id={} address={} extranonce={} nonce={}", contribution.job_id, contribution.address, extranonce, nonce);
        return Ok(false);
    }

    let job_id_num = match contribution.job_id.parse::<u8>() {
        Ok(id) => id,
        Err(e) => {
            warn!("Invalid job_id={} for address={}: {}", contribution.job_id, contribution.address, e);
            return Ok(false);
        }
    };
    if jobs.get_job(job_id_num).await.is_none() {
        warn!("Stale share detected for job_id={} address={}", contribution.job_id, contribution.address);
        return Ok(false);
    }

    debug!("Share validated for job_id={} address={} extranonce={} nonce={}", contribution.job_id, contribution.address, extranonce, nonce);
    Ok(true)
}