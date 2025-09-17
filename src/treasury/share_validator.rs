// src/treasury/share_validator.rs
use anyhow::Result;
use crate::stratum::jobs::Jobs;
use crate::treasury::sharehandler::Contribution;
use log::{debug, warn};

pub async fn validate_share(
    contribution: &Contribution,
    jobs: &Jobs,
    nonce: &str,
) -> Result<bool> {
    // Get the job to retrieve the block template
    let job_id_num = match contribution.job_id.parse::<u8>() {
        Ok(id) => id,
        Err(e) => {
            warn!(
                "Invalid job_id={} for address={}: {}",
                contribution.job_id, contribution.address, e
            );
            return Ok(false);
        }
    };

    let job = jobs.get_job(job_id_num).await;
    if job.is_none() {
        warn!(
            "Stale share detected for job_id={} address={}",
            contribution.job_id, contribution.address
        );
        return Ok(false);
    }

    let block = job.unwrap();
    let block_hash = match block.header.as_ref() {
        Some(header) => {
            let pow_hash = header.hash(false)?;
            hex::encode(pow_hash.as_bytes())
        }
        None => {
            warn!(
                "No header found for job_id={} address={}",
                contribution.job_id, contribution.address
            );
            return Ok(false);
        }
    };

    debug!(
        "Share validated for job_id={} block_hash={} nonce={} address={}",
        contribution.job_id, block_hash, nonce, contribution.address
    );
    Ok(true)
}