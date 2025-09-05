use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::process::Command;
use reqwest::Client;
use tokio_retry::{Retry, strategy::ExponentialBackoff};
use tokio::io::{AsyncReadExt, BufReader};
use std::process::Stdio;

#[derive(Clone, Serialize, Deserialize)]
pub struct Transaction {
    from: String,
    to: String,
    amount: u64,
    network_id: String,
}

pub async fn initialize_wasm() -> Result<()> {
    let retry_strategy = ExponentialBackoff::from_millis(100).max_delay(std::time::Duration::from_secs(3)).take(5);
    Retry::spawn(retry_strategy, || async {
        let mut child = Command::new("node")
            .arg("src/wasm/run_vecno.js")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("Failed to start Node.js process")?;

        let stdout = child.stdout.take().ok_or_else(|| anyhow::anyhow!("Failed to capture Node.js stdout"))?;
        let stderr = child.stderr.take().ok_or_else(|| anyhow::anyhow!("Failed to capture Node.js stderr"))?;
        let mut stdout_reader = BufReader::new(stdout);
        let mut stderr_reader = BufReader::new(stderr);
        let mut stdout_output = String::new();
        let mut stderr_output = String::new();

        let stdout_handle = tokio::spawn(async move {
            stdout_reader.read_to_string(&mut stdout_output).await.unwrap_or(0);
            stdout_output
        });
        let stderr_handle = tokio::spawn(async move {
            stderr_reader.read_to_string(&mut stderr_output).await.unwrap_or(0);
            stderr_output
        });

        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        if let Some(status) = child.try_wait().context("Failed to check Node.js process status")? {
            let stdout = stdout_handle.await.context("Failed to read stdout")?;
            let stderr = stderr_handle.await.context("Failed to read stderr")?;
            return Err(anyhow::anyhow!(
                "Node.js process exited with status {}: stdout: '{}', stderr: '{}'",
                status,
                stdout,
                stderr
            ));
        }

        let client = Client::new();
        let response = client
            .get("http://localhost:8181/ping")
            .send()
            .await
            .context("Failed to ping Node.js server")?;
        if !response.status().is_success() {
            let stdout = stdout_handle.await.context("Failed to read stdout")?;
            let stderr = stderr_handle.await.context("Failed to read stderr")?;
            return Err(anyhow::anyhow!(
                "Node.js server responded with status {}: stdout: '{}', stderr: '{}'",
                response.status(),
                stdout,
                stderr
            ));
        }

        Ok(())
    })
    .await
    .context("Failed to initialize WASM module after retries")
}