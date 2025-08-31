use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::process::{Command, ChildStdout, ChildStderr};
use reqwest::Client;
use serde_json::json;
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

impl Transaction {
    pub fn new(from: &str, to: &str, amount: u64, network_id: &str) -> Self {
        Transaction {
            from: from.to_string(),
            to: to.to_string(),
            amount,
            network_id: network_id.to_string(),
        }
    }

    pub async fn to_js_value(&self) -> Result<serde_json::Value> {
        let client = Client::new();
        let retry_strategy = ExponentialBackoff::from_millis(100).take(3);
        let response = Retry::spawn(retry_strategy, || async {
            client
                .post("http://localhost:8181/createTransaction")
                .json(&json!({ "from": self.from, "to": self.to, "amount": self.amount, "network_id": self.network_id }))
                .send()
                .await
        })
        .await
        .context("Failed to call createTransaction")?;
        response
            .json::<serde_json::Value>()
            .await
            .context("Failed to parse createTransaction response")
            .map(|v| v["result"].clone())
    }

    pub async fn submit(&self) -> Result<String> {
        let tx = self.to_js_value().await?;
        let client = Client::new();
        let retry_strategy = ExponentialBackoff::from_millis(100).take(3);
        let response = Retry::spawn(retry_strategy, || async {
            client
                .post("http://localhost:8181/submitTransaction")
                .json(&json!({ "tx": tx }))
                .send()
                .await
        })
        .await
        .context("Failed to call submitTransaction")?;
        let result: serde_json::Value = response
            .json()
            .await
            .context("Failed to parse submitTransaction response")?;
        if result.get("error").is_some() {
            return Err(anyhow::anyhow!("Transaction submission failed: {:?}", result));
        }
        let tx_id: String = serde_json::from_value(result["result"].clone())
            .context("Failed to parse transaction ID")?;
        Ok(tx_id)
    }

    pub async fn format_amount(&self) -> Result<String> {
        let client = Client::new();
        let retry_strategy = ExponentialBackoff::from_millis(100).take(3);
        let response = Retry::spawn(retry_strategy, || async {
            client
                .post("http://localhost:8181/sompiToKaspaStringWithSuffix")
                .json(&json!({ "sompi": self.amount, "network_id": self.network_id }))
                .send()
                .await
        })
        .await
        .context("Failed to call sompiToKaspaStringWithSuffix")?;
        let result: serde_json::Value = response
            .json()
            .await
            .context("Failed to parse sompiToKaspaStringWithSuffix response")?;
        let formatted: String = serde_json::from_value(result["result"].clone())
            .context("Failed to parse formatted amount")?;
        Ok(formatted)
    }
}

pub async fn initialize_wasm() -> Result<()> {
    let retry_strategy = ExponentialBackoff::from_millis(100).max_delay(std::time::Duration::from_secs(3)).take(5);
    Retry::spawn(retry_strategy, || async {
        // Start the Node.js script with stderr and stdout capture
        let mut child = Command::new("node")
            .arg("src/wasm/run_vecno.js")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("Failed to start Node.js process")?;

        // Capture stdout and stderr
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

        // Wait to ensure the server starts
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        // Check if the process is still running
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

        // Verify server is responding
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