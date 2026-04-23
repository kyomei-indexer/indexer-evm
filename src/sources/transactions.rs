use super::TransactionSource;
use crate::config::RetryConfig;
use crate::sources::traces::{parse_block_info, parse_hex_u64, parse_hex_u256};
use crate::sync::retry::backoff_delay;
use crate::types::{BlockRange, BlockWithTransactions, TransactionInfo};
use anyhow::{Context, Result};
use async_trait::async_trait;
use tracing::{debug, trace, warn};

/// RPC-based transaction source using `eth_getBlockByNumber` with full transactions.
pub struct RpcTransactionSource {
    client: reqwest::Client,
    rpc_url: String,
    retry_config: RetryConfig,
    chain_id: u32,
}

impl RpcTransactionSource {
    pub fn new(rpc_url: &str, chain_id: u32, retry_config: RetryConfig) -> Self {
        Self {
            client: reqwest::Client::new(),
            rpc_url: rpc_url.to_string(),
            retry_config,
            chain_id,
        }
    }

    /// Fetch a single block with full transactions, with retry.
    async fn fetch_block_with_retry(
        &self,
        block_number: u64,
    ) -> Result<BlockWithTransactions> {
        let mut last_error = None;

        for attempt in 0..=self.retry_config.max_retries {
            let start = std::time::Instant::now();
            match self.fetch_block_once(block_number).await {
                Ok(block) => {
                    crate::metrics::rpc_request(self.chain_id, "ok");
                    crate::metrics::rpc_latency(self.chain_id, start.elapsed().as_secs_f64());
                    if attempt > 0 {
                        debug!(
                            chain_id = self.chain_id,
                            block_number,
                            attempt,
                            tx_count = block.transactions.len(),
                            "Block transactions fetch succeeded after retry"
                        );
                    }
                    return Ok(block);
                }
                Err(e) => {
                    crate::metrics::rpc_request(self.chain_id, "error");
                    crate::metrics::rpc_latency(self.chain_id, start.elapsed().as_secs_f64());

                    if attempt < self.retry_config.max_retries {
                        let delay = backoff_delay(&self.retry_config, attempt);
                        warn!(
                            chain_id = self.chain_id,
                            block_number,
                            attempt = attempt + 1,
                            max_retries = self.retry_config.max_retries,
                            backoff_ms = delay.as_millis() as u64,
                            error = %e,
                            "Block transactions fetch failed, retrying"
                        );
                        tokio::time::sleep(delay).await;
                    }
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap())
    }

    /// Single attempt to fetch a block with full transactions.
    async fn fetch_block_once(
        &self,
        block_number: u64,
    ) -> Result<BlockWithTransactions> {
        let block_hex = format!("0x{:x}", block_number);

        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": [block_hex, true],
            "id": 1
        });

        let response = self
            .client
            .post(&self.rpc_url)
            .json(&payload)
            .send()
            .await
            .context("RPC request failed")?;

        let body: serde_json::Value = response
            .json()
            .await
            .context("Failed to parse RPC response JSON")?;

        if let Some(error) = body.get("error") {
            anyhow::bail!("RPC error: {}", error);
        }

        let result = body
            .get("result")
            .context("Missing 'result' in eth_getBlockByNumber response")?;

        let block_info = parse_block_info(result)?;

        let txs = result
            .get("transactions")
            .and_then(|v| v.as_array())
            .context("Missing 'transactions' array in block response")?;

        let transactions: Vec<TransactionInfo> = txs
            .iter()
            .filter_map(|tx| parse_transaction(tx, &block_info))
            .collect();

        trace!(
            chain_id = self.chain_id,
            block_number,
            tx_count = transactions.len(),
            "Fetched block transactions"
        );

        Ok(BlockWithTransactions {
            block: block_info,
            transactions,
        })
    }
}

#[async_trait]
impl TransactionSource for RpcTransactionSource {
    async fn get_block_transactions(
        &self,
        range: BlockRange,
    ) -> Result<Vec<BlockWithTransactions>> {
        let mut blocks = Vec::new();

        for block_number in range.from..=range.to {
            let block = self.fetch_block_with_retry(block_number).await?;
            blocks.push(block);
        }

        Ok(blocks)
    }
}

/// Parse a single transaction JSON object into TransactionInfo.
fn parse_transaction(tx: &serde_json::Value, block_info: &crate::types::BlockInfo) -> Option<TransactionInfo> {
    let tx_hash = tx.get("hash").and_then(|v| v.as_str())?.to_string();

    let from_address = tx
        .get("from")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let to_address = tx
        .get("to")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let value = parse_hex_u256(
        tx.get("value")
            .and_then(|v| v.as_str())
            .unwrap_or("0x0"),
    );

    let input = tx
        .get("input")
        .and_then(|v| v.as_str())
        .unwrap_or("0x")
        .to_string();

    let gas_price = parse_hex_u256(
        tx.get("gasPrice")
            .and_then(|v| v.as_str())
            .unwrap_or("0x0"),
    );

    let tx_index = parse_hex_u64(
        tx.get("transactionIndex")
            .and_then(|v| v.as_str())
            .unwrap_or("0x0"),
    ) as u32;

    let nonce = parse_hex_u64(
        tx.get("nonce")
            .and_then(|v| v.as_str())
            .unwrap_or("0x0"),
    );

    Some(TransactionInfo {
        block_number: block_info.number,
        block_hash: block_info.hash.clone(),
        block_timestamp: block_info.timestamp,
        tx_hash,
        tx_index,
        from_address,
        to_address,
        value,
        input,
        gas_price,
        gas_used: None,   // Not available in block tx data; needs receipt
        status: None,     // Not available in block tx data; needs receipt
        nonce,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::BlockInfo;

    fn test_block_info() -> BlockInfo {
        BlockInfo {
            number: 18000000,
            hash: "0xblockhash".to_string(),
            parent_hash: "0xparent".to_string(),
            timestamp: 1700000000,
        }
    }

    #[test]
    fn test_parse_transaction_basic() {
        let block = test_block_info();
        let tx = serde_json::json!({
            "hash": "0xtxhash",
            "from": "0xaaaa",
            "to": "0xbbbb",
            "value": "0xde0b6b3a7640000",
            "input": "0x38ed1739",
            "gasPrice": "0x4a817c800",
            "transactionIndex": "0x5",
            "nonce": "0x2a"
        });

        let info = parse_transaction(&tx, &block).unwrap();
        assert_eq!(info.tx_hash, "0xtxhash");
        assert_eq!(info.from_address, "0xaaaa");
        assert_eq!(info.to_address, Some("0xbbbb".to_string()));
        assert_eq!(info.value, "1000000000000000000");
        assert_eq!(info.input, "0x38ed1739");
        assert_eq!(info.tx_index, 5);
        assert_eq!(info.nonce, 42);
        assert_eq!(info.block_number, 18000000);
        assert_eq!(info.block_timestamp, 1700000000);
    }

    #[test]
    fn test_parse_transaction_contract_creation() {
        let block = test_block_info();
        let tx = serde_json::json!({
            "hash": "0xtxhash",
            "from": "0xaaaa",
            "to": null,
            "value": "0x0",
            "input": "0x6060604052",
            "gasPrice": "0x0",
            "transactionIndex": "0x0",
            "nonce": "0x0"
        });

        let info = parse_transaction(&tx, &block).unwrap();
        assert!(info.to_address.is_none());
        assert_eq!(info.value, "0");
    }

    #[test]
    fn test_parse_transaction_missing_hash() {
        let block = test_block_info();
        let tx = serde_json::json!({
            "from": "0xaaaa",
            "to": "0xbbbb",
            "value": "0x0"
        });

        // hash is required — should return None
        assert!(parse_transaction(&tx, &block).is_none());
    }

    #[test]
    fn test_parse_transaction_zero_value() {
        let block = test_block_info();
        let tx = serde_json::json!({
            "hash": "0x1",
            "from": "0xa",
            "to": "0xb",
            "value": "0x0",
            "input": "0x",
            "gasPrice": "0x0",
            "transactionIndex": "0x0",
            "nonce": "0x0"
        });

        let info = parse_transaction(&tx, &block).unwrap();
        assert_eq!(info.value, "0");
        assert_eq!(info.gas_price, "0");
    }

    #[test]
    fn test_parse_transaction_large_value() {
        let block = test_block_info();
        // 100 ETH
        let tx = serde_json::json!({
            "hash": "0x1",
            "from": "0xa",
            "to": "0xb",
            "value": "0x56bc75e2d63100000",
            "input": "0x",
            "gasPrice": "0x0",
            "transactionIndex": "0x0",
            "nonce": "0x0"
        });

        let info = parse_transaction(&tx, &block).unwrap();
        assert_eq!(info.value, "100000000000000000000");
    }

    #[test]
    fn test_parse_transaction_gas_used_and_status_not_available() {
        let block = test_block_info();
        let tx = serde_json::json!({
            "hash": "0x1",
            "from": "0xa",
            "to": "0xb",
            "value": "0x0",
            "input": "0x",
            "gasPrice": "0x0",
            "transactionIndex": "0x0",
            "nonce": "0x0"
        });

        let info = parse_transaction(&tx, &block).unwrap();
        // gas_used and status require receipt data, not available in block tx
        assert!(info.gas_used.is_none());
        assert!(info.status.is_none());
    }

    #[test]
    fn test_parse_transaction_preserves_block_info() {
        let block = BlockInfo {
            number: 42,
            hash: "0xspecial".to_string(),
            parent_hash: "0xparent".to_string(),
            timestamp: 9999,
        };
        let tx = serde_json::json!({
            "hash": "0x1",
            "from": "0xa",
            "to": "0xb",
            "value": "0x0",
            "input": "0x",
            "gasPrice": "0x0",
            "transactionIndex": "0x0",
            "nonce": "0x0"
        });

        let info = parse_transaction(&tx, &block).unwrap();
        assert_eq!(info.block_number, 42);
        assert_eq!(info.block_hash, "0xspecial");
        assert_eq!(info.block_timestamp, 9999);
    }
}
