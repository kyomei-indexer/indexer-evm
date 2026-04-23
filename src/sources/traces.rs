use super::TraceSource;
use crate::config::RetryConfig;
use crate::sync::retry::backoff_delay;
use crate::types::{BlockInfo, BlockRange, BlockWithTraces, CallTrace};
use anyhow::{Context, Result};
use async_trait::async_trait;
use std::collections::HashSet;
use tracing::{debug, trace, warn};

/// RPC-based trace source supporting both `debug_traceBlockByNumber` and `trace_block` methods.
pub struct RpcTraceSource {
    client: reqwest::Client,
    rpc_url: String,
    method: TraceMethod,
    tracer: String,
    retry_config: RetryConfig,
    chain_id: u32,
}

/// The RPC method to use for fetching traces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceMethod {
    /// `debug_traceBlockByNumber` with callTracer — returns nested call tree
    Debug,
    /// `trace_block` (Parity/OpenEthereum style) — returns flat trace array
    Parity,
}

impl TraceMethod {
    pub fn from_str(s: &str) -> Result<Self> {
        match s {
            "debug" => Ok(Self::Debug),
            "parity" => Ok(Self::Parity),
            _ => anyhow::bail!("Unknown trace method '{}', expected 'debug' or 'parity'", s),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Parity => "parity",
        }
    }
}

impl RpcTraceSource {
    pub fn new(
        rpc_url: &str,
        chain_id: u32,
        method: TraceMethod,
        tracer: &str,
        retry_config: RetryConfig,
    ) -> Self {
        Self {
            client: reqwest::Client::new(),
            rpc_url: rpc_url.to_string(),
            method,
            tracer: tracer.to_string(),
            retry_config,
            chain_id,
        }
    }

    /// Fetch traces for a single block with retry logic.
    async fn fetch_block_traces_with_retry(
        &self,
        block_number: u64,
    ) -> Result<Vec<CallTrace>> {
        let mut last_error = None;

        for attempt in 0..=self.retry_config.max_retries {
            let start = std::time::Instant::now();
            match self.fetch_block_traces_once(block_number).await {
                Ok(traces) => {
                    crate::metrics::rpc_request(self.chain_id, "ok");
                    crate::metrics::rpc_latency(self.chain_id, start.elapsed().as_secs_f64());
                    if attempt > 0 {
                        debug!(
                            chain_id = self.chain_id,
                            block_number,
                            attempt,
                            trace_count = traces.len(),
                            "Trace fetch succeeded after retry"
                        );
                    }
                    return Ok(traces);
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
                            "Trace fetch failed, retrying"
                        );
                        tokio::time::sleep(delay).await;
                    }
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap())
    }

    /// Single attempt to fetch traces for a block.
    async fn fetch_block_traces_once(
        &self,
        block_number: u64,
    ) -> Result<Vec<CallTrace>> {
        let block_hex = format!("0x{:x}", block_number);

        match self.method {
            TraceMethod::Debug => self.fetch_debug_traces(&block_hex, block_number).await,
            TraceMethod::Parity => self.fetch_parity_traces(&block_hex, block_number).await,
        }
    }

    /// Fetch traces using `debug_traceBlockByNumber` with callTracer.
    async fn fetch_debug_traces(
        &self,
        block_hex: &str,
        block_number: u64,
    ) -> Result<Vec<CallTrace>> {
        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "debug_traceBlockByNumber",
            "params": [block_hex, { "tracer": &self.tracer }],
            "id": 1
        });

        let body: serde_json::Value = self.rpc_call(&payload).await?;
        let results = body
            .get("result")
            .and_then(|v| v.as_array())
            .context("Missing 'result' array in debug_traceBlockByNumber response")?;

        // We also need the block header for hash/timestamp
        let block_info = self.fetch_block_header(block_hex).await?;

        let mut traces = Vec::new();
        for (tx_index, tx_trace) in results.iter().enumerate() {
            let tx_hash = tx_trace
                .get("txHash")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            if let Some(result) = tx_trace.get("result") {
                flatten_debug_call(
                    result,
                    &mut traces,
                    block_number,
                    &block_info.hash,
                    block_info.timestamp,
                    &tx_hash,
                    tx_index as u32,
                    &mut vec![],
                );
            }
        }

        trace!(
            chain_id = self.chain_id,
            block_number,
            trace_count = traces.len(),
            "Fetched debug traces"
        );

        Ok(traces)
    }

    /// Fetch traces using `trace_block` (Parity-style).
    async fn fetch_parity_traces(
        &self,
        block_hex: &str,
        block_number: u64,
    ) -> Result<Vec<CallTrace>> {
        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "trace_block",
            "params": [block_hex],
            "id": 1
        });

        let body: serde_json::Value = self.rpc_call(&payload).await?;
        let results = body
            .get("result")
            .and_then(|v| v.as_array())
            .context("Missing 'result' array in trace_block response")?;

        // Parity traces include block info in each trace
        let block_info = self.fetch_block_header(block_hex).await?;

        let mut traces = Vec::new();
        for item in results {
            if let Some(trace) = parse_parity_trace(item, &block_info) {
                traces.push(trace);
            }
        }

        trace!(
            chain_id = self.chain_id,
            block_number,
            trace_count = traces.len(),
            "Fetched parity traces"
        );

        Ok(traces)
    }

    /// Fetch a block header for hash and timestamp.
    async fn fetch_block_header(&self, block_hex: &str) -> Result<BlockInfo> {
        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": [block_hex, false],
            "id": 1
        });

        let body: serde_json::Value = self.rpc_call(&payload).await?;
        let result = body
            .get("result")
            .context("Missing 'result' in eth_getBlockByNumber response")?;

        parse_block_info(result)
    }

    /// Send a JSON-RPC request and return the parsed response.
    async fn rpc_call(&self, payload: &serde_json::Value) -> Result<serde_json::Value> {
        let response = self
            .client
            .post(&self.rpc_url)
            .json(payload)
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

        Ok(body)
    }
}

#[async_trait]
impl TraceSource for RpcTraceSource {
    async fn get_block_traces(
        &self,
        range: BlockRange,
        addresses: &[String],
    ) -> Result<Vec<BlockWithTraces>> {
        let address_set: HashSet<String> = addresses
            .iter()
            .map(|a| a.to_lowercase())
            .collect();

        let mut blocks = Vec::new();

        for block_number in range.from..=range.to {
            let all_traces = self.fetch_block_traces_with_retry(block_number).await?;

            // Filter by target addresses if specified
            let filtered = if address_set.is_empty() {
                all_traces
            } else {
                all_traces
                    .into_iter()
                    .filter(|t| {
                        address_set.contains(&t.to_address.to_lowercase())
                            || address_set.contains(&t.from_address.to_lowercase())
                    })
                    .collect()
            };

            if !filtered.is_empty() {
                let block_info = BlockInfo {
                    number: block_number,
                    hash: filtered
                        .first()
                        .map(|t| t.block_hash.clone())
                        .unwrap_or_default(),
                    parent_hash: String::new(),
                    timestamp: filtered
                        .first()
                        .map(|t| t.block_timestamp)
                        .unwrap_or(0),
                };

                blocks.push(BlockWithTraces {
                    block: block_info,
                    traces: filtered,
                });
            }
        }

        Ok(blocks)
    }

    fn trace_method(&self) -> &str {
        self.method.as_str()
    }
}

/// Recursively flatten a nested debug callTracer result into a flat list of CallTrace.
fn flatten_debug_call(
    call: &serde_json::Value,
    out: &mut Vec<CallTrace>,
    block_number: u64,
    block_hash: &str,
    block_timestamp: u64,
    tx_hash: &str,
    tx_index: u32,
    path: &mut Vec<u32>,
) {
    let call_type = call
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("call")
        .to_lowercase();

    let from = call
        .get("from")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let to = call
        .get("to")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let input = call
        .get("input")
        .and_then(|v| v.as_str())
        .unwrap_or("0x")
        .to_string();

    let output = call
        .get("output")
        .and_then(|v| v.as_str())
        .unwrap_or("0x")
        .to_string();

    let value = parse_hex_u256(
        call.get("value")
            .and_then(|v| v.as_str())
            .unwrap_or("0x0"),
    );

    let gas = parse_hex_u64(
        call.get("gas")
            .and_then(|v| v.as_str())
            .unwrap_or("0x0"),
    );

    let gas_used = parse_hex_u64(
        call.get("gasUsed")
            .and_then(|v| v.as_str())
            .unwrap_or("0x0"),
    );

    let error = call
        .get("error")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    out.push(CallTrace {
        block_number,
        block_hash: block_hash.to_string(),
        block_timestamp,
        tx_hash: tx_hash.to_string(),
        tx_index,
        trace_address: path.clone(),
        call_type,
        from_address: from,
        to_address: to,
        input,
        output,
        value,
        gas,
        gas_used,
        error,
    });

    // Recurse into child calls
    if let Some(calls) = call.get("calls").and_then(|v| v.as_array()) {
        for (i, child) in calls.iter().enumerate() {
            path.push(i as u32);
            flatten_debug_call(
                child,
                out,
                block_number,
                block_hash,
                block_timestamp,
                tx_hash,
                tx_index,
                path,
            );
            path.pop();
        }
    }
}

/// Parse a single Parity-style trace into a CallTrace.
fn parse_parity_trace(item: &serde_json::Value, block_info: &BlockInfo) -> Option<CallTrace> {
    let action = item.get("action")?;

    let call_type = action
        .get("callType")
        .or_else(|| item.get("type"))
        .and_then(|v| v.as_str())
        .unwrap_or("call")
        .to_string();

    let from = action
        .get("from")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let to = action
        .get("to")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let input = action
        .get("input")
        .and_then(|v| v.as_str())
        .unwrap_or("0x")
        .to_string();

    let value = parse_hex_u256(
        action
            .get("value")
            .and_then(|v| v.as_str())
            .unwrap_or("0x0"),
    );

    let gas = parse_hex_u64(
        action
            .get("gas")
            .and_then(|v| v.as_str())
            .unwrap_or("0x0"),
    );

    // Result fields
    let result_obj = item.get("result");
    let output = result_obj
        .and_then(|r| r.get("output"))
        .and_then(|v| v.as_str())
        .unwrap_or("0x")
        .to_string();

    let gas_used = parse_hex_u64(
        result_obj
            .and_then(|r| r.get("gasUsed"))
            .and_then(|v| v.as_str())
            .unwrap_or("0x0"),
    );

    let error = item
        .get("error")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    // trace_address from the Parity format is already a flat array
    let trace_address: Vec<u32> = item
        .get("traceAddress")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_u64().map(|n| n as u32))
                .collect()
        })
        .unwrap_or_default();

    let tx_hash = item
        .get("transactionHash")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let tx_index = item
        .get("transactionPosition")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u32;

    Some(CallTrace {
        block_number: block_info.number,
        block_hash: block_info.hash.clone(),
        block_timestamp: block_info.timestamp,
        tx_hash,
        tx_index,
        trace_address,
        call_type,
        from_address: from,
        to_address: to,
        input,
        output,
        value,
        gas,
        gas_used,
        error,
    })
}

/// Parse a block JSON object into a BlockInfo.
pub fn parse_block_info(block: &serde_json::Value) -> Result<BlockInfo> {
    let number = parse_hex_u64(
        block
            .get("number")
            .and_then(|v| v.as_str())
            .context("Missing block number")?,
    );
    let hash = block
        .get("hash")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let parent_hash = block
        .get("parentHash")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let timestamp = parse_hex_u64(
        block
            .get("timestamp")
            .and_then(|v| v.as_str())
            .unwrap_or("0x0"),
    );

    Ok(BlockInfo {
        number,
        hash,
        parent_hash,
        timestamp,
    })
}

/// Parse a hex string (with or without 0x prefix) as u64.
pub fn parse_hex_u64(hex: &str) -> u64 {
    let clean = hex.strip_prefix("0x").unwrap_or(hex);
    u64::from_str_radix(clean, 16).unwrap_or(0)
}

/// Parse a hex string as a decimal string representation of a u256 value.
/// For simplicity we use u128 which covers most practical ETH values.
pub fn parse_hex_u256(hex: &str) -> String {
    let clean = hex.strip_prefix("0x").unwrap_or(hex);
    if clean.is_empty() || clean == "0" {
        return "0".to_string();
    }
    // Use u128 for values up to ~340 undecillion (covers all practical ETH values)
    match u128::from_str_radix(clean, 16) {
        Ok(val) => val.to_string(),
        Err(_) => {
            // Fall back to raw hex for extremely large values
            format!("0x{}", clean)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // === Hex parsing ===

    #[test]
    fn test_parse_hex_u64_with_prefix() {
        assert_eq!(parse_hex_u64("0xff"), 255);
        assert_eq!(parse_hex_u64("0x0"), 0);
        assert_eq!(parse_hex_u64("0x1"), 1);
        assert_eq!(parse_hex_u64("0x112a880"), 18000000);
    }

    #[test]
    fn test_parse_hex_u64_without_prefix() {
        assert_eq!(parse_hex_u64("ff"), 255);
        assert_eq!(parse_hex_u64("0"), 0);
    }

    #[test]
    fn test_parse_hex_u64_invalid() {
        assert_eq!(parse_hex_u64(""), 0);
        assert_eq!(parse_hex_u64("xyz"), 0);
    }

    #[test]
    fn test_parse_hex_u256_zero() {
        assert_eq!(parse_hex_u256("0x0"), "0");
        assert_eq!(parse_hex_u256("0x"), "0");
    }

    #[test]
    fn test_parse_hex_u256_one_eth() {
        // 1 ETH = 10^18 wei = 0xDE0B6B3A7640000
        assert_eq!(parse_hex_u256("0xDE0B6B3A7640000"), "1000000000000000000");
    }

    #[test]
    fn test_parse_hex_u256_small() {
        assert_eq!(parse_hex_u256("0x1"), "1");
        assert_eq!(parse_hex_u256("0xa"), "10");
    }

    // === TraceMethod ===

    #[test]
    fn test_trace_method_from_str() {
        assert_eq!(TraceMethod::from_str("debug").unwrap(), TraceMethod::Debug);
        assert_eq!(TraceMethod::from_str("parity").unwrap(), TraceMethod::Parity);
        assert!(TraceMethod::from_str("unknown").is_err());
    }

    #[test]
    fn test_trace_method_as_str() {
        assert_eq!(TraceMethod::Debug.as_str(), "debug");
        assert_eq!(TraceMethod::Parity.as_str(), "parity");
    }

    // === Debug trace flattening ===

    #[test]
    fn test_flatten_debug_call_simple() {
        let call = serde_json::json!({
            "type": "CALL",
            "from": "0xaaaa",
            "to": "0xbbbb",
            "input": "0x38ed1739",
            "output": "0x",
            "value": "0xde0b6b3a7640000",
            "gas": "0x5208",
            "gasUsed": "0x5208"
        });

        let mut traces = Vec::new();
        flatten_debug_call(
            &call,
            &mut traces,
            100,
            "0xblockhash",
            1700000000,
            "0xtxhash",
            0,
            &mut vec![],
        );

        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0].call_type, "call");
        assert_eq!(traces[0].from_address, "0xaaaa");
        assert_eq!(traces[0].to_address, "0xbbbb");
        assert_eq!(traces[0].value, "1000000000000000000");
        assert_eq!(traces[0].gas, 21000);
        assert!(traces[0].trace_address.is_empty());
    }

    #[test]
    fn test_flatten_debug_call_nested() {
        let call = serde_json::json!({
            "type": "CALL",
            "from": "0xaaaa",
            "to": "0xbbbb",
            "input": "0x",
            "output": "0x",
            "value": "0x0",
            "gas": "0x0",
            "gasUsed": "0x0",
            "calls": [
                {
                    "type": "DELEGATECALL",
                    "from": "0xbbbb",
                    "to": "0xcccc",
                    "input": "0x12345678",
                    "output": "0x",
                    "value": "0x0",
                    "gas": "0x0",
                    "gasUsed": "0x0",
                    "calls": [
                        {
                            "type": "STATICCALL",
                            "from": "0xcccc",
                            "to": "0xdddd",
                            "input": "0x",
                            "output": "0xresult",
                            "value": "0x0",
                            "gas": "0x0",
                            "gasUsed": "0x0"
                        }
                    ]
                }
            ]
        });

        let mut traces = Vec::new();
        flatten_debug_call(
            &call,
            &mut traces,
            100,
            "0xhash",
            1000,
            "0xtx",
            0,
            &mut vec![],
        );

        assert_eq!(traces.len(), 3);
        // Root call
        assert!(traces[0].trace_address.is_empty());
        assert_eq!(traces[0].call_type, "call");
        // First child
        assert_eq!(traces[1].trace_address, vec![0]);
        assert_eq!(traces[1].call_type, "delegatecall");
        // Grandchild
        assert_eq!(traces[2].trace_address, vec![0, 0]);
        assert_eq!(traces[2].call_type, "staticcall");
    }

    #[test]
    fn test_flatten_debug_call_with_error() {
        let call = serde_json::json!({
            "type": "CALL",
            "from": "0xaaaa",
            "to": "0xbbbb",
            "input": "0x",
            "output": "0x",
            "value": "0x0",
            "gas": "0x10000",
            "gasUsed": "0x10000",
            "error": "execution reverted"
        });

        let mut traces = Vec::new();
        flatten_debug_call(&call, &mut traces, 100, "0x", 0, "0x", 0, &mut vec![]);

        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0].error, Some("execution reverted".to_string()));
    }

    #[test]
    fn test_flatten_debug_call_multiple_siblings() {
        let call = serde_json::json!({
            "type": "CALL",
            "from": "0xa",
            "to": "0xb",
            "input": "0x",
            "output": "0x",
            "value": "0x0",
            "gas": "0x0",
            "gasUsed": "0x0",
            "calls": [
                {
                    "type": "CALL",
                    "from": "0xb",
                    "to": "0xc",
                    "input": "0x",
                    "output": "0x",
                    "value": "0x0",
                    "gas": "0x0",
                    "gasUsed": "0x0"
                },
                {
                    "type": "CALL",
                    "from": "0xb",
                    "to": "0xd",
                    "input": "0x",
                    "output": "0x",
                    "value": "0x0",
                    "gas": "0x0",
                    "gasUsed": "0x0"
                }
            ]
        });

        let mut traces = Vec::new();
        flatten_debug_call(&call, &mut traces, 100, "0x", 0, "0x", 0, &mut vec![]);

        assert_eq!(traces.len(), 3);
        assert_eq!(traces[1].trace_address, vec![0]);
        assert_eq!(traces[1].to_address, "0xc");
        assert_eq!(traces[2].trace_address, vec![1]);
        assert_eq!(traces[2].to_address, "0xd");
    }

    // === Parity trace parsing ===

    #[test]
    fn test_parse_parity_trace_call() {
        let block_info = BlockInfo {
            number: 100,
            hash: "0xblockhash".to_string(),
            parent_hash: "0xparent".to_string(),
            timestamp: 1700000000,
        };

        let item = serde_json::json!({
            "action": {
                "callType": "call",
                "from": "0xaaaa",
                "to": "0xbbbb",
                "input": "0x38ed1739",
                "value": "0xde0b6b3a7640000",
                "gas": "0x5208"
            },
            "result": {
                "output": "0xresult",
                "gasUsed": "0x5208"
            },
            "traceAddress": [0, 1],
            "transactionHash": "0xtxhash",
            "transactionPosition": 5
        });

        let trace = parse_parity_trace(&item, &block_info).unwrap();
        assert_eq!(trace.call_type, "call");
        assert_eq!(trace.from_address, "0xaaaa");
        assert_eq!(trace.to_address, "0xbbbb");
        assert_eq!(trace.value, "1000000000000000000");
        assert_eq!(trace.trace_address, vec![0, 1]);
        assert_eq!(trace.tx_hash, "0xtxhash");
        assert_eq!(trace.tx_index, 5);
        assert_eq!(trace.output, "0xresult");
    }

    #[test]
    fn test_parse_parity_trace_no_action() {
        let block_info = BlockInfo {
            number: 100,
            hash: "0x".to_string(),
            parent_hash: "0x".to_string(),
            timestamp: 0,
        };

        let item = serde_json::json!({});
        assert!(parse_parity_trace(&item, &block_info).is_none());
    }

    #[test]
    fn test_parse_parity_trace_with_error() {
        let block_info = BlockInfo {
            number: 100,
            hash: "0x".to_string(),
            parent_hash: "0x".to_string(),
            timestamp: 0,
        };

        let item = serde_json::json!({
            "action": {
                "callType": "call",
                "from": "0xa",
                "to": "0xb",
                "input": "0x",
                "value": "0x0",
                "gas": "0x0"
            },
            "error": "Out of gas",
            "traceAddress": [],
            "transactionHash": "0x1",
            "transactionPosition": 0
        });

        let trace = parse_parity_trace(&item, &block_info).unwrap();
        assert_eq!(trace.error, Some("Out of gas".to_string()));
    }

    // === Block info parsing ===

    #[test]
    fn test_parse_block_info() {
        let block = serde_json::json!({
            "number": "0x112a880",
            "hash": "0xabcdef1234567890",
            "parentHash": "0x1234567890abcdef",
            "timestamp": "0x6553f900"
        });

        let info = parse_block_info(&block).unwrap();
        assert_eq!(info.number, 18000000);
        assert_eq!(info.hash, "0xabcdef1234567890");
        assert_eq!(info.parent_hash, "0x1234567890abcdef");
        assert_eq!(info.timestamp, 0x6553f900);
    }

    #[test]
    fn test_parse_block_info_missing_number() {
        let block = serde_json::json!({
            "hash": "0x",
            "parentHash": "0x",
            "timestamp": "0x0"
        });
        assert!(parse_block_info(&block).is_err());
    }

    // === Address filtering ===

    #[test]
    fn test_address_filter_case_insensitive() {
        let addresses: HashSet<String> = vec!["0xaaaa".to_string()]
            .into_iter()
            .collect();

        let trace = CallTrace {
            block_number: 100,
            block_hash: "0x".to_string(),
            block_timestamp: 0,
            tx_hash: "0x".to_string(),
            tx_index: 0,
            trace_address: vec![],
            call_type: "call".to_string(),
            from_address: "0xother".to_string(),
            to_address: "0xAAAA".to_string(), // uppercase
            input: "0x".to_string(),
            output: "0x".to_string(),
            value: "0".to_string(),
            gas: 0,
            gas_used: 0,
            error: None,
        };

        // Should match after lowercasing
        assert!(addresses.contains(&trace.to_address.to_lowercase()));
    }
}
