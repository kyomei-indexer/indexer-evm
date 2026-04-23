use super::{AdaptiveRange, BlockSource};
use crate::config::RetryConfig;
use crate::sync::retry::{backoff_delay, classify_rpc_error, BlockValidationError, RpcErrorKind};
use crate::types::{BlockInfo, BlockRange, BlockWithLogs, LogEntry, LogFilter};
use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tracing::{debug, info, trace, warn};

/// HyperSync block source for fast historical indexing.
/// Uses the Envio HyperSync JSON API for bulk block/log retrieval.
/// Falls back to RPC for operations HyperSync doesn't support.
pub struct HyperSyncBlockSource {
    http: reqwest::Client,
    base_url: String,
    retry_config: RetryConfig,
    adaptive_range: AdaptiveRange,
    /// Fallback RPC source for get_block_hash (reorg detection)
    fallback: super::rpc::RpcBlockSource,
    chain_id: u32,
}

// --- HyperSync API request/response types ---

#[derive(Serialize)]
struct HyperSyncQuery<'a> {
    from_block: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    to_block: Option<u64>,
    logs: Vec<HyperSyncLogSelection<'a>>,
    field_selection: HyperSyncFieldSelection,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    include_all_blocks: bool,
}

#[derive(Serialize)]
struct HyperSyncLogSelection<'a> {
    #[serde(skip_serializing_if = "<[String]>::is_empty")]
    address: &'a [String],
    #[serde(skip_serializing_if = "Vec::is_empty")]
    topics: Vec<Vec<String>>,
}

#[derive(Serialize)]
struct HyperSyncFieldSelection {
    block: Vec<String>,
    log: Vec<String>,
}

#[derive(Deserialize)]
struct HyperSyncResponse {
    data: Vec<HyperSyncDataBatch>,
    #[serde(default)]
    archive_height: Option<u64>,
    #[serde(default)]
    next_block: Option<u64>,
}

#[derive(Deserialize)]
struct HyperSyncDataBatch {
    #[serde(default)]
    blocks: Vec<HyperSyncBlock>,
    #[serde(default)]
    logs: Vec<HyperSyncLog>,
}

#[derive(Deserialize)]
struct HyperSyncBlock {
    number: Option<u64>,
    hash: Option<String>,
    parent_hash: Option<String>,
    timestamp: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct HyperSyncLog {
    block_number: Option<u64>,
    block_hash: Option<String>,
    transaction_hash: Option<String>,
    transaction_index: Option<u64>,
    log_index: Option<u64>,
    address: Option<String>,
    topic0: Option<String>,
    topic1: Option<String>,
    topic2: Option<String>,
    topic3: Option<String>,
    data: Option<String>,
}

#[derive(Deserialize)]
struct HyperSyncHeightResponse {
    height: u64,
}

/// Resolve the fallback RPC URL for a given chain.
/// Returns an error for unknown chains to prevent silently using the wrong RPC
/// (which would cause corrupted data for reorg detection).
pub fn resolve_fallback_rpc(fallback_rpc: Option<&str>, chain_id: u32) -> Result<String> {
    match fallback_rpc {
        Some(url) => Ok(url.to_string()),
        None => match chain_id {
            1 => Ok("https://eth.llamarpc.com".to_string()),
            11155111 => Ok("https://rpc.sepolia.org".to_string()),
            137 => Ok("https://polygon.llamarpc.com".to_string()),
            42161 => Ok("https://arb1.arbitrum.io/rpc".to_string()),
            10 => Ok("https://mainnet.optimism.io".to_string()),
            8453 => Ok("https://mainnet.base.org".to_string()),
            56 => Ok("https://bsc-dataseed.binance.org".to_string()),
            43114 => Ok("https://api.avax.network/ext/bc/C/rpc".to_string()),
            100 => Ok("https://rpc.gnosischain.com".to_string()),
            250 => Ok("https://rpc.ftm.tools".to_string()),
            324 => Ok("https://mainnet.era.zksync.io".to_string()),
            59144 => Ok("https://rpc.linea.build".to_string()),
            534352 => Ok("https://rpc.scroll.io".to_string()),
            _ => anyhow::bail!(
                "No fallback RPC URL configured for chain_id {}. \
                 HyperSync requires a fallback RPC for block hash lookups (reorg detection). \
                 Please set `fallback_rpc` in the HyperSync source config.",
                chain_id
            ),
        },
    }
}

impl HyperSyncBlockSource {
    /// Create a new HyperSync block source.
    pub async fn new(
        url: Option<&str>,
        api_token: Option<&str>,
        fallback_rpc: Option<&str>,
        chain_id: u32,
        blocks_per_request: u64,
    ) -> Result<Self> {
        Self::with_retry_config(
            url,
            api_token,
            fallback_rpc,
            chain_id,
            blocks_per_request,
            RetryConfig::default(),
        )
        .await
    }

    /// Create a new HyperSync block source with custom retry configuration
    pub async fn with_retry_config(
        url: Option<&str>,
        api_token: Option<&str>,
        fallback_rpc: Option<&str>,
        chain_id: u32,
        blocks_per_request: u64,
        retry_config: RetryConfig,
    ) -> Result<Self> {
        let base_url = url
            .unwrap_or("https://eth.hypersync.xyz")
            .trim_end_matches('/')
            .to_string();

        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(token) = api_token {
            headers.insert(
                reqwest::header::AUTHORIZATION,
                reqwest::header::HeaderValue::from_str(&format!("Bearer {}", token))
                    .context("Invalid API token")?,
            );
        }

        let http = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .context("Failed to create HTTP client")?;

        debug!(
            chain_id,
            url = %base_url,
            has_api_token = api_token.is_some(),
            source = "hypersync",
            "Creating HyperSync source"
        );

        let rpc_url = resolve_fallback_rpc(fallback_rpc, chain_id)?;

        debug!(
            chain_id,
            fallback_rpc = %rpc_url,
            "Creating RPC fallback for HyperSync"
        );

        let fallback = super::rpc::RpcBlockSource::with_retry_config(
            &rpc_url,
            chain_id,
            blocks_per_request,
            retry_config.clone(),
        )
        .await?;

        Ok(Self {
            http,
            base_url,
            retry_config,
            adaptive_range: AdaptiveRange::new(blocks_per_request),
            fallback,
            chain_id,
        })
    }

    /// Execute a single HyperSync query attempt
    async fn query_once(&self, query: &HyperSyncQuery<'_>) -> Result<HyperSyncResponse> {
        let url = format!("{}/query", self.base_url);

        let resp = self
            .http
            .post(&url)
            .json(query)
            .send()
            .await
            .context("HyperSync request failed")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "HyperSync query failed with status {}: {}",
                status,
                body
            );
        }

        resp.json::<HyperSyncResponse>()
            .await
            .context("Failed to parse HyperSync response")
    }

    /// Execute a HyperSync query with retry and rate limit awareness.
    /// RangeTooBig errors are returned immediately (caller must rebuild the query
    /// with a smaller range — retrying the same payload is pointless).
    async fn query(&self, query: &HyperSyncQuery<'_>) -> Result<HyperSyncResponse> {
        let mut last_error = None;

        for attempt in 0..=self.retry_config.max_retries {
            match self.query_once(query).await {
                Ok(resp) => {
                    if attempt > 0 {
                        info!(
                            chain_id = self.chain_id,
                            attempt,
                            "HyperSync query succeeded after retry"
                        );
                    }
                    return Ok(resp);
                }
                Err(e) => {
                    let error_kind = classify_rpc_error(&e);

                    if matches!(error_kind, RpcErrorKind::RangeTooBig) {
                        return Err(e);
                    }

                    if attempt < self.retry_config.max_retries {
                        let delay = match error_kind {
                            RpcErrorKind::RateLimit => {
                                let base = backoff_delay(&self.retry_config, attempt);
                                base.max(std::time::Duration::from_secs(5))
                            }
                            _ => backoff_delay(&self.retry_config, attempt),
                        };
                        warn!(
                            chain_id = self.chain_id,
                            attempt = attempt + 1,
                            max_retries = self.retry_config.max_retries,
                            backoff_ms = delay.as_millis() as u64,
                            error_kind = ?error_kind,
                            error = %e,
                            "HyperSync query failed, retrying with backoff"
                        );
                        tokio::time::sleep(delay).await;
                    } else {
                        warn!(
                            chain_id = self.chain_id,
                            total_attempts = attempt + 1,
                            error = %e,
                            "HyperSync query failed after all retries"
                        );
                    }
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap())
    }

    /// Get the chain height from HyperSync (single attempt)
    async fn get_height_once(&self) -> Result<u64> {
        let url = format!("{}/height", self.base_url);

        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .context("HyperSync height request failed")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("HyperSync height failed with status {}: {}", status, body);
        }

        let height_resp: HyperSyncHeightResponse = resp
            .json()
            .await
            .context("Failed to parse HyperSync height response")?;

        Ok(height_resp.height)
    }

    /// Get the chain height from HyperSync with retry
    async fn get_height(&self) -> Result<u64> {
        let mut last_error = None;

        for attempt in 0..=self.retry_config.max_retries {
            match self.get_height_once().await {
                Ok(height) => {
                    if attempt > 0 {
                        debug!(
                            chain_id = self.chain_id,
                            attempt,
                            height,
                            "HyperSync get_height succeeded after retry"
                        );
                    }
                    return Ok(height);
                }
                Err(e) => {
                    if attempt < self.retry_config.max_retries {
                        let delay = backoff_delay(&self.retry_config, attempt);
                        warn!(
                            chain_id = self.chain_id,
                            attempt = attempt + 1,
                            max_retries = self.retry_config.max_retries,
                            backoff_ms = delay.as_millis() as u64,
                            error = %e,
                            "HyperSync get_height failed, retrying"
                        );
                        tokio::time::sleep(delay).await;
                    }
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap())
    }

    /// Query HyperSync for a single block's hash.
    /// Used for reorg detection before falling back to RPC.
    async fn query_block_hash(&self, block_number: u64) -> Result<Option<String>> {
        let query = HyperSyncQuery {
            from_block: block_number,
            to_block: Some(block_number),
            logs: vec![],
            field_selection: HyperSyncFieldSelection {
                block: vec!["number".into(), "hash".into()],
                log: vec![],
            },
            include_all_blocks: true,
        };

        let response = self.query_once(&query).await?;

        for batch in &response.data {
            for block in &batch.blocks {
                if block.number == Some(block_number) {
                    if let Some(hash) = &block.hash {
                        if !hash.is_empty() {
                            return Ok(Some(hash.clone()));
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    /// Parse a hex timestamp or numeric value to u64
    fn parse_timestamp(value: &serde_json::Value) -> u64 {
        match value {
            serde_json::Value::Number(n) => n.as_u64().unwrap_or(0),
            serde_json::Value::String(s) => {
                let s = s.trim_start_matches("0x");
                u64::from_str_radix(s, 16).unwrap_or(0)
            }
            _ => 0,
        }
    }

    /// Convert HyperSync response into our internal BlockWithLogs format.
    /// Validates log block_hash against block headers and drops out-of-range logs.
    fn convert_response(
        &self,
        response: HyperSyncResponse,
        range: &BlockRange,
    ) -> Vec<BlockWithLogs> {
        convert_hypersync_response(response, range, self.chain_id)
    }
}

/// Standalone conversion of HyperSync response to BlockWithLogs.
/// Extracted from HyperSyncBlockSource for testability.
fn convert_hypersync_response(
    response: HyperSyncResponse,
    range: &BlockRange,
    chain_id: u32,
) -> Vec<BlockWithLogs> {
    let mut block_map: BTreeMap<u64, BlockWithLogs> = BTreeMap::new();

    for batch in &response.data {

    // Process blocks
    for block in &batch.blocks {
        let number = block.number.unwrap_or(0);
        let hash = block.hash.clone().unwrap_or_default();
        let parent_hash = block.parent_hash.clone().unwrap_or_default();
        let timestamp = block
            .timestamp
            .as_ref()
            .map(HyperSyncBlockSource::parse_timestamp)
            .unwrap_or(0);

        block_map.insert(
            number,
            BlockWithLogs {
                block: BlockInfo {
                    number,
                    hash,
                    parent_hash,
                    timestamp,
                },
                logs: Vec::new(),
            },
        );
    }

    // Process logs with cross-validation
    for log in &batch.logs {
        let block_number = log.block_number.unwrap_or(0);

        // Drop logs with block_number outside the requested range
        if block_number < range.from || block_number > range.to {
            warn!(
                chain_id,
                log_block_number = block_number,
                range_from = range.from,
                range_to = range.to,
                "HyperSync returned log outside requested range, dropping"
            );
            continue;
        }

        let log_block_hash = log.block_hash.clone().unwrap_or_default();

        // Cross-validate log block_hash against fetched block header
        if !log_block_hash.is_empty() {
            if let Some(block_with_logs) = block_map.get(&block_number) {
                let header_hash = &block_with_logs.block.hash;
                if !header_hash.is_empty() && log_block_hash != *header_hash {
                    let err = BlockValidationError::LogBlockMismatch {
                        block_number,
                        log_block_hash: log_block_hash.clone(),
                        header_block_hash: header_hash.clone(),
                    };
                    warn!(
                        chain_id,
                        error = %err,
                        "HyperSync log block_hash mismatch, dropping log"
                    );
                    continue;
                }
            }
        }

        let entry = LogEntry {
            block_number,
            block_hash: log_block_hash,
            block_timestamp: block_map
                .get(&block_number)
                .map(|b| b.block.timestamp)
                .unwrap_or(0),
            transaction_hash: log.transaction_hash.clone().unwrap_or_default(),
            transaction_index: log.transaction_index.map(|i| i as u32).unwrap_or(0),
            log_index: log.log_index.map(|i| i as u32).unwrap_or(0),
            address: log.address.clone().unwrap_or_default(),
            topic0: log.topic0.clone(),
            topic1: log.topic1.clone(),
            topic2: log.topic2.clone(),
            topic3: log.topic3.clone(),
            data: log.data.clone().unwrap_or_else(|| "0x".to_string()),
        };

        if let Some(block_with_logs) = block_map.get_mut(&block_number) {
            block_with_logs.logs.push(entry);
        } else {
            block_map.insert(
                block_number,
                BlockWithLogs {
                    block: BlockInfo {
                        number: block_number,
                        hash: entry.block_hash.clone(),
                        parent_hash: String::new(),
                        timestamp: 0,
                    },
                    logs: vec![entry],
                },
            );
        }
    }

    } // end for batch

    block_map.into_values().collect()
}

#[async_trait]
impl BlockSource for HyperSyncBlockSource {
    async fn get_blocks(
        &self,
        range: BlockRange,
        filter: &LogFilter,
    ) -> Result<Vec<BlockWithLogs>> {
        let mut all_blocks: Vec<BlockWithLogs> = Vec::new();
        let mut current_from = range.from;

        while current_from <= range.to {
            let effective_range = self.adaptive_range.get();
            let page_to = std::cmp::min(current_from + effective_range - 1, range.to);

            let mut log_selection = HyperSyncLogSelection {
                address: &[],
                topics: Vec::new(),
            };

            if !filter.addresses.is_empty() {
                log_selection.address = &filter.addresses;
            }

            let query = HyperSyncQuery {
                from_block: current_from,
                to_block: Some(page_to),
                logs: vec![log_selection],
                field_selection: HyperSyncFieldSelection {
                    block: vec![
                        "number".into(),
                        "hash".into(),
                        "parent_hash".into(),
                        "timestamp".into(),
                    ],
                    log: vec![
                        "block_number".into(),
                        "block_hash".into(),
                        "transaction_hash".into(),
                        "transaction_index".into(),
                        "log_index".into(),
                        "address".into(),
                        "topic0".into(),
                        "topic1".into(),
                        "topic2".into(),
                        "topic3".into(),
                        "data".into(),
                    ],
                },
                include_all_blocks: false,
            };

            debug!(
                chain_id = self.chain_id,
                from = current_from,
                to = page_to,
                adaptive_range = effective_range,
                addresses = filter.addresses.len(),
                source = "hypersync",
                "Fetching block range via HyperSync"
            );

            match self.query(&query).await {
                Ok(response) => {
                    self.adaptive_range.grow();
                    let next_block = response.next_block;
                    let page_range = BlockRange { from: current_from, to: page_to };
                    let blocks = self.convert_response(response, &page_range);
                    all_blocks.extend(blocks);

                    match next_block {
                        Some(nb) if nb > current_from && nb <= page_to => {
                            debug!(
                                chain_id = self.chain_id,
                                next_block = nb,
                                page_to,
                                "HyperSync returned partial response, paginating within page"
                            );
                            current_from = nb;
                        }
                        _ => {
                            current_from = page_to + 1;
                        }
                    }
                }
                Err(e) => {
                    if classify_rpc_error(&e) == RpcErrorKind::RangeTooBig {
                        let old = self.adaptive_range.get();
                        self.adaptive_range.shrink();
                        let new = self.adaptive_range.get();
                        if new < old {
                            warn!(
                                chain_id = self.chain_id,
                                old_range = old,
                                new_range = new,
                                addresses = filter.addresses.len(),
                                "Payload too large, shrinking adaptive range and retrying"
                            );
                            continue;
                        }
                        // Already at minimum — can't shrink further
                        return Err(e.context(format!(
                            "HyperSync payload too large even at minimum range ({new}). \
                             Filter has {} addresses — consider reducing the batch size.",
                            filter.addresses.len()
                        )));
                    }
                    return Err(e);
                }
            }
        }

        trace!(
            chain_id = self.chain_id,
            from = range.from,
            to = range.to,
            blocks_returned = all_blocks.len(),
            total_logs = all_blocks.iter().map(|b| b.logs.len()).sum::<usize>(),
            "HyperSync block range fetch complete"
        );

        Ok(all_blocks)
    }

    async fn get_latest_block_number(&self) -> Result<u64> {
        match self.get_height().await {
            Ok(height) => Ok(height),
            Err(e) => {
                warn!(
                    chain_id = self.chain_id,
                    error = %e,
                    "HyperSync get_height failed, falling back to RPC"
                );
                self.fallback.get_latest_block_number().await
            }
        }
    }

    async fn get_block_hash(&self, block_number: u64) -> Result<Option<String>> {
        // Try HyperSync first: fetch the single block header
        match self.query_block_hash(block_number).await {
            Ok(Some(hash)) => return Ok(Some(hash)),
            Ok(None) => {
                debug!(
                    chain_id = self.chain_id,
                    block_number,
                    "HyperSync returned no block for hash lookup, falling back to RPC"
                );
            }
            Err(e) => {
                warn!(
                    chain_id = self.chain_id,
                    block_number,
                    error = %e,
                    "HyperSync block hash lookup failed, falling back to RPC"
                );
            }
        }
        self.fallback.get_block_hash(block_number).await
    }

    fn source_type(&self) -> &'static str {
        "hypersync"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::BlockRange;

    const TEST_CHAIN_ID: u32 = 1;

    fn convert(response: HyperSyncResponse, range: &BlockRange) -> Vec<BlockWithLogs> {
        convert_hypersync_response(response, range, TEST_CHAIN_ID)
    }

    fn make_block(number: u64, hash: &str, parent_hash: &str, timestamp: u64) -> HyperSyncBlock {
        HyperSyncBlock {
            number: Some(number),
            hash: Some(hash.to_string()),
            parent_hash: Some(parent_hash.to_string()),
            timestamp: Some(serde_json::Value::Number(serde_json::Number::from(timestamp))),
        }
    }

    fn make_log(block_number: u64, block_hash: &str, log_index: u64) -> HyperSyncLog {
        HyperSyncLog {
            block_number: Some(block_number),
            block_hash: Some(block_hash.to_string()),
            transaction_hash: Some(format!("0xtx{}", log_index)),
            transaction_index: Some(0),
            log_index: Some(log_index),
            address: Some("0xabc".to_string()),
            topic0: Some("0xtopic0".to_string()),
            topic1: None,
            topic2: None,
            topic3: None,
            data: Some("0x1234".to_string()),
        }
    }

    fn make_response(blocks: Vec<HyperSyncBlock>, logs: Vec<HyperSyncLog>) -> HyperSyncResponse {
        HyperSyncResponse {
            data: vec![HyperSyncDataBatch { blocks, logs }],
            archive_height: None,
            next_block: None,
        }
    }

    // --- convert_response: basic functionality ---

    #[test]
    fn test_convert_response_valid_logs_kept() {
        let response = make_response(
            vec![make_block(100, "0xhash100", "0xparent", 1000)],
            vec![make_log(100, "0xhash100", 0), make_log(100, "0xhash100", 1)],
        );
        let range = BlockRange { from: 100, to: 100 };
        let blocks = convert(response, &range);

        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].block.number, 100);
        assert_eq!(blocks[0].logs.len(), 2);
        assert_eq!(blocks[0].logs[0].log_index, 0);
        assert_eq!(blocks[0].logs[1].log_index, 1);
    }

    #[test]
    fn test_convert_response_sets_timestamp_from_block() {
        let response = make_response(
            vec![make_block(100, "0xhash", "0xparent", 1700000000)],
            vec![make_log(100, "0xhash", 0)],
        );
        let range = BlockRange { from: 100, to: 100 };
        let blocks = convert(response, &range);

        assert_eq!(blocks[0].logs[0].block_timestamp, 1700000000);
    }

    #[test]
    fn test_convert_response_empty_response() {
        let response = make_response(vec![], vec![]);
        let range = BlockRange { from: 100, to: 200 };
        let blocks = convert(response, &range);

        assert!(blocks.is_empty());
    }

    #[test]
    fn test_convert_response_blocks_without_logs() {
        let response = make_response(
            vec![
                make_block(100, "0xh1", "0xp0", 1000),
                make_block(101, "0xh2", "0xh1", 1001),
            ],
            vec![],
        );
        let range = BlockRange { from: 100, to: 101 };
        let blocks = convert(response, &range);

        assert_eq!(blocks.len(), 2);
        assert!(blocks[0].logs.is_empty());
        assert!(blocks[1].logs.is_empty());
    }

    // --- convert_response: out-of-range log filtering ---

    #[test]
    fn test_convert_response_drops_log_below_range() {
        let response = make_response(
            vec![make_block(100, "0xhash100", "0xparent", 1000)],
            vec![
                make_log(99, "0xother", 0),  // below range
                make_log(100, "0xhash100", 1), // in range
            ],
        );
        let range = BlockRange { from: 100, to: 200 };
        let blocks = convert(response, &range);

        // Block 99 shouldn't exist, block 100 should have 1 log
        let block100 = blocks.iter().find(|b| b.block.number == 100).unwrap();
        assert_eq!(block100.logs.len(), 1);
        assert_eq!(block100.logs[0].log_index, 1);
        // No block 99 should be in the result
        assert!(blocks.iter().all(|b| b.block.number != 99));
    }

    #[test]
    fn test_convert_response_drops_log_above_range() {
        let response = make_response(
            vec![make_block(100, "0xhash100", "0xparent", 1000)],
            vec![
                make_log(100, "0xhash100", 0), // in range
                make_log(201, "0xother", 1),    // above range
            ],
        );
        let range = BlockRange { from: 100, to: 200 };
        let blocks = convert(response, &range);

        let block100 = blocks.iter().find(|b| b.block.number == 100).unwrap();
        assert_eq!(block100.logs.len(), 1);
        assert!(blocks.iter().all(|b| b.block.number != 201));
    }

    #[test]
    fn test_convert_response_keeps_logs_at_range_boundaries() {
        let response = make_response(
            vec![
                make_block(100, "0xh100", "0xp", 1000),
                make_block(200, "0xh200", "0xp", 2000),
            ],
            vec![
                make_log(100, "0xh100", 0), // at from boundary
                make_log(200, "0xh200", 1), // at to boundary
            ],
        );
        let range = BlockRange { from: 100, to: 200 };
        let blocks = convert(response, &range);

        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].logs.len(), 1);
        assert_eq!(blocks[1].logs.len(), 1);
    }

    #[test]
    fn test_convert_response_all_logs_out_of_range() {
        let response = make_response(
            vec![make_block(100, "0xhash", "0xp", 1000)],
            vec![
                make_log(50, "0xa", 0),
                make_log(300, "0xb", 1),
            ],
        );
        let range = BlockRange { from: 100, to: 200 };
        let blocks = convert(response, &range);

        // Block 100 exists from the block header but should have 0 logs
        let block100 = blocks.iter().find(|b| b.block.number == 100).unwrap();
        assert!(block100.logs.is_empty());
    }

    // --- convert_response: block_hash cross-validation ---

    #[test]
    fn test_convert_response_drops_log_with_mismatched_block_hash() {
        let response = make_response(
            vec![make_block(100, "0xcorrect_hash", "0xparent", 1000)],
            vec![
                make_log(100, "0xcorrect_hash", 0), // matches
                make_log(100, "0xwrong_hash", 1),    // mismatch — should be dropped
            ],
        );
        let range = BlockRange { from: 100, to: 100 };
        let blocks = convert(response, &range);

        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].logs.len(), 1);
        assert_eq!(blocks[0].logs[0].log_index, 0);
    }

    #[test]
    fn test_convert_response_empty_log_hash_not_validated() {
        let response = make_response(
            vec![make_block(100, "0xcorrect", "0xparent", 1000)],
            vec![{
                // Log with empty block_hash — should be accepted (no validation)
                let mut log = make_log(100, "", 0);
                log.block_hash = Some(String::new());
                log
            }],
        );
        let range = BlockRange { from: 100, to: 100 };
        let blocks = convert(response, &range);

        assert_eq!(blocks[0].logs.len(), 1);
    }

    #[test]
    fn test_convert_response_empty_header_hash_not_validated() {
        let response = make_response(
            vec![make_block(100, "", "0xparent", 1000)], // empty header hash
            vec![make_log(100, "0xsome_hash", 0)],       // log has a hash
        );
        let range = BlockRange { from: 100, to: 100 };
        let blocks = convert(response, &range);

        // Should be kept because header hash is empty (can't validate)
        assert_eq!(blocks[0].logs.len(), 1);
    }

    #[test]
    fn test_convert_response_all_logs_hash_mismatch() {
        let response = make_response(
            vec![make_block(100, "0xreal", "0xparent", 1000)],
            vec![
                make_log(100, "0xfake1", 0),
                make_log(100, "0xfake2", 1),
                make_log(100, "0xfake3", 2),
            ],
        );
        let range = BlockRange { from: 100, to: 100 };
        let blocks = convert(response, &range);

        assert_eq!(blocks[0].logs.len(), 0);
    }

    #[test]
    fn test_convert_response_log_without_block_header_still_kept() {

        // Log for block 101 but no block header for 101 — log gets a synthetic block entry
        let response = make_response(
            vec![make_block(100, "0xh100", "0xp", 1000)],
            vec![make_log(101, "0xh101", 0)],
        );
        let range = BlockRange { from: 100, to: 101 };
        let blocks = convert(response, &range);

        // Block 100 from header (no logs), block 101 from log (synthetic)
        let block101 = blocks.iter().find(|b| b.block.number == 101).unwrap();
        assert_eq!(block101.logs.len(), 1);
        // Synthetic block should use the log's block_hash
        assert_eq!(block101.block.hash, "0xh101");
    }

    // --- convert_response: multiple batches ---

    #[test]
    fn test_convert_response_multiple_batches() {

        let response = HyperSyncResponse {
            data: vec![
                HyperSyncDataBatch {
                    blocks: vec![make_block(100, "0xh100", "0xp", 1000)],
                    logs: vec![make_log(100, "0xh100", 0)],
                },
                HyperSyncDataBatch {
                    blocks: vec![make_block(101, "0xh101", "0xh100", 1001)],
                    logs: vec![make_log(101, "0xh101", 0)],
                },
            ],
            archive_height: None,
            next_block: None,
        };
        let range = BlockRange { from: 100, to: 101 };
        let blocks = convert(response, &range);

        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].block.number, 100);
        assert_eq!(blocks[0].logs.len(), 1);
        assert_eq!(blocks[1].block.number, 101);
        assert_eq!(blocks[1].logs.len(), 1);
    }

    // --- parse_timestamp ---

    #[test]
    fn test_parse_timestamp_numeric() {
        let val = serde_json::Value::Number(serde_json::Number::from(1700000000u64));
        assert_eq!(HyperSyncBlockSource::parse_timestamp(&val), 1700000000);
    }

    #[test]
    fn test_parse_timestamp_hex_string() {
        let val = serde_json::Value::String("0x65527f00".to_string());
        assert_eq!(HyperSyncBlockSource::parse_timestamp(&val), 0x65527f00);
    }

    #[test]
    fn test_parse_timestamp_hex_no_prefix() {
        let val = serde_json::Value::String("65527f00".to_string());
        assert_eq!(HyperSyncBlockSource::parse_timestamp(&val), 0x65527f00);
    }

    #[test]
    fn test_parse_timestamp_null() {
        let val = serde_json::Value::Null;
        assert_eq!(HyperSyncBlockSource::parse_timestamp(&val), 0);
    }

    #[test]
    fn test_parse_timestamp_invalid_string() {
        let val = serde_json::Value::String("not_a_number".to_string());
        assert_eq!(HyperSyncBlockSource::parse_timestamp(&val), 0);
    }

    // --- convert_response: combined out-of-range + hash mismatch ---

    #[test]
    fn test_convert_response_mixed_invalid_logs() {
        let response = make_response(
            vec![
                make_block(100, "0xh100", "0xp", 1000),
                make_block(101, "0xh101", "0xh100", 1001),
            ],
            vec![
                make_log(99, "0xother", 0),       // out of range — dropped
                make_log(100, "0xwrong", 1),       // hash mismatch — dropped
                make_log(100, "0xh100", 2),        // valid
                make_log(101, "0xh101", 3),        // valid
                make_log(202, "0xother", 4),       // out of range — dropped
            ],
        );
        let range = BlockRange { from: 100, to: 200 };
        let blocks = convert(response, &range);

        let block100 = blocks.iter().find(|b| b.block.number == 100).unwrap();
        assert_eq!(block100.logs.len(), 1);
        assert_eq!(block100.logs[0].log_index, 2); // only the valid one

        let block101 = blocks.iter().find(|b| b.block.number == 101).unwrap();
        assert_eq!(block101.logs.len(), 1);
        assert_eq!(block101.logs[0].log_index, 3);
    }

    // --- resolve_fallback_rpc tests ---

    #[test]
    fn test_resolve_fallback_rpc_explicit_url() {
        let url = resolve_fallback_rpc(Some("http://my-rpc:8545"), 999999).unwrap();
        assert_eq!(url, "http://my-rpc:8545");
    }

    #[test]
    fn test_resolve_fallback_rpc_ethereum() {
        let url = resolve_fallback_rpc(None, 1).unwrap();
        assert!(url.contains("llamarpc"));
    }

    #[test]
    fn test_resolve_fallback_rpc_polygon() {
        let url = resolve_fallback_rpc(None, 137).unwrap();
        assert!(url.contains("polygon"));
    }

    #[test]
    fn test_resolve_fallback_rpc_arbitrum() {
        let url = resolve_fallback_rpc(None, 42161).unwrap();
        assert!(url.contains("arbitrum"));
    }

    #[test]
    fn test_resolve_fallback_rpc_optimism() {
        let url = resolve_fallback_rpc(None, 10).unwrap();
        assert!(url.contains("optimism"));
    }

    #[test]
    fn test_resolve_fallback_rpc_base() {
        let url = resolve_fallback_rpc(None, 8453).unwrap();
        assert!(url.contains("base"));
    }

    #[test]
    fn test_resolve_fallback_rpc_bsc() {
        let url = resolve_fallback_rpc(None, 56).unwrap();
        assert!(url.contains("binance"));
    }

    #[test]
    fn test_resolve_fallback_rpc_avalanche() {
        let url = resolve_fallback_rpc(None, 43114).unwrap();
        assert!(url.contains("avax"));
    }

    #[test]
    fn test_resolve_fallback_rpc_sepolia() {
        let url = resolve_fallback_rpc(None, 11155111).unwrap();
        assert!(url.contains("sepolia"));
    }

    #[test]
    fn test_resolve_fallback_rpc_unknown_chain_errors() {
        let result = resolve_fallback_rpc(None, 999999);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("999999"));
        assert!(err.contains("fallback_rpc"));
    }

    #[test]
    fn test_resolve_fallback_rpc_explicit_overrides_default() {
        // Even for known chains, explicit URL takes precedence
        let url = resolve_fallback_rpc(Some("http://custom:8545"), 1).unwrap();
        assert_eq!(url, "http://custom:8545");
    }

    #[test]
    fn test_resolve_fallback_rpc_explicit_allows_unknown_chain() {
        // Unknown chain is fine when fallback_rpc is explicitly provided
        let url = resolve_fallback_rpc(Some("http://custom:8545"), 999999).unwrap();
        assert_eq!(url, "http://custom:8545");
    }
}
