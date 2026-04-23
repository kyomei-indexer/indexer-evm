use super::{AdaptiveRange, BlockSource};
use crate::config::RetryConfig;
use crate::sync::retry::{backoff_delay, classify_rpc_error, BlockValidationError, RpcErrorKind};
use crate::types::{BlockInfo, BlockRange, BlockWithLogs, LogEntry, LogFilter};
use alloy::primitives::Bloom;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::{BlockNumberOrTag, Filter};
use anyhow::{Context, Result};
use async_trait::async_trait;
use tracing::{debug, info, trace, warn};

/// Parse RPC error messages for a suggested block range.
/// Common patterns from Alchemy, Infura, QuickNode, and other providers.
pub fn parse_suggested_range(error_msg: &str) -> Option<u64> {
    let lower = error_msg.to_lowercase();

    // Alchemy: "Log response size exceeded. this block range should work: [0xABC, 0xDEF]"
    if let Some(bracket_start) = lower.find("block range should work: [") {
        let after = &error_msg[bracket_start + 26..];
        if let Some(comma) = after.find(',') {
            let from_hex = after[..comma].trim().trim_start_matches("0x");
            let rest = &after[comma + 1..];
            if let Some(bracket_end) = rest.find(']') {
                let to_hex = rest[..bracket_end].trim().trim_start_matches("0x");
                if let (Ok(from), Ok(to)) =
                    (u64::from_str_radix(from_hex, 16), u64::from_str_radix(to_hex, 16))
                {
                    return Some(to.saturating_sub(from) + 1);
                }
            }
        }
    }

    // Infura/generic: "query returned more than 10000 results"
    if lower.contains("returned more than") || lower.contains("too many results") {
        // No specific range hint, return None to trigger shrink
        return None;
    }

    // Generic: "exceed maximum block range: 2000" or "block range is too wide: 2000"
    for pattern in &["maximum block range:", "block range is too wide:"] {
        if let Some(pos) = lower.find(pattern) {
            let after = &error_msg[pos + pattern.len()..];
            let num_str: String = after.trim().chars().take_while(|c| c.is_ascii_digit()).collect();
            if let Ok(n) = num_str.parse::<u64>() {
                return Some(n);
            }
        }
    }

    None
}

/// Standard JSON-RPC block source using alloy
pub struct RpcBlockSource {
    provider: Box<dyn Provider + Send + Sync>,
    blocks_per_request: u64,
    adaptive_range: AdaptiveRange,
    retry_config: RetryConfig,
    chain_id: u32,
    concurrency: Option<std::sync::Arc<super::concurrency::AdaptiveConcurrency>>,
}

impl RpcBlockSource {
    /// Create a new RPC block source from a URL
    pub async fn new(url: &str, chain_id: u32, blocks_per_request: u64) -> Result<Self> {
        Self::with_retry_config(url, chain_id, blocks_per_request, RetryConfig::default()).await
    }

    /// Create a new RPC block source with custom retry configuration
    pub async fn with_retry_config(
        url: &str,
        chain_id: u32,
        blocks_per_request: u64,
        retry_config: RetryConfig,
    ) -> Result<Self> {
        let provider = ProviderBuilder::new()
            .connect_http(url.parse().context("Invalid RPC URL")?);

        debug!(
            url,
            chain_id,
            max_retries = retry_config.max_retries,
            validate_logs_bloom = retry_config.validate_logs_bloom,
            "Connected to RPC source"
        );

        Ok(Self {
            provider: Box::new(provider),
            blocks_per_request,
            adaptive_range: AdaptiveRange::new(blocks_per_request),
            retry_config,
            chain_id,
            concurrency: None,
        })
    }

    /// Set a shared adaptive concurrency limiter (for multi-worker throttling).
    pub fn with_concurrency(mut self, concurrency: std::sync::Arc<super::concurrency::AdaptiveConcurrency>) -> Self {
        self.concurrency = Some(concurrency);
        self
    }

    /// Fetch logs for a sub-range with retry logic
    async fn fetch_logs_with_retry(
        &self,
        from: u64,
        to: u64,
        filter: &LogFilter,
    ) -> Result<Vec<alloy::rpc::types::Log>> {
        let mut last_error = None;

        for attempt in 0..=self.retry_config.max_retries {
            // Acquire concurrency permit if configured
            let _permit = match &self.concurrency {
                Some(c) => Some(c.acquire().await),
                None => None,
            };
            let start = std::time::Instant::now();
            match self.fetch_logs_once(from, to, filter).await {
                Ok(logs) => {
                    crate::metrics::rpc_request(self.chain_id, "ok");
                    crate::metrics::rpc_latency(self.chain_id, start.elapsed().as_secs_f64());
                    if let Some(c) = &self.concurrency {
                        c.on_success();
                    }
                    // Grow adaptive range on success
                    self.adaptive_range.grow();

                    if attempt > 0 {
                        info!(
                            chain_id = self.chain_id,
                            from_block = from,
                            to_block = to,
                            attempt,
                            log_count = logs.len(),
                            adaptive_range = self.adaptive_range.get(),
                            "eth_getLogs succeeded after retry"
                        );
                    }
                    return Ok(logs);
                }
                Err(e) => {
                    crate::metrics::rpc_latency(self.chain_id, start.elapsed().as_secs_f64());
                    let error_kind = classify_rpc_error(&e);
                    let status = match error_kind {
                        RpcErrorKind::RateLimit => "rate_limited",
                        RpcErrorKind::RangeTooBig => "error",
                        RpcErrorKind::Transient => "error",
                    };
                    crate::metrics::rpc_request(self.chain_id, status);

                    // Notify concurrency limiter of rate limits
                    if let Some(c) = &self.concurrency {
                        if matches!(error_kind, RpcErrorKind::RateLimit) {
                            c.on_rate_limit();
                        }
                    }

                    // Adjust adaptive range based on error type
                    match error_kind {
                        RpcErrorKind::RangeTooBig => {
                            let error_msg = e.to_string();
                            if let Some(suggested) = parse_suggested_range(&error_msg) {
                                self.adaptive_range.set(suggested);
                            } else {
                                debug!(
                                    error_msg = %error_msg,
                                    "Could not parse suggested range from RPC error, shrinking adaptively"
                                );
                                self.adaptive_range.shrink();
                            }
                        }
                        RpcErrorKind::RateLimit => {
                            // Don't adjust range for rate limits
                        }
                        RpcErrorKind::Transient => {}
                    }

                    if attempt < self.retry_config.max_retries {
                        // Use longer backoff for rate limits (minimum 5 seconds)
                        let delay = match error_kind {
                            RpcErrorKind::RateLimit => {
                                let base = backoff_delay(&self.retry_config, attempt);
                                base.max(std::time::Duration::from_secs(5))
                            }
                            _ => backoff_delay(&self.retry_config, attempt),
                        };
                        warn!(
                            chain_id = self.chain_id,
                            from_block = from,
                            to_block = to,
                            attempt = attempt + 1,
                            max_retries = self.retry_config.max_retries,
                            backoff_ms = delay.as_millis() as u64,
                            adaptive_range = self.adaptive_range.get(),
                            error_kind = ?error_kind,
                            error = %e,
                            "eth_getLogs failed, retrying with backoff"
                        );
                        tokio::time::sleep(delay).await;
                    } else {
                        warn!(
                            chain_id = self.chain_id,
                            from_block = from,
                            to_block = to,
                            total_attempts = attempt + 1,
                            error = %e,
                            "eth_getLogs failed after all retry attempts"
                        );
                    }
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap())
    }

    /// Single attempt to fetch logs
    async fn fetch_logs_once(
        &self,
        from: u64,
        to: u64,
        filter: &LogFilter,
    ) -> Result<Vec<alloy::rpc::types::Log>> {
        let mut log_filter = Filter::new().from_block(from).to_block(to);

        if !filter.addresses.is_empty() {
            let addresses: Vec<alloy_primitives::Address> = filter
                .addresses
                .iter()
                .filter_map(|a| a.parse().ok())
                .collect();
            log_filter = log_filter.address(addresses);
        }

        if !filter.topics.is_empty() {
            if let Some(Some(topic0)) = filter.topics.first() {
                if let Ok(hash) = topic0.parse::<alloy_primitives::B256>() {
                    log_filter = log_filter.event_signature(hash);
                }
            }
        }

        let logs = self
            .provider
            .get_logs(&log_filter)
            .await
            .with_context(|| {
                format!(
                    "eth_getLogs failed for chain_id={} range [{}, {}]",
                    self.chain_id, from, to
                )
            })?;

        trace!(
            chain_id = self.chain_id,
            from_block = from,
            to_block = to,
            log_count = logs.len(),
            "Fetched logs from RPC"
        );

        Ok(logs)
    }

    /// Fetch a single block header with retry logic
    async fn fetch_block_header_with_retry(
        &self,
        block_number: u64,
    ) -> Result<Option<alloy::rpc::types::Block>> {
        let mut last_error = None;

        for attempt in 0..=self.retry_config.max_retries {
            match self
                .provider
                .get_block_by_number(BlockNumberOrTag::Number(block_number))
                .await
            {
                Ok(block) => {
                    if attempt > 0 {
                        debug!(
                            chain_id = self.chain_id,
                            block_number,
                            attempt,
                            "Block header fetch succeeded after retry"
                        );
                    }
                    return Ok(block);
                }
                Err(e) => {
                    if attempt < self.retry_config.max_retries {
                        let delay = backoff_delay(&self.retry_config, attempt);
                        warn!(
                            chain_id = self.chain_id,
                            block_number,
                            attempt = attempt + 1,
                            max_retries = self.retry_config.max_retries,
                            backoff_ms = delay.as_millis() as u64,
                            error = %e,
                            "Block header fetch failed, retrying"
                        );
                        tokio::time::sleep(delay).await;
                    }
                    last_error = Some(e);
                }
            }
        }

        Err(last_error
            .unwrap()
            .into())
    }

    /// Validate that a block's logsBloom matches the logs we received.
    ///
    /// Detects the known RPC issue where `block.logsBloom` is non-empty
    /// (indicating logs exist) but `eth_getLogs` returned zero logs.
    fn validate_logs_bloom(
        &self,
        block_number: u64,
        block_hash: &str,
        logs_bloom: &Bloom,
        log_count: usize,
    ) -> Option<BlockValidationError> {
        if !self.retry_config.validate_logs_bloom {
            return None;
        }

        let is_bloom_empty = logs_bloom.is_zero();

        if !is_bloom_empty && log_count == 0 {
            return Some(BlockValidationError::InvalidLogsBloom {
                block_number,
                block_hash: block_hash.to_string(),
            });
        }

        None
    }

    /// Re-fetch logs for a specific block that failed logsBloom validation.
    /// Uses single-block range and retries to get correct data.
    async fn refetch_block_logs(
        &self,
        block_number: u64,
        filter: &LogFilter,
    ) -> Result<Vec<alloy::rpc::types::Log>> {
        debug!(
            chain_id = self.chain_id,
            block_number,
            "Re-fetching logs for block after logsBloom validation failure"
        );

        // Use single-block range to minimize chance of error
        self.fetch_logs_with_retry(block_number, block_number, filter)
            .await
    }

    /// Check if a block's logsBloom indicates any of the filter's addresses might have logs.
    /// Returns true if the bloom contains any monitored address (or if filter has no addresses).
    fn bloom_matches_filter(bloom: &Bloom, filter: &LogFilter) -> bool {
        if bloom.is_zero() {
            return false;
        }
        if filter.addresses.is_empty() {
            // No address filter means any non-empty bloom could match
            return true;
        }
        for addr_str in &filter.addresses {
            if let Ok(addr) = addr_str.parse::<alloy_primitives::Address>() {
                if bloom.contains_input(alloy::primitives::BloomInput::Raw(addr.as_slice())) {
                    return true;
                }
            }
        }
        false
    }

    /// Validate that all logs' block_hash values match the fetched block header hash.
    /// Returns true if any mismatches were found (and logs should be refetched).
    fn has_log_block_hash_mismatch(
        &self,
        block_number: u64,
        header_block_hash: &str,
        logs: &[LogEntry],
    ) -> bool {
        for log in logs {
            if !log.block_hash.is_empty() && log.block_hash != header_block_hash {
                warn!(
                    chain_id = self.chain_id,
                    block_number,
                    log_block_hash = %log.block_hash,
                    header_block_hash,
                    log_index = log.log_index,
                    "Log block_hash does not match block header — RPC returned inconsistent data"
                );
                return true;
            }
        }
        false
    }

    /// Remove logs with block_number outside the requested range.
    /// Returns the number of out-of-range blocks dropped.
    fn drop_out_of_range_blocks(
        &self,
        block_map: &mut std::collections::BTreeMap<u64, Vec<LogEntry>>,
        from: u64,
        to: u64,
    ) -> usize {
        let out_of_range: Vec<u64> = block_map
            .keys()
            .filter(|&&bn| bn < from || bn > to)
            .copied()
            .collect();

        if !out_of_range.is_empty() {
            warn!(
                chain_id = self.chain_id,
                count = out_of_range.len(),
                range_from = from,
                range_to = to,
                blocks = ?out_of_range,
                "Dropping logs with block_number outside requested range — RPC returned out-of-range data"
            );
            for bn in &out_of_range {
                block_map.remove(bn);
            }
        }

        out_of_range.len()
    }

    /// Convert alloy logs into our internal LogEntry format, grouped by block
    fn group_logs_by_block(
        &self,
        logs: &[alloy::rpc::types::Log],
    ) -> std::collections::BTreeMap<u64, Vec<LogEntry>> {
        let mut block_map: std::collections::BTreeMap<u64, Vec<LogEntry>> =
            std::collections::BTreeMap::new();

        for log in logs {
            let block_number = log.block_number.unwrap_or(0);
            let entry = LogEntry {
                block_number,
                block_hash: log
                    .block_hash
                    .map(|h| format!("0x{}", hex::encode(h)))
                    .unwrap_or_default(),
                block_timestamp: 0, // Will be filled from block header
                transaction_hash: log
                    .transaction_hash
                    .map(|h| format!("0x{}", hex::encode(h)))
                    .unwrap_or_default(),
                transaction_index: log.transaction_index.unwrap_or(0) as u32,
                log_index: log.log_index.unwrap_or(0) as u32,
                address: format!("0x{}", hex::encode(log.address())),
                topic0: log.topics().first().map(|t| format!("0x{}", hex::encode(t))),
                topic1: log.topics().get(1).map(|t| format!("0x{}", hex::encode(t))),
                topic2: log.topics().get(2).map(|t| format!("0x{}", hex::encode(t))),
                topic3: log.topics().get(3).map(|t| format!("0x{}", hex::encode(t))),
                data: format!("0x{}", hex::encode(log.data().data.as_ref())),
            };
            block_map.entry(block_number).or_default().push(entry);
        }

        block_map
    }
}

#[async_trait]
impl BlockSource for RpcBlockSource {
    async fn get_blocks(
        &self,
        range: BlockRange,
        filter: &LogFilter,
    ) -> Result<Vec<BlockWithLogs>> {
        let mut all_blocks: Vec<BlockWithLogs> = Vec::new();
        let total_blocks = range.to.saturating_sub(range.from) + 1;

        debug!(
            chain_id = self.chain_id,
            from = range.from,
            to = range.to,
            total_blocks,
            source = "rpc",
            "Fetching block range"
        );

        // Fetch logs in sub-ranges, using adaptive range to avoid RPC limits
        let mut from = range.from;
        while from <= range.to {
            let effective_range = self.adaptive_range.get();
            let to = std::cmp::min(from + effective_range - 1, range.to);

            let logs = self.fetch_logs_with_retry(from, to, filter).await?;

            trace!(
                chain_id = self.chain_id,
                from_block = from,
                to_block = to,
                log_count = logs.len(),
                "Processing fetched logs"
            );

            // Group logs by block number
            let mut block_map = self.group_logs_by_block(&logs);

            // Drop any logs with block_number outside the requested range
            self.drop_out_of_range_blocks(&mut block_map, from, to);

            // Prefetch all block headers in parallel for throughput
            let block_numbers: Vec<u64> = block_map.keys().copied().collect();
            let header_futures: Vec<_> = block_numbers.iter()
                .map(|&bn| self.fetch_block_header_with_retry(bn))
                .collect();
            let header_results = futures::future::join_all(header_futures).await;
            let headers: std::collections::HashMap<u64, _> = block_numbers.into_iter()
                .zip(header_results)
                .collect();

            for (block_number, block_logs) in &mut block_map {
                match headers.get(block_number).unwrap() {
                    Ok(Some(block)) => {
                        let block_hash = format!("0x{}", hex::encode(block.header.hash));

                        // Validate logsBloom
                        if let Some(validation_err) = self.validate_logs_bloom(
                            *block_number,
                            &block_hash,
                            &block.header.logs_bloom,
                            block_logs.len(),
                        ) {
                            warn!(
                                chain_id = self.chain_id,
                                block_number = *block_number,
                                block_hash = %block_hash,
                                log_count = block_logs.len(),
                                error = %validation_err,
                                "logsBloom validation failed — re-fetching block logs"
                            );

                            // Retry fetching logs for this specific block
                            let mut bloom_valid = false;
                            for bloom_attempt in 0..self.retry_config.bloom_validation_retries {
                                let delay = backoff_delay(&self.retry_config, bloom_attempt);
                                tokio::time::sleep(delay).await;

                                match self.refetch_block_logs(*block_number, filter).await {
                                    Ok(refetched_logs) => {
                                        if !refetched_logs.is_empty() {
                                            info!(
                                                chain_id = self.chain_id,
                                                block_number = *block_number,
                                                log_count = refetched_logs.len(),
                                                bloom_attempt = bloom_attempt + 1,
                                                "logsBloom re-fetch recovered missing logs"
                                            );

                                            // Replace the empty logs with refetched ones
                                            let new_block_map =
                                                self.group_logs_by_block(&refetched_logs);
                                            if let Some(new_logs) =
                                                new_block_map.get(block_number)
                                            {
                                                *block_logs = new_logs.clone();
                                            }
                                            bloom_valid = true;
                                            break;
                                        } else {
                                            warn!(
                                                chain_id = self.chain_id,
                                                block_number = *block_number,
                                                bloom_attempt = bloom_attempt + 1,
                                                max_bloom_retries = self.retry_config.bloom_validation_retries,
                                                "logsBloom re-fetch still returned zero logs"
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        warn!(
                                            chain_id = self.chain_id,
                                            block_number = *block_number,
                                            bloom_attempt = bloom_attempt + 1,
                                            error = %e,
                                            "logsBloom re-fetch failed"
                                        );
                                    }
                                }
                            }

                            if !bloom_valid {
                                warn!(
                                    chain_id = self.chain_id,
                                    block_number = *block_number,
                                    block_hash = %block_hash,
                                    "logsBloom validation failed after all retries — \
                                     proceeding with available data. This block may have missing events."
                                );
                            }
                        }

                        // Cross-validate log block_hashes against the header
                        if self.has_log_block_hash_mismatch(*block_number, &block_hash, block_logs) {
                            match self.refetch_block_logs(*block_number, filter).await {
                                Ok(refetched) => {
                                    let new_map = self.group_logs_by_block(&refetched);
                                    if let Some(new_logs) = new_map.get(block_number) {
                                        *block_logs = new_logs.clone();
                                        info!(
                                            chain_id = self.chain_id,
                                            block_number = *block_number,
                                            log_count = block_logs.len(),
                                            "Replaced mismatched logs with refetched data"
                                        );
                                    }
                                }
                                Err(e) => {
                                    warn!(
                                        chain_id = self.chain_id,
                                        block_number = *block_number,
                                        error = %e,
                                        "Failed to refetch logs after block_hash mismatch"
                                    );
                                }
                            }
                        }

                        let block_info = BlockInfo {
                            number: *block_number,
                            hash: block_hash,
                            parent_hash: format!(
                                "0x{}",
                                hex::encode(block.header.parent_hash)
                            ),
                            timestamp: block.header.timestamp,
                        };

                        // Set timestamps on logs
                        for log in block_logs.iter_mut() {
                            log.block_timestamp = block_info.timestamp;
                        }

                        all_blocks.push(BlockWithLogs {
                            block: block_info,
                            logs: block_logs.clone(),
                        });
                    }
                    Ok(None) => {
                        warn!(
                            chain_id = self.chain_id,
                            block_number = *block_number,
                            log_count = block_logs.len(),
                            "Block header not found — block may not yet be available. \
                             Logs for this block will be skipped."
                        );
                    }
                    Err(e) => {
                        warn!(
                            chain_id = self.chain_id,
                            block_number = *block_number,
                            log_count = block_logs.len(),
                            error = %e,
                            "Failed to fetch block header after retries — \
                             logs for this block will be skipped"
                        );
                    }
                }
            }

            from = to + 1;
        }

        // For small ranges (live sync), fetch headers for blocks with no matching logs.
        // This is important for reorg detection — we need parent_hash chain continuity.
        // Also use bloom filter pre-check: if the bloom doesn't match our filter,
        // we know there are no relevant logs and can skip the eth_getLogs call.
        if range.to - range.from < 10 {
            let mut missing_blocks = Vec::new();
            for block_num in range.from..=range.to {
                if !all_blocks.iter().any(|b| b.block.number == block_num) {
                    missing_blocks.push(block_num);
                }
            }

            if !missing_blocks.is_empty() {
                trace!(
                    chain_id = self.chain_id,
                    missing_count = missing_blocks.len(),
                    range_from = range.from,
                    range_to = range.to,
                    "Fetching headers for blocks with no matching logs (live sync)"
                );

                for block_num in missing_blocks {
                    match self.fetch_block_header_with_retry(block_num).await {
                        Ok(Some(block)) => {
                            let block_hash =
                                format!("0x{}", hex::encode(block.header.hash));

                            // Bloom pre-check: if the bloom matches our filter,
                            // fetch logs for this block (they may have been missed
                            // because they weren't in the original range response)
                            if Self::bloom_matches_filter(&block.header.logs_bloom, filter) {
                                debug!(
                                    chain_id = self.chain_id,
                                    block_number = block_num,
                                    "Bloom filter matches — fetching logs for block"
                                );
                                match self.refetch_block_logs(block_num, filter).await {
                                    Ok(refetched) if !refetched.is_empty() => {
                                        let new_map = self.group_logs_by_block(&refetched);
                                        let logs = new_map
                                            .get(&block_num)
                                            .cloned()
                                            .unwrap_or_default()
                                            .into_iter()
                                            .map(|mut log| {
                                                log.block_timestamp = block.header.timestamp;
                                                log
                                            })
                                            .collect();

                                        all_blocks.push(BlockWithLogs {
                                            block: BlockInfo {
                                                number: block_num,
                                                hash: block_hash,
                                                parent_hash: format!(
                                                    "0x{}",
                                                    hex::encode(block.header.parent_hash)
                                                ),
                                                timestamp: block.header.timestamp,
                                            },
                                            logs,
                                        });
                                        continue;
                                    }
                                    Ok(_) => {
                                        debug!(
                                            chain_id = self.chain_id,
                                            block_number = block_num,
                                            "Bloom matched but no logs returned — false positive or unrelated events"
                                        );
                                    }
                                    Err(e) => {
                                        warn!(
                                            chain_id = self.chain_id,
                                            block_number = block_num,
                                            error = %e,
                                            "Failed to fetch logs after bloom match"
                                        );
                                    }
                                }
                            }

                            all_blocks.push(BlockWithLogs {
                                block: BlockInfo {
                                    number: block_num,
                                    hash: block_hash,
                                    parent_hash: format!(
                                        "0x{}",
                                        hex::encode(block.header.parent_hash)
                                    ),
                                    timestamp: block.header.timestamp,
                                },
                                logs: Vec::new(),
                            });
                        }
                        Ok(None) => {
                            warn!(
                                chain_id = self.chain_id,
                                block_number = block_num,
                                "Block not found during live sync — \
                                 block may not be available yet from this RPC node"
                            );
                        }
                        Err(e) => {
                            warn!(
                                chain_id = self.chain_id,
                                block_number = block_num,
                                error = %e,
                                "Failed to fetch block header during live sync"
                            );
                        }
                    }
                }
            }
        }

        // Sort by block number
        all_blocks.sort_by_key(|b| b.block.number);

        debug!(
            chain_id = self.chain_id,
            from = range.from,
            to = range.to,
            blocks_returned = all_blocks.len(),
            total_logs = all_blocks.iter().map(|b| b.logs.len()).sum::<usize>(),
            "Block range fetch complete"
        );

        Ok(all_blocks)
    }

    async fn get_latest_block_number(&self) -> Result<u64> {
        let mut last_error = None;

        for attempt in 0..=self.retry_config.max_retries {
            match self.provider.get_block_number().await {
                Ok(number) => {
                    if attempt > 0 {
                        debug!(
                            chain_id = self.chain_id,
                            attempt,
                            block_number = number,
                            "eth_blockNumber succeeded after retry"
                        );
                    }
                    return Ok(number);
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
                            "eth_blockNumber failed, retrying"
                        );
                        tokio::time::sleep(delay).await;
                    }
                    last_error = Some(e);
                }
            }
        }

        Err(last_error
            .unwrap()
            .into())
    }

    async fn get_block_hash(&self, block_number: u64) -> Result<Option<String>> {
        match self.fetch_block_header_with_retry(block_number).await? {
            Some(block) => Ok(Some(format!("0x{}", hex::encode(block.header.hash)))),
            None => {
                debug!(
                    chain_id = self.chain_id,
                    block_number,
                    "Block hash not found (block may not exist)"
                );
                Ok(None)
            }
        }
    }

    fn source_type(&self) -> &'static str {
        "rpc"
    }
}

/// Check if a logsBloom is empty (all zeros)
pub fn is_bloom_empty(bloom: &Bloom) -> bool {
    bloom.is_zero()
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::Bloom;

    #[test]
    fn test_is_bloom_empty_zeros() {
        let bloom = Bloom::ZERO;
        assert!(is_bloom_empty(&bloom));
    }

    #[test]
    fn test_is_bloom_empty_non_zero() {
        let mut bytes = [0u8; 256];
        bytes[0] = 1;
        let bloom = Bloom::from(bytes);
        assert!(!is_bloom_empty(&bloom));
    }

    #[test]
    fn test_is_bloom_empty_last_byte_set() {
        let mut bytes = [0u8; 256];
        bytes[255] = 0xFF;
        let bloom = Bloom::from(bytes);
        assert!(!is_bloom_empty(&bloom));
    }

    #[test]
    fn test_validate_logs_bloom_empty_bloom_zero_logs() {
        let source = create_test_source();
        let bloom = Bloom::ZERO;
        let result = source.validate_logs_bloom(100, "0xhash", &bloom, 0);
        assert!(result.is_none()); // Empty bloom with zero logs is fine
    }

    #[test]
    fn test_validate_logs_bloom_nonempty_bloom_has_logs() {
        let source = create_test_source();
        let mut bytes = [0u8; 256];
        bytes[0] = 1;
        let bloom = Bloom::from(bytes);
        let result = source.validate_logs_bloom(100, "0xhash", &bloom, 5);
        assert!(result.is_none()); // Non-empty bloom with logs is fine
    }

    #[test]
    fn test_validate_logs_bloom_nonempty_bloom_zero_logs_error() {
        let source = create_test_source();
        let mut bytes = [0u8; 256];
        bytes[0] = 1;
        let bloom = Bloom::from(bytes);
        let result = source.validate_logs_bloom(100, "0xhash", &bloom, 0);
        assert!(result.is_some());
        let err = result.unwrap();
        match err {
            BlockValidationError::InvalidLogsBloom {
                block_number,
                block_hash,
            } => {
                assert_eq!(block_number, 100);
                assert_eq!(block_hash, "0xhash");
            }
            _ => panic!("Expected InvalidLogsBloom error"),
        }
    }

    #[test]
    fn test_validate_logs_bloom_disabled() {
        let mut source = create_test_source();
        source.retry_config.validate_logs_bloom = false;

        let mut bytes = [0u8; 256];
        bytes[0] = 1;
        let bloom = Bloom::from(bytes);
        // Even with non-empty bloom and zero logs, validation should pass when disabled
        let result = source.validate_logs_bloom(100, "0xhash", &bloom, 0);
        assert!(result.is_none());
    }

    #[test]
    fn test_group_logs_by_block_empty() {
        let source = create_test_source();
        let logs: Vec<alloy::rpc::types::Log> = Vec::new();
        let result = source.group_logs_by_block(&logs);
        assert!(result.is_empty());
    }

    // === AdaptiveRange tests ===

    #[test]
    fn test_adaptive_range_initial() {
        let range = AdaptiveRange::new(1000);
        assert_eq!(range.get(), 1000);
    }

    #[test]
    fn test_adaptive_range_shrink() {
        let range = AdaptiveRange::new(1000);
        range.shrink();
        assert_eq!(range.get(), 500);
        range.shrink();
        assert_eq!(range.get(), 250);
    }

    #[test]
    fn test_adaptive_range_shrink_min() {
        let range = AdaptiveRange::new(20);
        range.shrink(); // 10
        assert_eq!(range.get(), 10);
        range.shrink(); // still 10 (min)
        assert_eq!(range.get(), 10);
    }

    #[test]
    fn test_adaptive_range_grow() {
        let range = AdaptiveRange::new(1000);
        range.shrink(); // 500
        range.grow(); // 525
        assert_eq!(range.get(), 525);
    }

    #[test]
    fn test_adaptive_range_grow_capped_at_max() {
        let range = AdaptiveRange::new(1000);
        range.grow(); // should stay at 1000 (max)
        assert_eq!(range.get(), 1000);
    }

    #[test]
    fn test_adaptive_range_set() {
        let range = AdaptiveRange::new(1000);
        range.set(200);
        assert_eq!(range.get(), 200);
    }

    #[test]
    fn test_adaptive_range_set_clamped() {
        let range = AdaptiveRange::new(1000);
        range.set(5); // below min, clamped to 10
        assert_eq!(range.get(), 10);
        range.set(5000); // above max, clamped to 1000
        assert_eq!(range.get(), 1000);
    }

    // === parse_suggested_range tests ===

    #[test]
    fn test_parse_suggested_range_alchemy() {
        let msg = "Log response size exceeded. this block range should work: [0x100, 0x1FF]";
        let result = parse_suggested_range(msg);
        assert!(result.is_some());
        // 0x1FF - 0x100 + 1 = 256
        assert_eq!(result.unwrap(), 256);
    }

    #[test]
    fn test_parse_suggested_range_generic_max() {
        let msg = "exceed maximum block range: 2000";
        let result = parse_suggested_range(msg);
        assert_eq!(result, Some(2000));
    }

    #[test]
    fn test_parse_suggested_range_too_many_results() {
        let msg = "query returned more than 10000 results";
        let result = parse_suggested_range(msg);
        assert_eq!(result, None); // No specific range, triggers shrink
    }

    #[test]
    fn test_parse_suggested_range_unrelated_error() {
        let msg = "connection refused";
        let result = parse_suggested_range(msg);
        assert_eq!(result, None);
    }

    // === bloom_matches_filter tests ===

    #[test]
    fn test_bloom_matches_filter_empty_bloom() {
        let bloom = Bloom::ZERO;
        let filter = LogFilter {
            addresses: vec!["0x1234567890abcdef1234567890abcdef12345678".to_string()],
            topics: vec![],
        };
        assert!(!RpcBlockSource::bloom_matches_filter(&bloom, &filter));
    }

    #[test]
    fn test_bloom_matches_filter_empty_addresses() {
        let mut bytes = [0u8; 256];
        bytes[0] = 1;
        let bloom = Bloom::from(bytes);
        let filter = LogFilter::default();
        // No address filter + non-empty bloom → could match
        assert!(RpcBlockSource::bloom_matches_filter(&bloom, &filter));
    }

    // === validate_log_block_hashes tests ===

    #[test]
    fn test_has_log_block_hash_mismatch_all_match() {
        let source = create_test_source();
        let logs = vec![
            LogEntry {
                block_number: 100,
                block_hash: "0xabc".to_string(),
                block_timestamp: 0,
                transaction_hash: "0x".to_string(),
                transaction_index: 0,
                log_index: 0,
                address: "0x".to_string(),
                topic0: None, topic1: None, topic2: None, topic3: None,
                data: "0x".to_string(),
            },
        ];
        assert!(!source.has_log_block_hash_mismatch(100, "0xabc", &logs));
    }

    #[test]
    fn test_has_log_block_hash_mismatch_detected() {
        let source = create_test_source();
        let logs = vec![
            LogEntry {
                block_number: 100,
                block_hash: "0xwrong".to_string(),
                block_timestamp: 0,
                transaction_hash: "0x".to_string(),
                transaction_index: 0,
                log_index: 0,
                address: "0x".to_string(),
                topic0: None, topic1: None, topic2: None, topic3: None,
                data: "0x".to_string(),
            },
        ];
        assert!(source.has_log_block_hash_mismatch(100, "0xabc", &logs));
    }

    #[test]
    fn test_has_log_block_hash_mismatch_empty_hash_ignored() {
        let source = create_test_source();
        let logs = vec![
            LogEntry {
                block_number: 100,
                block_hash: "".to_string(),
                block_timestamp: 0,
                transaction_hash: "0x".to_string(),
                transaction_index: 0,
                log_index: 0,
                address: "0x".to_string(),
                topic0: None, topic1: None, topic2: None, topic3: None,
                data: "0x".to_string(),
            },
        ];
        assert!(!source.has_log_block_hash_mismatch(100, "0xabc", &logs));
    }

    // === drop_out_of_range_blocks tests ===

    #[test]
    fn test_drop_out_of_range_blocks_none_dropped() {
        let source = create_test_source();
        let mut block_map = std::collections::BTreeMap::new();
        block_map.insert(100u64, vec![]);
        block_map.insert(101u64, vec![]);
        let dropped = source.drop_out_of_range_blocks(&mut block_map, 100, 101);
        assert_eq!(dropped, 0);
        assert_eq!(block_map.len(), 2);
    }

    #[test]
    fn test_drop_out_of_range_blocks_some_dropped() {
        let source = create_test_source();
        let mut block_map = std::collections::BTreeMap::new();
        block_map.insert(99u64, vec![]);
        block_map.insert(100u64, vec![]);
        block_map.insert(200u64, vec![]);
        let dropped = source.drop_out_of_range_blocks(&mut block_map, 100, 150);
        assert_eq!(dropped, 2); // 99 and 200 are out of range
        assert_eq!(block_map.len(), 1);
        assert!(block_map.contains_key(&100));
    }

    /// Creates a minimal test RpcBlockSource (without actual connection)
    fn create_test_source() -> RpcBlockSource {
        // We use a dummy provider that won't actually be called in unit tests
        let provider = ProviderBuilder::new()
            .connect_http("http://localhost:1".parse().unwrap());

        RpcBlockSource {
            provider: Box::new(provider),
            blocks_per_request: 1000,
            adaptive_range: AdaptiveRange::new(1000),
            retry_config: RetryConfig::default(),
            chain_id: 1,
            concurrency: None,
        }
    }
}
