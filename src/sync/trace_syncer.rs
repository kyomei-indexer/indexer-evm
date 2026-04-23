use crate::abi::function_decoder::FunctionDecoder;
use crate::config::{RetryConfig, TraceConfig};
use crate::db::traces::TraceRepository;
use crate::sources::traces::{RpcTraceSource, TraceMethod};
use crate::sources::TraceSource;
use crate::types::BlockRange;
use anyhow::{Context, Result};
use sqlx::PgPool;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

/// Orchestrates call trace indexing: fetches traces per-block, optionally decodes
/// function calls, and stores both raw and decoded trace records.
pub struct TraceSyncer {
    trace_source: Arc<dyn TraceSource>,
    trace_repo: TraceRepository,
    function_decoder: Option<Arc<FunctionDecoder>>,
    chain_id: i32,
    addresses: Vec<String>,
    cancel: CancellationToken,
}

impl TraceSyncer {
    /// Create a TraceSyncer from config. Returns None if trace config is not enabled.
    pub fn new(
        pool: PgPool,
        data_schema: &str,
        chain_id: i32,
        trace_config: &TraceConfig,
        rpc_url: &str,
        retry_config: RetryConfig,
        function_decoder: Option<Arc<FunctionDecoder>>,
        cancel: CancellationToken,
    ) -> Result<Self> {
        let method = TraceMethod::from_str(&trace_config.method)
            .context("Invalid trace method in config")?;

        let trace_source = Arc::new(RpcTraceSource::new(
            rpc_url,
            chain_id as u32,
            method,
            &trace_config.tracer,
            retry_config,
        ));

        let trace_repo = TraceRepository::new(pool, data_schema.to_string());

        // Collect addresses to filter by
        let addresses: Vec<String> = trace_config
            .contracts
            .iter()
            .filter_map(|c| c.address.clone())
            .map(|a| a.to_lowercase())
            .collect();

        debug!(
            chain_id,
            method = trace_config.method.as_str(),
            tracer = trace_config.tracer.as_str(),
            contract_count = trace_config.contracts.len(),
            address_count = addresses.len(),
            "TraceSyncer initialized"
        );

        Ok(Self {
            trace_source,
            trace_repo,
            function_decoder,
            chain_id,
            addresses,
            cancel,
        })
    }

    /// Index traces for a single block. Called from ChainSyncer during live sync.
    pub async fn index_block(&self, block_number: u64) -> Result<u64> {
        let range = BlockRange {
            from: block_number,
            to: block_number,
        };

        let blocks = self
            .trace_source
            .get_block_traces(range, &self.addresses)
            .await?;

        let mut total = 0u64;
        for block in &blocks {
            let mut records: Vec<_> = block
                .traces
                .iter()
                .map(|t| t.to_raw_record(self.chain_id))
                .collect();

            // Enrich with function names if decoder available
            if let Some(ref decoder) = self.function_decoder {
                for record in &mut records {
                    if let Some(decoded) = decoder.decode_trace(record) {
                        record.function_name = Some(decoded.function_name);
                    }
                }
            }

            let inserted = self.trace_repo.insert_batch(&records).await?;
            total += inserted;
        }

        if total > 0 {
            crate::metrics::traces_indexed(self.chain_id as u32, "live", total);
            debug!(
                chain_id = self.chain_id,
                block_number,
                traces = total,
                "Indexed traces for block"
            );
        }

        Ok(total)
    }

    /// Run historic trace sync for a block range.
    /// Processes blocks sequentially (trace API calls are expensive, one per block).
    pub async fn run_historic(&self, from_block: u64, to_block: u64) -> Result<u64> {
        let mut total = 0u64;
        let mut current = from_block;
        let batch_size = 10u64; // Process blocks in small batches for progress logging

        info!(
            chain_id = self.chain_id,
            from_block,
            to_block,
            "Starting historic trace sync"
        );

        while current <= to_block {
            if self.cancel.is_cancelled() {
                info!(
                    chain_id = self.chain_id,
                    current_block = current,
                    "Trace sync cancelled"
                );
                break;
            }

            let batch_end = std::cmp::min(current + batch_size - 1, to_block);

            match self.index_range(current, batch_end).await {
                Ok(count) => {
                    total += count;
                    if count > 0 {
                        crate::metrics::traces_indexed(self.chain_id as u32, "historic", count);
                        debug!(
                            chain_id = self.chain_id,
                            from = current,
                            to = batch_end,
                            traces = count,
                            total,
                            "Trace batch indexed"
                        );
                    }
                }
                Err(e) => {
                    error!(
                        chain_id = self.chain_id,
                        from = current,
                        to = batch_end,
                        error = %e,
                        "Trace batch failed"
                    );
                    // Continue to next batch rather than failing entirely
                }
            }

            current = batch_end + 1;
        }

        info!(
            chain_id = self.chain_id,
            from_block,
            to_block,
            total_traces = total,
            "Historic trace sync complete"
        );

        Ok(total)
    }

    /// Index traces for a small range of blocks.
    async fn index_range(&self, from: u64, to: u64) -> Result<u64> {
        let range = BlockRange { from, to };

        let blocks = self
            .trace_source
            .get_block_traces(range, &self.addresses)
            .await?;

        let mut total = 0u64;
        for block in &blocks {
            let mut records: Vec<_> = block
                .traces
                .iter()
                .map(|t| t.to_raw_record(self.chain_id))
                .collect();

            if let Some(ref decoder) = self.function_decoder {
                for record in &mut records {
                    if let Some(decoded) = decoder.decode_trace(record) {
                        record.function_name = Some(decoded.function_name);
                    }
                }
            }

            let inserted = self.trace_repo.insert_batch(&records).await?;
            total += inserted;
        }

        Ok(total)
    }

    /// Delete traces in a block range (for reorg handling).
    pub async fn delete_range(&self, from_block: i64, to_block: i64) -> Result<u64> {
        self.trace_repo
            .delete_range(self.chain_id, from_block, to_block)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trace_method_from_config() {
        assert!(TraceMethod::from_str("debug").is_ok());
        assert!(TraceMethod::from_str("parity").is_ok());
        assert!(TraceMethod::from_str("unknown").is_err());
    }

    #[test]
    fn test_addresses_collected_and_lowercased() {
        use crate::config::TraceContractConfig;
        let config = TraceConfig {
            enabled: true,
            method: "debug".to_string(),
            tracer: "callTracer".to_string(),
            contracts: vec![
                TraceContractConfig {
                    name: "Router".to_string(),
                    address: Some("0xAAAA".to_string()),
                    factory_ref: None,
                    abi_path: None,
                    start_block: None,
                },
                TraceContractConfig {
                    name: "Factory".to_string(),
                    address: None, // no address (factory_ref)
                    factory_ref: Some("UniswapV2Pair".to_string()),
                    abi_path: None,
                    start_block: None,
                },
                TraceContractConfig {
                    name: "Token".to_string(),
                    address: Some("0xBBBB".to_string()),
                    factory_ref: None,
                    abi_path: None,
                    start_block: None,
                },
            ],
        };

        let addresses: Vec<String> = config
            .contracts
            .iter()
            .filter_map(|c| c.address.clone())
            .map(|a| a.to_lowercase())
            .collect();

        assert_eq!(addresses.len(), 2);
        assert_eq!(addresses[0], "0xaaaa");
        assert_eq!(addresses[1], "0xbbbb");
    }
}
