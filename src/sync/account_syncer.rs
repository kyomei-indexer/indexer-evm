use crate::config::{AccountConfig, RetryConfig};
use crate::db::accounts::AccountEventRepository;
use crate::sources::transactions::RpcTransactionSource;
use crate::sources::TransactionSource;
use crate::types::{BlockRange, RawAccountEventRecord, TransactionInfo};
use anyhow::Result;
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

/// Orchestrates account/transaction indexing: fetches full block transactions,
/// filters by tracked addresses, and stores account event records.
pub struct AccountSyncer {
    tx_source: Arc<dyn TransactionSource>,
    account_repo: AccountEventRepository,
    chain_id: i32,
    /// address (lowercased) → set of event types to track
    tracked: HashMap<String, HashSet<String>>,
    cancel: CancellationToken,
}

impl AccountSyncer {
    /// Create an AccountSyncer from config.
    pub fn new(
        pool: PgPool,
        data_schema: &str,
        chain_id: i32,
        account_config: &AccountConfig,
        rpc_url: &str,
        retry_config: RetryConfig,
        cancel: CancellationToken,
    ) -> Self {
        let tx_source = Arc::new(RpcTransactionSource::new(
            rpc_url,
            chain_id as u32,
            retry_config,
        ));

        let account_repo = AccountEventRepository::new(pool, data_schema.to_string());

        let mut tracked = HashMap::new();
        for addr_config in &account_config.addresses {
            let events: HashSet<String> = addr_config.events.iter().cloned().collect();
            tracked.insert(addr_config.address.to_lowercase(), events);
        }

        debug!(
            chain_id,
            tracked_addresses = tracked.len(),
            "AccountSyncer initialized"
        );

        Self {
            tx_source,
            account_repo,
            chain_id,
            tracked,
            cancel,
        }
    }

    /// Index account events for a single block. Called from ChainSyncer during live sync.
    pub async fn index_block(&self, block_number: u64) -> Result<u64> {
        let range = BlockRange {
            from: block_number,
            to: block_number,
        };

        let blocks = self.tx_source.get_block_transactions(range).await?;

        let mut total = 0u64;
        for block in &blocks {
            let events = self.extract_account_events(&block.transactions, &block.block);
            if !events.is_empty() {
                let inserted = self.account_repo.insert_batch(&events).await?;
                total += inserted;
            }
        }

        if total > 0 {
            crate::metrics::account_events_indexed(self.chain_id as u32, "all", total);
            debug!(
                chain_id = self.chain_id,
                block_number,
                events = total,
                "Indexed account events for block"
            );
        }

        Ok(total)
    }

    /// Run historic account event sync for a block range.
    pub async fn run_historic(&self, from_block: u64, to_block: u64) -> Result<u64> {
        let mut total = 0u64;
        let mut current = from_block;
        let batch_size = 10u64;

        info!(
            chain_id = self.chain_id,
            from_block,
            to_block,
            "Starting historic account sync"
        );

        while current <= to_block {
            if self.cancel.is_cancelled() {
                info!(
                    chain_id = self.chain_id,
                    current_block = current,
                    "Account sync cancelled"
                );
                break;
            }

            let batch_end = std::cmp::min(current + batch_size - 1, to_block);
            let range = BlockRange {
                from: current,
                to: batch_end,
            };

            match self.tx_source.get_block_transactions(range).await {
                Ok(blocks) => {
                    for block in &blocks {
                        let events =
                            self.extract_account_events(&block.transactions, &block.block);
                        if !events.is_empty() {
                            let inserted = self.account_repo.insert_batch(&events).await?;
                            total += inserted;
                            crate::metrics::account_events_indexed(
                                self.chain_id as u32,
                                "all",
                                inserted,
                            );
                        }
                    }
                }
                Err(e) => {
                    error!(
                        chain_id = self.chain_id,
                        from = current,
                        to = batch_end,
                        error = %e,
                        "Account event batch failed"
                    );
                }
            }

            current = batch_end + 1;
        }

        info!(
            chain_id = self.chain_id,
            from_block,
            to_block,
            total_events = total,
            "Historic account sync complete"
        );

        Ok(total)
    }

    /// Extract account events from a block's transactions by matching against tracked addresses.
    pub fn extract_account_events(
        &self,
        transactions: &[TransactionInfo],
        block: &crate::types::BlockInfo,
    ) -> Vec<RawAccountEventRecord> {
        let mut events = Vec::new();

        for tx in transactions {
            let from_lower = tx.from_address.to_lowercase();
            let to_lower = tx
                .to_address
                .as_ref()
                .map(|a| a.to_lowercase())
                .unwrap_or_default();

            // Check if sender is tracked
            if let Some(tracked_events) = self.tracked.get(&from_lower) {
                if tracked_events.contains("transaction:from") {
                    events.push(RawAccountEventRecord {
                        chain_id: self.chain_id,
                        block_number: block.number as i64,
                        tx_index: tx.tx_index as i32,
                        tx_hash: tx.tx_hash.clone(),
                        block_hash: block.hash.clone(),
                        block_timestamp: block.timestamp as i64,
                        event_type: "transaction:from".to_string(),
                        address: from_lower.clone(),
                        counterparty: to_lower.clone(),
                        value: tx.value.clone(),
                        input: Some(tx.input.clone()),
                        trace_address: None,
                    });
                }
            }

            // Check if receiver is tracked
            if !to_lower.is_empty() {
                if let Some(tracked_events) = self.tracked.get(&to_lower) {
                    if tracked_events.contains("transaction:to") {
                        events.push(RawAccountEventRecord {
                            chain_id: self.chain_id,
                            block_number: block.number as i64,
                            tx_index: tx.tx_index as i32,
                            tx_hash: tx.tx_hash.clone(),
                            block_hash: block.hash.clone(),
                            block_timestamp: block.timestamp as i64,
                            event_type: "transaction:to".to_string(),
                            address: to_lower.clone(),
                            counterparty: from_lower.clone(),
                            value: tx.value.clone(),
                            input: Some(tx.input.clone()),
                            trace_address: None,
                        });
                    }
                }
            }

            // Note: transfer:from and transfer:to events require trace data
            // (internal value transfers). These are handled separately when
            // both trace and account indexing are enabled.
        }

        events
    }

    /// Delete account events in a block range (for reorg handling).
    pub async fn delete_range(&self, from_block: i64, to_block: i64) -> Result<u64> {
        self.account_repo
            .delete_range(self.chain_id, from_block, to_block)
            .await
    }

    /// Extract transfer events from call traces for tracked addresses.
    /// Called when both trace and account indexing are enabled.
    pub fn extract_transfer_events_from_traces(
        &self,
        traces: &[crate::types::RawTraceRecord],
        block: &crate::types::BlockInfo,
    ) -> Vec<RawAccountEventRecord> {
        let mut events = Vec::new();

        for trace in traces {
            // Only consider value transfers
            if trace.value == "0" {
                continue;
            }

            let from_lower = trace.from_address.to_lowercase();
            let to_lower = trace.to_address.to_lowercase();

            // Check if sender is tracked for transfer:from
            if let Some(tracked_events) = self.tracked.get(&from_lower) {
                if tracked_events.contains("transfer:from") {
                    events.push(RawAccountEventRecord {
                        chain_id: self.chain_id,
                        block_number: block.number as i64,
                        tx_index: trace.tx_index,
                        tx_hash: trace.tx_hash.clone(),
                        block_hash: block.hash.clone(),
                        block_timestamp: block.timestamp as i64,
                        event_type: "transfer:from".to_string(),
                        address: from_lower.clone(),
                        counterparty: to_lower.clone(),
                        value: trace.value.clone(),
                        input: None,
                        trace_address: Some(trace.trace_address.clone()),
                    });
                }
            }

            // Check if receiver is tracked for transfer:to
            if let Some(tracked_events) = self.tracked.get(&to_lower) {
                if tracked_events.contains("transfer:to") {
                    events.push(RawAccountEventRecord {
                        chain_id: self.chain_id,
                        block_number: block.number as i64,
                        tx_index: trace.tx_index,
                        tx_hash: trace.tx_hash.clone(),
                        block_hash: block.hash.clone(),
                        block_timestamp: block.timestamp as i64,
                        event_type: "transfer:to".to_string(),
                        address: to_lower.clone(),
                        counterparty: from_lower.clone(),
                        value: trace.value.clone(),
                        input: None,
                        trace_address: Some(trace.trace_address.clone()),
                    });
                }
            }
        }

        events
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{BlockInfo, TransactionInfo};

    fn test_block() -> BlockInfo {
        BlockInfo {
            number: 18000000,
            hash: "0xblockhash".to_string(),
            parent_hash: "0xparent".to_string(),
            timestamp: 1700000000,
        }
    }

    fn test_tx(from: &str, to: Option<&str>, value: &str) -> TransactionInfo {
        TransactionInfo {
            block_number: 18000000,
            block_hash: "0xblockhash".to_string(),
            block_timestamp: 1700000000,
            tx_hash: "0xtxhash".to_string(),
            tx_index: 0,
            from_address: from.to_string(),
            to_address: to.map(|s| s.to_string()),
            value: value.to_string(),
            input: "0x".to_string(),
            gas_price: "0".to_string(),
            gas_used: None,
            status: None,
            nonce: 0,
        }
    }

    fn build_tracked(addresses: &[(&str, &[&str])]) -> HashMap<String, HashSet<String>> {
        let mut tracked = HashMap::new();
        for (addr, events) in addresses {
            tracked.insert(
                addr.to_lowercase().to_string(),
                events.iter().map(|e| e.to_string()).collect(),
            );
        }
        tracked
    }

    /// Extract account events using the same logic as AccountSyncer, but without requiring a DB pool.
    fn extract_events(
        tracked: &HashMap<String, HashSet<String>>,
        chain_id: i32,
        transactions: &[TransactionInfo],
        block: &BlockInfo,
    ) -> Vec<RawAccountEventRecord> {
        let mut events = Vec::new();

        for tx in transactions {
            let from_lower = tx.from_address.to_lowercase();
            let to_lower = tx.to_address.as_ref().map(|a| a.to_lowercase()).unwrap_or_default();

            if let Some(tracked_events) = tracked.get(&from_lower) {
                if tracked_events.contains("transaction:from") {
                    events.push(RawAccountEventRecord {
                        chain_id,
                        block_number: block.number as i64,
                        tx_index: tx.tx_index as i32,
                        tx_hash: tx.tx_hash.clone(),
                        block_hash: block.hash.clone(),
                        block_timestamp: block.timestamp as i64,
                        event_type: "transaction:from".to_string(),
                        address: from_lower.clone(),
                        counterparty: to_lower.clone(),
                        value: tx.value.clone(),
                        input: Some(tx.input.clone()),
                        trace_address: None,
                    });
                }
            }

            if !to_lower.is_empty() {
                if let Some(tracked_events) = tracked.get(&to_lower) {
                    if tracked_events.contains("transaction:to") {
                        events.push(RawAccountEventRecord {
                            chain_id,
                            block_number: block.number as i64,
                            tx_index: tx.tx_index as i32,
                            tx_hash: tx.tx_hash.clone(),
                            block_hash: block.hash.clone(),
                            block_timestamp: block.timestamp as i64,
                            event_type: "transaction:to".to_string(),
                            address: to_lower.clone(),
                            counterparty: from_lower.clone(),
                            value: tx.value.clone(),
                            input: Some(tx.input.clone()),
                            trace_address: None,
                        });
                    }
                }
            }
        }
        events
    }

    /// Extract transfer events from traces (same logic as AccountSyncer).
    fn extract_transfers(
        tracked: &HashMap<String, HashSet<String>>,
        chain_id: i32,
        traces: &[crate::types::RawTraceRecord],
        block: &BlockInfo,
    ) -> Vec<RawAccountEventRecord> {
        let mut events = Vec::new();
        for trace in traces {
            if trace.value == "0" { continue; }
            let from_lower = trace.from_address.to_lowercase();
            let to_lower = trace.to_address.to_lowercase();
            if let Some(te) = tracked.get(&from_lower) {
                if te.contains("transfer:from") {
                    events.push(RawAccountEventRecord {
                        chain_id,
                        block_number: block.number as i64,
                        tx_index: trace.tx_index,
                        tx_hash: trace.tx_hash.clone(),
                        block_hash: block.hash.clone(),
                        block_timestamp: block.timestamp as i64,
                        event_type: "transfer:from".to_string(),
                        address: from_lower.clone(),
                        counterparty: to_lower.clone(),
                        value: trace.value.clone(),
                        input: None,
                        trace_address: Some(trace.trace_address.clone()),
                    });
                }
            }
            if let Some(te) = tracked.get(&to_lower) {
                if te.contains("transfer:to") {
                    events.push(RawAccountEventRecord {
                        chain_id,
                        block_number: block.number as i64,
                        tx_index: trace.tx_index,
                        tx_hash: trace.tx_hash.clone(),
                        block_hash: block.hash.clone(),
                        block_timestamp: block.timestamp as i64,
                        event_type: "transfer:to".to_string(),
                        address: to_lower.clone(),
                        counterparty: from_lower.clone(),
                        value: trace.value.clone(),
                        input: None,
                        trace_address: Some(trace.trace_address.clone()),
                    });
                }
            }
        }
        events
    }

    #[test]
    fn test_extract_transaction_from() {
        let tracked = build_tracked(&[("0xaaaa", &["transaction:from"])]);
        let txs = vec![test_tx("0xaaaa", Some("0xbbbb"), "1000")];
        let events = extract_events(&tracked, 1, &txs, &test_block());

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "transaction:from");
        assert_eq!(events[0].address, "0xaaaa");
        assert_eq!(events[0].counterparty, "0xbbbb");
    }

    #[test]
    fn test_extract_transaction_to() {
        let tracked = build_tracked(&[("0xbbbb", &["transaction:to"])]);
        let txs = vec![test_tx("0xaaaa", Some("0xbbbb"), "1000")];
        let events = extract_events(&tracked, 1, &txs, &test_block());

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "transaction:to");
        assert_eq!(events[0].address, "0xbbbb");
        assert_eq!(events[0].counterparty, "0xaaaa");
    }

    #[test]
    fn test_extract_both_from_and_to() {
        let tracked = build_tracked(&[
            ("0xaaaa", &["transaction:from"]),
            ("0xbbbb", &["transaction:to"]),
        ]);
        let txs = vec![test_tx("0xaaaa", Some("0xbbbb"), "1000")];
        let events = extract_events(&tracked, 1, &txs, &test_block());

        assert_eq!(events.len(), 2);
    }

    #[test]
    fn test_extract_no_match() {
        let tracked = build_tracked(&[("0xcccc", &["transaction:from"])]);
        let txs = vec![test_tx("0xaaaa", Some("0xbbbb"), "1000")];
        let events = extract_events(&tracked, 1, &txs, &test_block());

        assert!(events.is_empty());
    }

    #[test]
    fn test_extract_contract_creation_no_to() {
        let tracked = build_tracked(&[("0xaaaa", &["transaction:from", "transaction:to"])]);
        let txs = vec![test_tx("0xaaaa", None, "0")];
        let events = extract_events(&tracked, 1, &txs, &test_block());

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "transaction:from");
    }

    #[test]
    fn test_extract_case_insensitive_matching() {
        let tracked = build_tracked(&[("0xaaaa", &["transaction:from"])]);
        let txs = vec![test_tx("0xAAAA", Some("0xBBBB"), "1000")];
        let events = extract_events(&tracked, 1, &txs, &test_block());

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].address, "0xaaaa");
    }

    #[test]
    fn test_extract_event_type_filtering() {
        let tracked = build_tracked(&[("0xaaaa", &["transaction:from"])]);
        let txs = vec![test_tx("0xbbbb", Some("0xaaaa"), "1000")];
        let events = extract_events(&tracked, 1, &txs, &test_block());

        assert!(events.is_empty());
    }

    #[test]
    fn test_extract_transfer_events_from_traces() {
        let tracked = build_tracked(&[
            ("0xaaaa", &["transfer:from"]),
            ("0xbbbb", &["transfer:to"]),
        ]);

        let traces = vec![crate::types::RawTraceRecord {
            chain_id: 1,
            block_number: 18000000,
            tx_index: 0,
            trace_address: "0.1".to_string(),
            tx_hash: "0xtx".to_string(),
            block_hash: "0xblock".to_string(),
            block_timestamp: 1700000000,
            call_type: "call".to_string(),
            from_address: "0xaaaa".to_string(),
            to_address: "0xbbbb".to_string(),
            input: "0x".to_string(),
            output: "0x".to_string(),
            value: "1000000000000000000".to_string(),
            gas: 100000,
            gas_used: 50000,
            error: None,
            function_name: None,
            function_sig: None,
        }];

        let events = extract_transfers(&tracked, 1, &traces, &test_block());

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, "transfer:from");
        assert_eq!(events[0].trace_address, Some("0.1".to_string()));
        assert_eq!(events[1].event_type, "transfer:to");
    }

    #[test]
    fn test_extract_transfer_events_zero_value_skipped() {
        let tracked = build_tracked(&[("0xaaaa", &["transfer:from"])]);

        let traces = vec![crate::types::RawTraceRecord {
            chain_id: 1,
            block_number: 100,
            tx_index: 0,
            trace_address: "".to_string(),
            tx_hash: "0x".to_string(),
            block_hash: "0x".to_string(),
            block_timestamp: 0,
            call_type: "call".to_string(),
            from_address: "0xaaaa".to_string(),
            to_address: "0xbbbb".to_string(),
            input: "0x".to_string(),
            output: "0x".to_string(),
            value: "0".to_string(),
            gas: 0,
            gas_used: 0,
            error: None,
            function_name: None,
            function_sig: None,
        }];

        let events = extract_transfers(&tracked, 1, &traces, &test_block());
        assert!(events.is_empty());
    }
}
