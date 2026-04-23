use serde::{Deserialize, Serialize};

/// A raw blockchain event record stored in the database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawEventRecord {
    pub chain_id: i32,
    pub block_number: i64,
    pub tx_index: i32,
    pub log_index: i32,
    pub block_hash: String,
    pub block_timestamp: i64,
    pub tx_hash: String,
    pub address: String,
    pub topic0: Option<String>,
    pub topic1: Option<String>,
    pub topic2: Option<String>,
    pub topic3: Option<String>,
    pub data: String,
}

/// A factory-discovered child contract
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactoryChild {
    pub chain_id: i32,
    pub factory_address: String,
    pub child_address: String,
    pub contract_name: String,
    pub created_at_block: i64,
    pub created_at_tx_hash: String,
    pub created_at_log_index: i32,
    pub metadata: Option<String>,
    pub child_abi: Option<String>,
}

/// Sync worker state persisted in database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncWorker {
    pub chain_id: i32,
    pub worker_id: i32,
    pub range_start: i64,
    pub range_end: Option<i64>,
    pub current_block: i64,
    pub status: SyncWorkerStatus,
}

/// Sync worker status
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SyncWorkerStatus {
    Historical,
    Live,
}

impl std::fmt::Display for SyncWorkerStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncWorkerStatus::Historical => write!(f, "historical"),
            SyncWorkerStatus::Live => write!(f, "live"),
        }
    }
}

impl TryFrom<&str> for SyncWorkerStatus {
    type Error = anyhow::Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "historical" => Ok(SyncWorkerStatus::Historical),
            "live" => Ok(SyncWorkerStatus::Live),
            _ => Err(anyhow::anyhow!("Invalid sync worker status: {}", s)),
        }
    }
}

/// A block with its associated logs
#[derive(Debug, Clone)]
pub struct BlockWithLogs {
    pub block: BlockInfo,
    pub logs: Vec<LogEntry>,
}

/// Basic block information
#[derive(Debug, Clone)]
pub struct BlockInfo {
    pub number: u64,
    pub hash: String,
    pub parent_hash: String,
    pub timestamp: u64,
}

/// A single log entry from a block
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub block_number: u64,
    pub block_hash: String,
    pub block_timestamp: u64,
    pub transaction_hash: String,
    pub transaction_index: u32,
    pub log_index: u32,
    pub address: String,
    pub topic0: Option<String>,
    pub topic1: Option<String>,
    pub topic2: Option<String>,
    pub topic3: Option<String>,
    pub data: String,
}

/// Log filter for block source queries
#[derive(Debug, Clone, Default)]
pub struct LogFilter {
    pub addresses: Vec<String>,
    pub topics: Vec<Option<String>>,
}

impl LogFilter {
    /// Add an address to the filter, normalizing to lowercase for consistent matching.
    pub fn add_address(&mut self, addr: &str) {
        self.addresses.push(addr.to_lowercase());
    }
}

/// Block range for sync operations
#[derive(Debug, Clone, Copy)]
pub struct BlockRange {
    pub from: u64,
    pub to: u64,
}

/// Worker chunk assignment for parallel sync
#[derive(Debug, Clone)]
pub struct WorkerChunk {
    pub worker_id: i32,
    pub from_block: u64,
    pub to_block: u64,
}

/// Sync progress information
#[derive(Debug, Clone, Serialize)]
pub struct SyncProgress {
    pub chain_id: i32,
    pub chain_name: String,
    pub blocks_synced: u64,
    pub total_blocks: u64,
    pub percentage: f64,
    pub phase: SyncPhase,
    pub blocks_per_second: f64,
    pub workers: u32,
    pub events_stored: u64,
}

/// Current sync phase
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SyncPhase {
    Historical,
    Live,
}

/// Sync status response for the /sync endpoint
#[derive(Debug, Clone, Serialize)]
pub struct SyncStatus {
    pub synced: bool,
    pub current_block: u64,
    pub target_block: u64,
    pub percentage: f64,
    pub mode: SyncPhase,
}

// === Call Trace Types ===

/// A block with its call traces
#[derive(Debug, Clone)]
pub struct BlockWithTraces {
    pub block: BlockInfo,
    pub traces: Vec<CallTrace>,
}

/// A single call trace entry from a block
#[derive(Debug, Clone)]
pub struct CallTrace {
    pub block_number: u64,
    pub block_hash: String,
    pub block_timestamp: u64,
    pub tx_hash: String,
    pub tx_index: u32,
    /// Position in the call tree, e.g. [0, 1, 3]
    pub trace_address: Vec<u32>,
    /// "call", "delegatecall", "staticcall", "create", "create2"
    pub call_type: String,
    pub from_address: String,
    pub to_address: String,
    /// Calldata hex
    pub input: String,
    /// Return data hex
    pub output: String,
    /// Wei transferred (decimal string for NUMERIC)
    pub value: String,
    pub gas: u64,
    pub gas_used: u64,
    /// Revert reason if failed
    pub error: Option<String>,
}

/// A raw call trace record for DB storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawTraceRecord {
    pub chain_id: i32,
    pub block_number: i64,
    pub tx_index: i32,
    /// Dotted notation: "0.1.3"
    pub trace_address: String,
    pub tx_hash: String,
    pub block_hash: String,
    pub block_timestamp: i64,
    pub call_type: String,
    pub from_address: String,
    pub to_address: String,
    pub input: String,
    pub output: String,
    pub value: String,
    pub gas: i64,
    pub gas_used: i64,
    pub error: Option<String>,
    pub function_name: Option<String>,
    pub function_sig: Option<String>,
}

impl CallTrace {
    /// Convert to a RawTraceRecord for database storage
    pub fn to_raw_record(&self, chain_id: i32) -> RawTraceRecord {
        RawTraceRecord {
            chain_id,
            block_number: self.block_number as i64,
            tx_index: self.tx_index as i32,
            trace_address: self
                .trace_address
                .iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join("."),
            tx_hash: self.tx_hash.clone(),
            block_hash: self.block_hash.clone(),
            block_timestamp: self.block_timestamp as i64,
            call_type: self.call_type.clone(),
            from_address: self.from_address.to_lowercase(),
            to_address: self.to_address.to_lowercase(),
            input: self.input.clone(),
            output: self.output.clone(),
            value: self.value.clone(),
            gas: self.gas as i64,
            gas_used: self.gas_used as i64,
            error: self.error.clone(),
            function_name: None,
            function_sig: if self.input.len() >= 10 {
                Some(self.input[..10].to_string())
            } else {
                None
            },
        }
    }
}

// === Account/Transaction Types ===

/// A block with its transactions
#[derive(Debug, Clone)]
pub struct BlockWithTransactions {
    pub block: BlockInfo,
    pub transactions: Vec<TransactionInfo>,
}

/// A single transaction from a block
#[derive(Debug, Clone)]
pub struct TransactionInfo {
    pub block_number: u64,
    pub block_hash: String,
    pub block_timestamp: u64,
    pub tx_hash: String,
    pub tx_index: u32,
    pub from_address: String,
    /// None for contract creation
    pub to_address: Option<String>,
    /// Wei amount (decimal string)
    pub value: String,
    pub input: String,
    pub gas_price: String,
    pub gas_used: Option<u64>,
    pub status: Option<bool>,
    pub nonce: u64,
}

/// A raw account event record for DB storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawAccountEventRecord {
    pub chain_id: i32,
    pub block_number: i64,
    pub tx_index: i32,
    pub tx_hash: String,
    pub block_hash: String,
    pub block_timestamp: i64,
    /// "transaction:from", "transaction:to", "transfer:from", "transfer:to"
    pub event_type: String,
    /// The tracked address
    pub address: String,
    /// The other party in the transaction/transfer
    pub counterparty: String,
    /// Wei amount (decimal string for NUMERIC)
    pub value: String,
    /// Transaction calldata (for transaction events)
    pub input: Option<String>,
    /// For internal transfers (from call traces)
    pub trace_address: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // === SyncWorkerStatus tests ===

    #[test]
    fn test_sync_worker_status_display_historical() {
        assert_eq!(SyncWorkerStatus::Historical.to_string(), "historical");
    }

    #[test]
    fn test_sync_worker_status_display_live() {
        assert_eq!(SyncWorkerStatus::Live.to_string(), "live");
    }

    #[test]
    fn test_sync_worker_status_try_from_historical() {
        let status = SyncWorkerStatus::try_from("historical").unwrap();
        assert_eq!(status, SyncWorkerStatus::Historical);
    }

    #[test]
    fn test_sync_worker_status_try_from_live() {
        let status = SyncWorkerStatus::try_from("live").unwrap();
        assert_eq!(status, SyncWorkerStatus::Live);
    }

    #[test]
    fn test_sync_worker_status_try_from_invalid() {
        let result = SyncWorkerStatus::try_from("invalid");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid sync worker status"));
    }

    #[test]
    fn test_sync_worker_status_try_from_empty() {
        let result = SyncWorkerStatus::try_from("");
        assert!(result.is_err());
    }

    #[test]
    fn test_sync_worker_status_try_from_case_sensitive() {
        // Should fail for uppercase variants
        assert!(SyncWorkerStatus::try_from("Historical").is_err());
        assert!(SyncWorkerStatus::try_from("LIVE").is_err());
        assert!(SyncWorkerStatus::try_from("Live").is_err());
    }

    #[test]
    fn test_sync_worker_status_equality() {
        assert_eq!(SyncWorkerStatus::Historical, SyncWorkerStatus::Historical);
        assert_eq!(SyncWorkerStatus::Live, SyncWorkerStatus::Live);
        assert_ne!(SyncWorkerStatus::Historical, SyncWorkerStatus::Live);
    }

    // === Serialization tests ===

    #[test]
    fn test_sync_worker_status_serialize_json() {
        let status = SyncWorkerStatus::Historical;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"historical\"");

        let status = SyncWorkerStatus::Live;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"live\"");
    }

    #[test]
    fn test_sync_worker_status_deserialize_json() {
        let status: SyncWorkerStatus = serde_json::from_str("\"historical\"").unwrap();
        assert_eq!(status, SyncWorkerStatus::Historical);

        let status: SyncWorkerStatus = serde_json::from_str("\"live\"").unwrap();
        assert_eq!(status, SyncWorkerStatus::Live);
    }

    #[test]
    fn test_raw_event_record_serialization_roundtrip() {
        let event = RawEventRecord {
            chain_id: 1,
            block_number: 18000000,
            tx_index: 42,
            log_index: 7,
            block_hash: "0xabc123".to_string(),
            block_timestamp: 1700000000,
            tx_hash: "0xdef456".to_string(),
            address: "0x1234567890abcdef1234567890abcdef12345678".to_string(),
            topic0: Some("0xtopic0".to_string()),
            topic1: None,
            topic2: None,
            topic3: None,
            data: "0x".to_string(),
        };
        let json = serde_json::to_string(&event).unwrap();
        let deserialized: RawEventRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.chain_id, 1);
        assert_eq!(deserialized.block_number, 18000000);
        assert_eq!(deserialized.topic0, Some("0xtopic0".to_string()));
        assert!(deserialized.topic1.is_none());
    }

    #[test]
    fn test_factory_child_serialization_roundtrip() {
        let child = FactoryChild {
            chain_id: 1,
            factory_address: "0xfactory".to_string(),
            child_address: "0xchild".to_string(),
            contract_name: "UniswapV2Pair".to_string(),
            created_at_block: 10000835,
            created_at_tx_hash: "0xtxhash".to_string(),
            created_at_log_index: 3,
            metadata: Some("{\"token0\": \"WETH\"}".to_string()),
            child_abi: None,
        };
        let json = serde_json::to_string(&child).unwrap();
        let deserialized: FactoryChild = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.contract_name, "UniswapV2Pair");
        assert_eq!(deserialized.metadata, Some("{\"token0\": \"WETH\"}".to_string()));
        assert!(deserialized.child_abi.is_none());
    }

    #[test]
    fn test_sync_worker_serialization() {
        let worker = SyncWorker {
            chain_id: 1,
            worker_id: 0,
            range_start: 10000000,
            range_end: None,
            current_block: 18000000,
            status: SyncWorkerStatus::Live,
        };
        let json = serde_json::to_string(&worker).unwrap();
        assert!(json.contains("\"live\""));
        let deserialized: SyncWorker = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.status, SyncWorkerStatus::Live);
        assert!(deserialized.range_end.is_none());
    }

    // === Struct construction and field access ===

    #[test]
    fn test_block_range_copy_semantics() {
        let range = BlockRange { from: 100, to: 200 };
        let copy = range; // Copy trait
        assert_eq!(copy.from, 100);
        assert_eq!(copy.to, 200);
        // Original still accessible (Copy)
        assert_eq!(range.from, 100);
    }

    #[test]
    fn test_log_filter_default() {
        let filter = LogFilter::default();
        assert!(filter.addresses.is_empty());
        assert!(filter.topics.is_empty());
    }

    #[test]
    fn test_sync_progress_serialization() {
        let progress = SyncProgress {
            chain_id: 1,
            chain_name: "ethereum".to_string(),
            blocks_synced: 500000,
            total_blocks: 1000000,
            percentage: 50.0,
            phase: SyncPhase::Historical,
            blocks_per_second: 1234.5,
            workers: 4,
            events_stored: 999999,
        };
        let json = serde_json::to_string(&progress).unwrap();
        assert!(json.contains("\"historical\""));
        assert!(json.contains("\"ethereum\""));
    }

    #[test]
    fn test_sync_status_serialization() {
        let status = SyncStatus {
            synced: true,
            current_block: 18000000,
            target_block: 18000000,
            percentage: 100.0,
            mode: SyncPhase::Live,
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("\"synced\":true"));
        assert!(json.contains("\"mode\":\"live\""));
    }

    #[test]
    fn test_worker_chunk_construction() {
        let chunk = WorkerChunk {
            worker_id: 1,
            from_block: 0,
            to_block: 249999,
        };
        assert_eq!(chunk.worker_id, 1);
        assert_eq!(chunk.to_block - chunk.from_block + 1, 250000);
    }

    #[test]
    fn test_block_info_construction() {
        let block = BlockInfo {
            number: 18000000,
            hash: "0xabc".to_string(),
            parent_hash: "0xdef".to_string(),
            timestamp: 1700000000,
        };
        assert_eq!(block.number, 18000000);
        assert_eq!(block.parent_hash, "0xdef");
    }

    #[test]
    fn test_log_entry_all_topics_populated() {
        let log = LogEntry {
            block_number: 100,
            block_hash: "0xhash".to_string(),
            block_timestamp: 1000,
            transaction_hash: "0xtx".to_string(),
            transaction_index: 5,
            log_index: 2,
            address: "0xaddr".to_string(),
            topic0: Some("0xt0".to_string()),
            topic1: Some("0xt1".to_string()),
            topic2: Some("0xt2".to_string()),
            topic3: Some("0xt3".to_string()),
            data: "0xdata".to_string(),
        };
        assert!(log.topic0.is_some());
        assert!(log.topic1.is_some());
        assert!(log.topic2.is_some());
        assert!(log.topic3.is_some());
    }

    #[test]
    fn test_log_filter_add_address_normalizes_to_lowercase() {
        let mut filter = LogFilter::default();
        filter.add_address("0xAbCdEf1234567890AbCdEf1234567890AbCdEf12");
        assert_eq!(filter.addresses.len(), 1);
        assert_eq!(filter.addresses[0], "0xabcdef1234567890abcdef1234567890abcdef12");
    }

    #[test]
    fn test_log_filter_add_address_already_lowercase() {
        let mut filter = LogFilter::default();
        filter.add_address("0xabcdef");
        assert_eq!(filter.addresses[0], "0xabcdef");
    }

    #[test]
    fn test_log_filter_add_address_multiple() {
        let mut filter = LogFilter::default();
        filter.add_address("0xABC");
        filter.add_address("0xDEF");
        assert_eq!(filter.addresses.len(), 2);
        assert_eq!(filter.addresses[0], "0xabc");
        assert_eq!(filter.addresses[1], "0xdef");
    }

    // === Call Trace tests ===

    #[test]
    fn test_call_trace_to_raw_record() {
        let trace = CallTrace {
            block_number: 18000000,
            block_hash: "0xabc".to_string(),
            block_timestamp: 1700000000,
            tx_hash: "0xdef".to_string(),
            tx_index: 5,
            trace_address: vec![0, 1, 3],
            call_type: "call".to_string(),
            from_address: "0xAAAA".to_string(),
            to_address: "0xBBBB".to_string(),
            input: "0x38ed173900000000".to_string(),
            output: "0x".to_string(),
            value: "1000000000000000000".to_string(),
            gas: 100000,
            gas_used: 50000,
            error: None,
        };

        let raw = trace.to_raw_record(1);
        assert_eq!(raw.chain_id, 1);
        assert_eq!(raw.block_number, 18000000);
        assert_eq!(raw.trace_address, "0.1.3");
        assert_eq!(raw.from_address, "0xaaaa"); // lowercased
        assert_eq!(raw.to_address, "0xbbbb"); // lowercased
        assert_eq!(raw.function_sig, Some("0x38ed1739".to_string()));
        assert!(raw.error.is_none());
        assert!(raw.function_name.is_none());
    }

    #[test]
    fn test_call_trace_to_raw_record_empty_trace_address() {
        let trace = CallTrace {
            block_number: 100,
            block_hash: "0x".to_string(),
            block_timestamp: 0,
            tx_hash: "0x".to_string(),
            tx_index: 0,
            trace_address: vec![],
            call_type: "call".to_string(),
            from_address: "0xa".to_string(),
            to_address: "0xb".to_string(),
            input: "0x1234".to_string(), // short input, no 4-byte selector
            output: "0x".to_string(),
            value: "0".to_string(),
            gas: 0,
            gas_used: 0,
            error: Some("revert".to_string()),
        };

        let raw = trace.to_raw_record(1);
        assert_eq!(raw.trace_address, ""); // empty vec → empty string
        assert!(raw.error.is_some());
        assert_eq!(raw.function_sig, None); // input too short for selector
    }

    #[test]
    fn test_raw_trace_record_serialization() {
        let record = RawTraceRecord {
            chain_id: 1,
            block_number: 18000000,
            tx_index: 5,
            trace_address: "0.1".to_string(),
            tx_hash: "0xdef".to_string(),
            block_hash: "0xabc".to_string(),
            block_timestamp: 1700000000,
            call_type: "call".to_string(),
            from_address: "0xaaaa".to_string(),
            to_address: "0xbbbb".to_string(),
            input: "0x38ed1739".to_string(),
            output: "0x".to_string(),
            value: "0".to_string(),
            gas: 100000,
            gas_used: 50000,
            error: None,
            function_name: Some("swap".to_string()),
            function_sig: Some("0x38ed1739".to_string()),
        };
        let json = serde_json::to_string(&record).unwrap();
        let deser: RawTraceRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.trace_address, "0.1");
        assert_eq!(deser.function_name, Some("swap".to_string()));
    }

    // === Account Event tests ===

    #[test]
    fn test_raw_account_event_record_serialization() {
        let record = RawAccountEventRecord {
            chain_id: 1,
            block_number: 18000000,
            tx_index: 5,
            tx_hash: "0xdef".to_string(),
            block_hash: "0xabc".to_string(),
            block_timestamp: 1700000000,
            event_type: "transaction:from".to_string(),
            address: "0xaaaa".to_string(),
            counterparty: "0xbbbb".to_string(),
            value: "1000000000000000000".to_string(),
            input: Some("0x38ed1739".to_string()),
            trace_address: None,
        };
        let json = serde_json::to_string(&record).unwrap();
        let deser: RawAccountEventRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.event_type, "transaction:from");
        assert_eq!(deser.address, "0xaaaa");
        assert!(deser.trace_address.is_none());
    }

    #[test]
    fn test_raw_account_event_transfer_with_trace() {
        let record = RawAccountEventRecord {
            chain_id: 1,
            block_number: 100,
            tx_index: 0,
            tx_hash: "0x1".to_string(),
            block_hash: "0x2".to_string(),
            block_timestamp: 1000,
            event_type: "transfer:from".to_string(),
            address: "0xaaaa".to_string(),
            counterparty: "0xbbbb".to_string(),
            value: "500".to_string(),
            input: None,
            trace_address: Some("0.1".to_string()),
        };
        assert_eq!(record.event_type, "transfer:from");
        assert!(record.input.is_none());
        assert_eq!(record.trace_address, Some("0.1".to_string()));
    }

    #[test]
    fn test_block_with_traces_construction() {
        let block = BlockWithTraces {
            block: BlockInfo {
                number: 100,
                hash: "0x".to_string(),
                parent_hash: "0x".to_string(),
                timestamp: 1000,
            },
            traces: vec![],
        };
        assert!(block.traces.is_empty());
    }

    #[test]
    fn test_block_with_transactions_construction() {
        let block = BlockWithTransactions {
            block: BlockInfo {
                number: 100,
                hash: "0x".to_string(),
                parent_hash: "0x".to_string(),
                timestamp: 1000,
            },
            transactions: vec![],
        };
        assert!(block.transactions.is_empty());
    }

    #[test]
    fn test_transaction_info_contract_creation() {
        let tx = TransactionInfo {
            block_number: 100,
            block_hash: "0xhash".to_string(),
            block_timestamp: 1000,
            tx_hash: "0xtx".to_string(),
            tx_index: 0,
            from_address: "0xsender".to_string(),
            to_address: None, // contract creation
            value: "0".to_string(),
            input: "0x6060604052".to_string(),
            gas_price: "20000000000".to_string(),
            gas_used: Some(500000),
            status: Some(true),
            nonce: 42,
        };
        assert!(tx.to_address.is_none());
        assert_eq!(tx.nonce, 42);
        assert_eq!(tx.status, Some(true));
    }

    #[test]
    fn test_block_with_logs_empty_logs() {
        let block = BlockWithLogs {
            block: BlockInfo {
                number: 1,
                hash: "0x".to_string(),
                parent_hash: "0x".to_string(),
                timestamp: 0,
            },
            logs: vec![],
        };
        assert!(block.logs.is_empty());
        assert_eq!(block.block.number, 1);
    }
}
