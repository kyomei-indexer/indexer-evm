use crate::types::{BlockRange, BlockWithLogs, BlockWithTraces, BlockWithTransactions, LogFilter};
use anyhow::Result;
use async_trait::async_trait;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::debug;

/// Dynamically adjusts block range based on source responses.
/// Shrinks on errors (range too big, payload too large), grows on successes.
pub struct AdaptiveRange {
    current: AtomicU64,
    min: u64,
    max: u64,
}

impl AdaptiveRange {
    pub fn new(initial: u64) -> Self {
        Self {
            current: AtomicU64::new(initial),
            min: 10,
            max: initial,
        }
    }

    pub fn get(&self) -> u64 {
        self.current.load(Ordering::Relaxed)
    }

    /// Halve the range (on error), clamped to min.
    /// Uses compare-exchange to avoid lost updates from concurrent workers.
    pub fn shrink(&self) {
        let _ = self.current.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            let new = (current / 2).max(self.min);
            if new != current { Some(new) } else { None }
        });
    }

    /// Grow by 5% (on success), clamped to max.
    /// Uses compare-exchange to avoid lost updates from concurrent workers.
    pub fn grow(&self) {
        let _ = self.current.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            let new = ((current as f64 * 1.05) as u64).min(self.max);
            if new != current { Some(new) } else { None }
        });
    }

    /// Set to a specific value (from RPC error hint), clamped to [min, max]
    pub fn set(&self, size: u64) {
        let clamped = size.max(self.min).min(self.max);
        self.current.store(clamped, Ordering::Relaxed);
        debug!(size = clamped, "Adaptive range set from RPC hint");
    }
}

/// Trait for block data sources (RPC, HyperSync, eRPC)
#[async_trait]
pub trait BlockSource: Send + Sync {
    /// Get blocks with logs matching the filter for a given range
    async fn get_blocks(
        &self,
        range: BlockRange,
        filter: &LogFilter,
    ) -> Result<Vec<BlockWithLogs>>;

    /// Get the latest block number from the chain
    async fn get_latest_block_number(&self) -> Result<u64>;

    /// Get a single block's info (for reorg detection)
    async fn get_block_hash(&self, block_number: u64) -> Result<Option<String>>;

    /// Source type name for logging
    fn source_type(&self) -> &'static str;
}

/// Trait for fetching call traces from blocks.
/// Separate from BlockSource because not all RPC providers support trace methods.
#[async_trait]
pub trait TraceSource: Send + Sync {
    /// Fetch call traces for a block range, optionally filtered to specific addresses.
    async fn get_block_traces(
        &self,
        range: BlockRange,
        addresses: &[String],
    ) -> Result<Vec<BlockWithTraces>>;

    /// The trace method used (e.g. "debug" or "parity")
    fn trace_method(&self) -> &str;
}

/// Trait for fetching full transactions from blocks.
/// Used for account/transaction indexing.
#[async_trait]
pub trait TransactionSource: Send + Sync {
    /// Fetch blocks with full transaction data for a given range.
    async fn get_block_transactions(
        &self,
        range: BlockRange,
    ) -> Result<Vec<BlockWithTransactions>>;
}

pub mod concurrency;
pub mod rpc;
pub mod erpc;
pub mod fallback;
pub mod hypersync;
pub mod traces;
pub mod transactions;
