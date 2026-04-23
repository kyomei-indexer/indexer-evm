use super::BlockSource;
use crate::sync::retry::{classify_rpc_error, RpcErrorKind};
use crate::types::{BlockRange, BlockWithLogs, LogFilter};
use anyhow::Result;
use async_trait::async_trait;
use tracing::warn;

/// Wraps a primary `BlockSource` with a fallback RPC source.
///
/// On each call, tries the primary source first. If it fails (after the primary's
/// own internal retries are exhausted), logs the switch reason and delegates to
/// the fallback. The primary is always tried first on the next call so that
/// recovery is automatic when the primary comes back online.
///
/// Only created when `fallback_rpc` is configured in the YAML — otherwise the
/// primary source is used directly without this wrapper.
pub struct FallbackBlockSource {
    primary: Box<dyn BlockSource>,
    fallback: Box<dyn BlockSource>,
    chain_id: u32,
}

impl FallbackBlockSource {
    pub fn new(
        primary: Box<dyn BlockSource>,
        fallback: Box<dyn BlockSource>,
        chain_id: u32,
    ) -> Self {
        Self {
            primary,
            fallback,
            chain_id,
        }
    }

    /// Classify the error and return a human-readable reason for the switch.
    fn switch_reason(error: &anyhow::Error) -> &'static str {
        match classify_rpc_error(error) {
            RpcErrorKind::RateLimit => "rate limited",
            RpcErrorKind::RangeTooBig => "block range too large",
            RpcErrorKind::Transient => "request failed",
        }
    }
}

#[async_trait]
impl BlockSource for FallbackBlockSource {
    async fn get_blocks(
        &self,
        range: BlockRange,
        filter: &LogFilter,
    ) -> Result<Vec<BlockWithLogs>> {
        match self.primary.get_blocks(range.clone(), filter).await {
            Ok(blocks) => Ok(blocks),
            Err(primary_err) => {
                let reason = Self::switch_reason(&primary_err);
                warn!(
                    chain_id = self.chain_id,
                    primary = self.primary.source_type(),
                    fallback = self.fallback.source_type(),
                    reason,
                    range_from = range.from,
                    range_to = range.to,
                    error = %primary_err,
                    "Primary source failed, switching to fallback for get_blocks"
                );
                self.fallback.get_blocks(range, filter).await
            }
        }
    }

    async fn get_latest_block_number(&self) -> Result<u64> {
        match self.primary.get_latest_block_number().await {
            Ok(n) => Ok(n),
            Err(primary_err) => {
                let reason = Self::switch_reason(&primary_err);
                warn!(
                    chain_id = self.chain_id,
                    primary = self.primary.source_type(),
                    fallback = self.fallback.source_type(),
                    reason,
                    error = %primary_err,
                    "Primary source failed, switching to fallback for get_latest_block_number"
                );
                self.fallback.get_latest_block_number().await
            }
        }
    }

    async fn get_block_hash(&self, block_number: u64) -> Result<Option<String>> {
        match self.primary.get_block_hash(block_number).await {
            Ok(hash) => Ok(hash),
            Err(primary_err) => {
                let reason = Self::switch_reason(&primary_err);
                warn!(
                    chain_id = self.chain_id,
                    primary = self.primary.source_type(),
                    fallback = self.fallback.source_type(),
                    reason,
                    block_number,
                    error = %primary_err,
                    "Primary source failed, switching to fallback for get_block_hash"
                );
                self.fallback.get_block_hash(block_number).await
            }
        }
    }

    fn source_type(&self) -> &'static str {
        // Report the primary type — the wrapper is transparent
        self.primary.source_type()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{BlockInfo, BlockRange, BlockWithLogs, LogFilter};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    /// Mock source that can be configured to succeed or fail
    struct MockSource {
        name: &'static str,
        should_fail: bool,
        fail_with: Option<String>,
        call_count: Arc<AtomicU32>,
        blocks: Vec<BlockWithLogs>,
        latest_block: u64,
        block_hash: Option<String>,
    }

    impl MockSource {
        fn ok(name: &'static str) -> Self {
            Self {
                name,
                should_fail: false,
                fail_with: None,
                call_count: Arc::new(AtomicU32::new(0)),
                blocks: vec![BlockWithLogs {
                    block: BlockInfo {
                        number: 100,
                        hash: "0xhash".to_string(),
                        parent_hash: "0xparent".to_string(),
                        timestamp: 1000,
                    },
                    logs: vec![],
                }],
                latest_block: 100,
                block_hash: Some("0xhash".to_string()),
            }
        }

        fn failing(name: &'static str, error_msg: &str) -> Self {
            Self {
                name,
                should_fail: true,
                fail_with: Some(error_msg.to_string()),
                call_count: Arc::new(AtomicU32::new(0)),
                blocks: vec![],
                latest_block: 0,
                block_hash: None,
            }
        }

        fn calls(&self) -> u32 {
            self.call_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl BlockSource for MockSource {
        async fn get_blocks(
            &self,
            _range: BlockRange,
            _filter: &LogFilter,
        ) -> Result<Vec<BlockWithLogs>> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            if self.should_fail {
                anyhow::bail!("{}", self.fail_with.as_deref().unwrap_or("mock error"));
            }
            Ok(self.blocks.clone())
        }

        async fn get_latest_block_number(&self) -> Result<u64> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            if self.should_fail {
                anyhow::bail!("{}", self.fail_with.as_deref().unwrap_or("mock error"));
            }
            Ok(self.latest_block)
        }

        async fn get_block_hash(&self, _block_number: u64) -> Result<Option<String>> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            if self.should_fail {
                anyhow::bail!("{}", self.fail_with.as_deref().unwrap_or("mock error"));
            }
            Ok(self.block_hash.clone())
        }

        fn source_type(&self) -> &'static str {
            self.name
        }
    }

    fn test_filter() -> LogFilter {
        LogFilter {
            addresses: vec![],
            topics: vec![],
        }
    }

    fn test_range() -> BlockRange {
        BlockRange { from: 100, to: 100 }
    }

    // --- get_blocks ---

    #[tokio::test]
    async fn test_get_blocks_primary_succeeds() {
        let primary = MockSource::ok("primary");
        let fallback = MockSource::ok("fallback");
        let p_calls = primary.call_count.clone();
        let f_calls = fallback.call_count.clone();

        let source = FallbackBlockSource::new(
            Box::new(primary),
            Box::new(fallback),
            1,
        );

        let blocks = source.get_blocks(test_range(), &test_filter()).await.unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(p_calls.load(Ordering::SeqCst), 1);
        assert_eq!(f_calls.load(Ordering::SeqCst), 0); // fallback not called
    }

    #[tokio::test]
    async fn test_get_blocks_primary_fails_fallback_succeeds() {
        let primary = MockSource::failing("primary", "connection refused");
        let fallback = MockSource::ok("fallback");
        let p_calls = primary.call_count.clone();
        let f_calls = fallback.call_count.clone();

        let source = FallbackBlockSource::new(
            Box::new(primary),
            Box::new(fallback),
            1,
        );

        let blocks = source.get_blocks(test_range(), &test_filter()).await.unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(p_calls.load(Ordering::SeqCst), 1);
        assert_eq!(f_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_get_blocks_both_fail() {
        let primary = MockSource::failing("primary", "connection refused");
        let fallback = MockSource::failing("fallback", "fallback also down");

        let source = FallbackBlockSource::new(
            Box::new(primary),
            Box::new(fallback),
            1,
        );

        let result = source.get_blocks(test_range(), &test_filter()).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("fallback also down"));
    }

    // --- get_latest_block_number ---

    #[tokio::test]
    async fn test_get_latest_block_primary_succeeds() {
        let mut primary = MockSource::ok("primary");
        primary.latest_block = 500;
        let fallback = MockSource::ok("fallback");
        let f_calls = fallback.call_count.clone();

        let source = FallbackBlockSource::new(
            Box::new(primary),
            Box::new(fallback),
            1,
        );

        let latest = source.get_latest_block_number().await.unwrap();
        assert_eq!(latest, 500);
        assert_eq!(f_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_get_latest_block_primary_fails_fallback_succeeds() {
        let primary = MockSource::failing("primary", "HTTP 429 Too Many Requests");
        let mut fallback = MockSource::ok("fallback");
        fallback.latest_block = 300;

        let source = FallbackBlockSource::new(
            Box::new(primary),
            Box::new(fallback),
            1,
        );

        let latest = source.get_latest_block_number().await.unwrap();
        assert_eq!(latest, 300);
    }

    // --- get_block_hash ---

    #[tokio::test]
    async fn test_get_block_hash_primary_succeeds() {
        let mut primary = MockSource::ok("primary");
        primary.block_hash = Some("0xabc".to_string());
        let fallback = MockSource::ok("fallback");
        let f_calls = fallback.call_count.clone();

        let source = FallbackBlockSource::new(
            Box::new(primary),
            Box::new(fallback),
            1,
        );

        let hash = source.get_block_hash(100).await.unwrap();
        assert_eq!(hash, Some("0xabc".to_string()));
        assert_eq!(f_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_get_block_hash_primary_fails_fallback_succeeds() {
        let primary = MockSource::failing("primary", "request timed out");
        let mut fallback = MockSource::ok("fallback");
        fallback.block_hash = Some("0xdef".to_string());

        let source = FallbackBlockSource::new(
            Box::new(primary),
            Box::new(fallback),
            1,
        );

        let hash = source.get_block_hash(100).await.unwrap();
        assert_eq!(hash, Some("0xdef".to_string()));
    }

    // --- source_type ---

    #[tokio::test]
    async fn test_source_type_returns_primary() {
        let primary = MockSource::ok("rpc");
        let fallback = MockSource::ok("fallback-rpc");

        let source = FallbackBlockSource::new(
            Box::new(primary),
            Box::new(fallback),
            1,
        );

        assert_eq!(source.source_type(), "rpc");
    }

    // --- error classification in switch reason ---

    #[tokio::test]
    async fn test_switch_reason_rate_limit() {
        let primary = MockSource::failing("primary", "HTTP 429 Too Many Requests");
        let fallback = MockSource::ok("fallback");

        let source = FallbackBlockSource::new(
            Box::new(primary),
            Box::new(fallback),
            1,
        );

        // Just verify it doesn't panic and fallback works
        let result = source.get_latest_block_number().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_switch_reason_range_too_big() {
        let primary = MockSource::failing("primary", "exceed maximum block range: 2000");
        let fallback = MockSource::ok("fallback");

        let source = FallbackBlockSource::new(
            Box::new(primary),
            Box::new(fallback),
            1,
        );

        let result = source.get_blocks(test_range(), &test_filter()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_switch_reason_transient() {
        let primary = MockSource::failing("primary", "connection reset by peer");
        let fallback = MockSource::ok("fallback");

        let source = FallbackBlockSource::new(
            Box::new(primary),
            Box::new(fallback),
            1,
        );

        let result = source.get_block_hash(100).await;
        assert!(result.is_ok());
    }

    // --- primary always tried first ---

    #[tokio::test]
    async fn test_primary_always_tried_first_on_each_call() {
        let primary = MockSource::failing("primary", "down");
        let fallback = MockSource::ok("fallback");
        let p_calls = primary.call_count.clone();
        let f_calls = fallback.call_count.clone();

        let source = FallbackBlockSource::new(
            Box::new(primary),
            Box::new(fallback),
            1,
        );

        // Call twice — primary should be tried each time
        let _ = source.get_latest_block_number().await;
        let _ = source.get_latest_block_number().await;

        assert_eq!(p_calls.load(Ordering::SeqCst), 2);
        assert_eq!(f_calls.load(Ordering::SeqCst), 2);
    }
}
