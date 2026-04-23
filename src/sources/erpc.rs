use super::BlockSource;
use crate::config::RetryConfig;
use crate::types::{BlockRange, BlockWithLogs, LogFilter};
use anyhow::Result;
use async_trait::async_trait;
use tracing::debug;

/// Build the full eRPC endpoint URL with optional project routing and directives.
///
/// If `project_id` is provided: `{base_url}/{project_id}/evm/{chain_id}?directives`
/// Otherwise: `{base_url}?directives`
pub fn build_erpc_url(base_url: &str, chain_id: u32, project_id: Option<&str>) -> String {
    let base = base_url.trim_end_matches('/');
    let erpc_url = if let Some(pid) = project_id {
        format!("{}/{}/evm/{}", base, pid, chain_id)
    } else {
        base.to_string()
    };

    // Append eRPC directives as query parameters
    if erpc_url.contains('?') {
        format!(
            "{}&retry-empty=true&validate-logs-bloom-match=true",
            erpc_url
        )
    } else {
        format!(
            "{}?retry-empty=true&validate-logs-bloom-match=true",
            erpc_url
        )
    }
}

/// Build a reduced retry config appropriate for eRPC.
/// eRPC already retries 3x across upstreams with circuit breaker + hedge,
/// so we cap client-side retries at 2 to avoid quadratic retry latency.
pub fn build_erpc_retry_config(retry_config: &RetryConfig) -> RetryConfig {
    RetryConfig {
        max_retries: retry_config.max_retries.min(2),
        initial_backoff_ms: retry_config.initial_backoff_ms,
        max_backoff_ms: retry_config.max_backoff_ms,
        validate_logs_bloom: retry_config.validate_logs_bloom,
        bloom_validation_retries: retry_config.bloom_validation_retries,
    }
}

/// eRPC block source — builds on RpcBlockSource with eRPC-specific optimizations.
///
/// Key differences from raw RPC:
/// - Constructs proper eRPC URL: `{base_url}/{project_id}/evm/{chain_id}`
/// - Appends directives as query params (`retry-empty`, `validate-logs-bloom-match`)
/// - Uses reduced client-side retry since eRPC already provides:
///   - Automatic retry across multiple upstreams (default 3 attempts)
///   - Circuit breaker for failing upstreams
///   - Hedge policy for parallel requests
///   - Automatic eth_getLogs range splitting
///   - Rate limit budget management
pub struct ErpcBlockSource {
    inner: super::rpc::RpcBlockSource,
}

impl ErpcBlockSource {
    /// Create a new eRPC block source
    pub async fn new(url: &str, chain_id: u32, blocks_per_request: u64) -> Result<Self> {
        Self::with_retry_config(url, chain_id, blocks_per_request, RetryConfig::default(), None)
            .await
    }

    /// Create a new eRPC block source with custom retry configuration.
    ///
    /// If `project_id` is provided, constructs the full eRPC endpoint URL:
    /// `{url}/{project_id}/evm/{chain_id}?retry-empty=true`
    ///
    /// Otherwise uses the URL as-is (for pre-configured eRPC URLs that
    /// already include the project/network path).
    pub async fn with_retry_config(
        url: &str,
        chain_id: u32,
        blocks_per_request: u64,
        retry_config: RetryConfig,
        project_id: Option<&str>,
    ) -> Result<Self> {
        let erpc_url = build_erpc_url(url, chain_id, project_id);
        let erpc_retry = build_erpc_retry_config(&retry_config);

        debug!(
            url = %erpc_url,
            chain_id,
            project_id = project_id.unwrap_or("(none)"),
            max_client_retries = erpc_retry.max_retries,
            source = "erpc",
            "Connecting to eRPC source"
        );

        let inner = super::rpc::RpcBlockSource::with_retry_config(
            &erpc_url,
            chain_id,
            blocks_per_request,
            erpc_retry,
        )
        .await?;

        Ok(Self { inner })
    }
}

#[async_trait]
impl BlockSource for ErpcBlockSource {
    async fn get_blocks(
        &self,
        range: BlockRange,
        filter: &LogFilter,
    ) -> Result<Vec<BlockWithLogs>> {
        self.inner.get_blocks(range, filter).await
    }

    async fn get_latest_block_number(&self) -> Result<u64> {
        self.inner.get_latest_block_number().await
    }

    async fn get_block_hash(&self, block_number: u64) -> Result<Option<String>> {
        self.inner.get_block_hash(block_number).await
    }

    fn source_type(&self) -> &'static str {
        "erpc"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RetryConfig;

    // --- build_erpc_url tests ---

    #[test]
    fn test_build_erpc_url_with_project_id() {
        let url = build_erpc_url("http://erpc:4000", 1, Some("kyomei"));
        assert_eq!(
            url,
            "http://erpc:4000/kyomei/evm/1?retry-empty=true&validate-logs-bloom-match=true"
        );
    }

    #[test]
    fn test_build_erpc_url_without_project_id() {
        let url = build_erpc_url("http://erpc:4000/kyomei/evm/1", 1, None);
        assert_eq!(
            url,
            "http://erpc:4000/kyomei/evm/1?retry-empty=true&validate-logs-bloom-match=true"
        );
    }

    #[test]
    fn test_build_erpc_url_strips_trailing_slash() {
        let url = build_erpc_url("http://erpc:4000/", 42161, Some("myproject"));
        assert_eq!(
            url,
            "http://erpc:4000/myproject/evm/42161?retry-empty=true&validate-logs-bloom-match=true"
        );
    }

    #[test]
    fn test_build_erpc_url_with_existing_query_params() {
        let url = build_erpc_url("http://erpc:4000/kyomei/evm/1?skip-cache-read=true", 1, None);
        assert_eq!(
            url,
            "http://erpc:4000/kyomei/evm/1?skip-cache-read=true&retry-empty=true&validate-logs-bloom-match=true"
        );
    }

    #[test]
    fn test_build_erpc_url_different_chain_ids() {
        let eth = build_erpc_url("http://erpc:4000", 1, Some("p"));
        assert!(eth.contains("/p/evm/1?"));

        let polygon = build_erpc_url("http://erpc:4000", 137, Some("p"));
        assert!(polygon.contains("/p/evm/137?"));

        let arb = build_erpc_url("http://erpc:4000", 42161, Some("p"));
        assert!(arb.contains("/p/evm/42161?"));
    }

    #[test]
    fn test_build_erpc_url_always_has_directives() {
        let url = build_erpc_url("http://localhost:4000", 1, Some("test"));
        assert!(url.contains("retry-empty=true"));
        assert!(url.contains("validate-logs-bloom-match=true"));
    }

    // --- build_erpc_retry_config tests ---

    #[test]
    fn test_retry_config_caps_at_2() {
        let config = RetryConfig {
            max_retries: 5,
            initial_backoff_ms: 1000,
            max_backoff_ms: 30_000,
            validate_logs_bloom: true,
            bloom_validation_retries: 3,
        };
        let erpc = build_erpc_retry_config(&config);
        assert_eq!(erpc.max_retries, 2);
    }

    #[test]
    fn test_retry_config_keeps_lower_value() {
        let config = RetryConfig {
            max_retries: 1,
            initial_backoff_ms: 500,
            max_backoff_ms: 10_000,
            validate_logs_bloom: false,
            bloom_validation_retries: 1,
        };
        let erpc = build_erpc_retry_config(&config);
        assert_eq!(erpc.max_retries, 1);
    }

    #[test]
    fn test_retry_config_preserves_other_fields() {
        let config = RetryConfig {
            max_retries: 10,
            initial_backoff_ms: 2000,
            max_backoff_ms: 60_000,
            validate_logs_bloom: false,
            bloom_validation_retries: 5,
        };
        let erpc = build_erpc_retry_config(&config);
        assert_eq!(erpc.max_retries, 2);
        assert_eq!(erpc.initial_backoff_ms, 2000);
        assert_eq!(erpc.max_backoff_ms, 60_000);
        assert!(!erpc.validate_logs_bloom);
        assert_eq!(erpc.bloom_validation_retries, 5);
    }

    #[test]
    fn test_retry_config_zero_retries() {
        let config = RetryConfig {
            max_retries: 0,
            initial_backoff_ms: 1000,
            max_backoff_ms: 30_000,
            validate_logs_bloom: true,
            bloom_validation_retries: 3,
        };
        let erpc = build_erpc_retry_config(&config);
        assert_eq!(erpc.max_retries, 0);
    }

    #[test]
    fn test_retry_config_exactly_2() {
        let config = RetryConfig {
            max_retries: 2,
            initial_backoff_ms: 1000,
            max_backoff_ms: 30_000,
            validate_logs_bloom: true,
            bloom_validation_retries: 3,
        };
        let erpc = build_erpc_retry_config(&config);
        assert_eq!(erpc.max_retries, 2);
    }

    #[test]
    fn test_retry_config_default_gets_capped() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 5); // default is 5
        let erpc = build_erpc_retry_config(&config);
        assert_eq!(erpc.max_retries, 2); // capped to 2
    }
}
