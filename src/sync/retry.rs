use crate::config::RetryConfig;
use std::time::Duration;
use tracing::{debug, warn};

/// Calculates exponential backoff delay for a given attempt.
///
/// Uses the formula: min(initial_backoff * 2^attempt, max_backoff) with jitter.
pub fn backoff_delay(config: &RetryConfig, attempt: u32) -> Duration {
    let base_ms = config.initial_backoff_ms as f64 * 2.0f64.powi(attempt as i32);
    let clamped_ms = base_ms.min(config.max_backoff_ms as f64);

    // Add ~25% jitter to prevent thundering herd
    let jitter_factor = 1.0 + (simple_hash(attempt) as f64 / u32::MAX as f64) * 0.25;
    let final_ms = (clamped_ms * jitter_factor) as u64;

    Duration::from_millis(final_ms.min(config.max_backoff_ms))
}

/// Simple deterministic hash for jitter (avoids requiring rand dependency)
fn simple_hash(value: u32) -> u32 {
    let mut h = value;
    h = h.wrapping_mul(2654435761);
    h ^= h >> 16;
    h
}

/// Execute an async operation with exponential backoff retry.
///
/// Returns Ok(result) on success, or the last error after all retries are exhausted.
pub async fn with_retry<F, Fut, T, E>(
    config: &RetryConfig,
    operation_name: &str,
    mut operation: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut last_error = None;

    for attempt in 0..=config.max_retries {
        match operation().await {
            Ok(result) => {
                if attempt > 0 {
                    debug!(
                        operation = operation_name,
                        attempt,
                        "Operation succeeded after retry"
                    );
                }
                return Ok(result);
            }
            Err(e) => {
                if attempt < config.max_retries {
                    let delay = backoff_delay(config, attempt);
                    warn!(
                        operation = operation_name,
                        attempt = attempt + 1,
                        max_retries = config.max_retries,
                        backoff_ms = delay.as_millis() as u64,
                        error = %e,
                        "Operation failed, retrying with backoff"
                    );
                    tokio::time::sleep(delay).await;
                } else {
                    warn!(
                        operation = operation_name,
                        total_attempts = attempt + 1,
                        error = %e,
                        "Operation failed after all retry attempts exhausted"
                    );
                }
                last_error = Some(e);
            }
        }
    }

    Err(last_error.unwrap())
}

/// Classification of RPC errors to determine retry strategy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RpcErrorKind {
    /// HTTP 429 or rate limit response — use longer backoff, don't count toward consecutive errors
    RateLimit,
    /// RPC rejected due to block range too large — shrink adaptive range and retry
    RangeTooBig,
    /// Transient network/RPC error — use standard exponential backoff
    Transient,
}

/// Classify an RPC error to determine the appropriate retry strategy.
pub fn classify_rpc_error(error: &anyhow::Error) -> RpcErrorKind {
    let msg = error.to_string().to_lowercase();
    if msg.contains("429")
        || msg.contains("too many requests")
        || msg.contains("rate limit")
        || msg.contains("throttl")
    {
        RpcErrorKind::RateLimit
    } else if msg.contains("block range")
        || msg.contains("too many results")
        || msg.contains("response size exceeded")
        || msg.contains("query returned more than")
        || msg.contains("exceed maximum")
        || msg.contains("payload too large")
        || msg.contains("length limit exceeded")
    {
        RpcErrorKind::RangeTooBig
    } else {
        RpcErrorKind::Transient
    }
}

/// Block validation errors detected during sync
#[derive(Debug, Clone)]
pub enum BlockValidationError {
    /// Block's logsBloom is non-empty but eth_getLogs returned zero logs
    InvalidLogsBloom {
        block_number: u64,
        block_hash: String,
    },
    /// Expected block was missing from the RPC response
    MissingBlock {
        block_number: u64,
    },
    /// Block header could not be fetched
    BlockHeaderUnavailable {
        block_number: u64,
    },
    /// Log's block_hash does not match the fetched block header hash
    LogBlockMismatch {
        block_number: u64,
        log_block_hash: String,
        header_block_hash: String,
    },
    /// Log has a block_number outside the requested range
    LogOutOfRange {
        log_block_number: u64,
        range_from: u64,
        range_to: u64,
    },
}

impl std::fmt::Display for BlockValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlockValidationError::InvalidLogsBloom { block_number, block_hash } => {
                write!(
                    f,
                    "Detected invalid eth_getLogs response at block {}. \
                     block.logsBloom is not empty but zero logs were returned. \
                     block_hash={}",
                    block_number, block_hash
                )
            }
            BlockValidationError::MissingBlock { block_number } => {
                write!(f, "Block {} is missing from RPC response", block_number)
            }
            BlockValidationError::BlockHeaderUnavailable { block_number } => {
                write!(
                    f,
                    "Block header unavailable for block {} (RPC returned null)",
                    block_number
                )
            }
            BlockValidationError::LogBlockMismatch {
                block_number,
                log_block_hash,
                header_block_hash,
            } => {
                write!(
                    f,
                    "Log block_hash mismatch at block {}: log has {} but header has {}",
                    block_number, log_block_hash, header_block_hash
                )
            }
            BlockValidationError::LogOutOfRange {
                log_block_number,
                range_from,
                range_to,
            } => {
                write!(
                    f,
                    "Log has block_number {} outside requested range [{}, {}]",
                    log_block_number, range_from, range_to
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_delay_first_attempt() {
        let config = RetryConfig {
            max_retries: 5,
            initial_backoff_ms: 1000,
            max_backoff_ms: 30_000,
            validate_logs_bloom: true,
            bloom_validation_retries: 3,
        };
        let delay = backoff_delay(&config, 0);
        // First attempt: ~1000ms * 2^0 = ~1000ms + jitter
        assert!(delay.as_millis() >= 1000);
        assert!(delay.as_millis() <= 1500);
    }

    #[test]
    fn test_backoff_delay_exponential_growth() {
        let config = RetryConfig {
            max_retries: 5,
            initial_backoff_ms: 1000,
            max_backoff_ms: 60_000,
            validate_logs_bloom: true,
            bloom_validation_retries: 3,
        };

        let d0 = backoff_delay(&config, 0);
        let d1 = backoff_delay(&config, 1);
        let d2 = backoff_delay(&config, 2);

        // Each attempt should be roughly double the previous (ignoring jitter)
        assert!(d1.as_millis() > d0.as_millis());
        assert!(d2.as_millis() > d1.as_millis());
    }

    #[test]
    fn test_backoff_delay_capped_at_max() {
        let config = RetryConfig {
            max_retries: 10,
            initial_backoff_ms: 1000,
            max_backoff_ms: 5_000,
            validate_logs_bloom: true,
            bloom_validation_retries: 3,
        };
        let delay = backoff_delay(&config, 20);
        assert!(delay.as_millis() <= 5_000);
    }

    #[test]
    fn test_backoff_delay_small_initial() {
        let config = RetryConfig {
            max_retries: 3,
            initial_backoff_ms: 100,
            max_backoff_ms: 10_000,
            validate_logs_bloom: true,
            bloom_validation_retries: 3,
        };
        let delay = backoff_delay(&config, 0);
        assert!(delay.as_millis() >= 100);
        assert!(delay.as_millis() <= 200);
    }

    #[test]
    fn test_block_validation_error_display_invalid_bloom() {
        let err = BlockValidationError::InvalidLogsBloom {
            block_number: 18000000,
            block_hash: "0xabc123".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("18000000"));
        assert!(msg.contains("logsBloom"));
        assert!(msg.contains("0xabc123"));
    }

    #[test]
    fn test_block_validation_error_display_missing_block() {
        let err = BlockValidationError::MissingBlock {
            block_number: 18000001,
        };
        let msg = err.to_string();
        assert!(msg.contains("18000001"));
        assert!(msg.contains("missing"));
    }

    #[test]
    fn test_block_validation_error_display_header_unavailable() {
        let err = BlockValidationError::BlockHeaderUnavailable {
            block_number: 18000002,
        };
        let msg = err.to_string();
        assert!(msg.contains("18000002"));
        assert!(msg.contains("unavailable"));
    }

    #[test]
    fn test_classify_rpc_error_rate_limit_429() {
        let err = anyhow::anyhow!("HTTP 429 Too Many Requests");
        assert_eq!(classify_rpc_error(&err), RpcErrorKind::RateLimit);
    }

    #[test]
    fn test_classify_rpc_error_rate_limit_text() {
        let err = anyhow::anyhow!("rate limit exceeded for this endpoint");
        assert_eq!(classify_rpc_error(&err), RpcErrorKind::RateLimit);
    }

    #[test]
    fn test_classify_rpc_error_throttled() {
        let err = anyhow::anyhow!("request throttled by provider");
        assert_eq!(classify_rpc_error(&err), RpcErrorKind::RateLimit);
    }

    #[test]
    fn test_classify_rpc_error_range_too_big() {
        let err = anyhow::anyhow!("exceed maximum block range: 2000");
        assert_eq!(classify_rpc_error(&err), RpcErrorKind::RangeTooBig);
    }

    #[test]
    fn test_classify_rpc_error_too_many_results() {
        let err = anyhow::anyhow!("query returned more than 10000 results");
        assert_eq!(classify_rpc_error(&err), RpcErrorKind::RangeTooBig);
    }

    #[test]
    fn test_classify_rpc_error_response_size() {
        let err = anyhow::anyhow!("Log response size exceeded");
        assert_eq!(classify_rpc_error(&err), RpcErrorKind::RangeTooBig);
    }

    #[test]
    fn test_classify_rpc_error_transient() {
        let err = anyhow::anyhow!("connection refused");
        assert_eq!(classify_rpc_error(&err), RpcErrorKind::Transient);
    }

    #[test]
    fn test_classify_rpc_error_timeout() {
        let err = anyhow::anyhow!("request timed out after 30s");
        assert_eq!(classify_rpc_error(&err), RpcErrorKind::Transient);
    }

    #[test]
    fn test_block_validation_error_display_log_mismatch() {
        let err = BlockValidationError::LogBlockMismatch {
            block_number: 100,
            log_block_hash: "0xaaa".to_string(),
            header_block_hash: "0xbbb".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("100"));
        assert!(msg.contains("0xaaa"));
        assert!(msg.contains("0xbbb"));
    }

    #[test]
    fn test_block_validation_error_display_out_of_range() {
        let err = BlockValidationError::LogOutOfRange {
            log_block_number: 50,
            range_from: 100,
            range_to: 200,
        };
        let msg = err.to_string();
        assert!(msg.contains("50"));
        assert!(msg.contains("100"));
        assert!(msg.contains("200"));
    }

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.initial_backoff_ms, 1000);
        assert_eq!(config.max_backoff_ms, 30_000);
        assert!(config.validate_logs_bloom);
        assert_eq!(config.bloom_validation_retries, 3);
    }

    #[test]
    fn test_simple_hash_deterministic() {
        // Same input should produce same output
        assert_eq!(simple_hash(0), simple_hash(0));
        assert_eq!(simple_hash(42), simple_hash(42));
    }

    #[test]
    fn test_simple_hash_different_inputs() {
        // Different inputs should produce different outputs
        assert_ne!(simple_hash(0), simple_hash(1));
        assert_ne!(simple_hash(1), simple_hash(2));
    }

    #[tokio::test]
    async fn test_with_retry_succeeds_first_try() {
        let config = RetryConfig {
            max_retries: 3,
            initial_backoff_ms: 10,
            max_backoff_ms: 100,
            validate_logs_bloom: true,
            bloom_validation_retries: 3,
        };

        let result = with_retry(&config, "test_op", || async {
            Ok::<_, String>(42)
        })
        .await;

        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_with_retry_succeeds_after_failures() {
        let config = RetryConfig {
            max_retries: 3,
            initial_backoff_ms: 10,
            max_backoff_ms: 50,
            validate_logs_bloom: true,
            bloom_validation_retries: 3,
        };

        let attempt = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let attempt_clone = attempt.clone();

        let result = with_retry(&config, "test_op", || {
            let attempt = attempt_clone.clone();
            async move {
                let n = attempt.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if n < 2 {
                    Err(format!("attempt {} failed", n))
                } else {
                    Ok(99)
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), 99);
        assert_eq!(attempt.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_with_retry_all_attempts_fail() {
        let config = RetryConfig {
            max_retries: 2,
            initial_backoff_ms: 10,
            max_backoff_ms: 50,
            validate_logs_bloom: true,
            bloom_validation_retries: 3,
        };

        let result = with_retry(&config, "test_op", || async {
            Err::<i32, _>("always fails".to_string())
        })
        .await;

        assert_eq!(result.unwrap_err(), "always fails");
    }

    #[tokio::test]
    async fn test_with_retry_zero_retries() {
        let config = RetryConfig {
            max_retries: 0,
            initial_backoff_ms: 10,
            max_backoff_ms: 50,
            validate_logs_bloom: true,
            bloom_validation_retries: 3,
        };

        let result = with_retry(&config, "test_op", || async {
            Err::<i32, _>("fails once".to_string())
        })
        .await;

        assert!(result.is_err());
    }
}
