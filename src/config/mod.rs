use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;
use tracing_subscriber::{fmt, EnvFilter};

/// Top-level indexer configuration loaded from YAML
#[derive(Debug, Clone, Deserialize)]
pub struct IndexerConfig {
    pub database: DatabaseConfig,
    #[serde(default)]
    pub redis: Option<RedisConfig>,
    pub schema: SchemaConfig,
    pub chain: ChainConfig,
    pub source: SourceConfig,
    pub sync: SyncConfig,
    pub contracts: Vec<ContractConfig>,
    #[serde(default)]
    pub reorg: ReorgConfig,
    #[serde(default)]
    pub api: ApiConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    #[serde(default)]
    pub aggregations: AggregationConfig,
    #[serde(default)]
    pub export: ExportConfig,
    #[serde(default)]
    pub webhooks: Vec<WebhookConfig>,
    #[serde(default)]
    pub traces: Option<TraceConfig>,
    #[serde(default)]
    pub accounts: Option<AccountConfig>,
}

impl IndexerConfig {
    /// Load configuration from a YAML file
    pub fn from_file(path: &str) -> Result<Self> {
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path))?;

        // Expand environment variables in the YAML content
        let expanded = expand_env_vars(&contents);

        let config: IndexerConfig = serde_yaml::from_str(&expanded)
            .with_context(|| format!("Failed to parse config file: {}", path))?;

        config.validate()?;
        Ok(config)
    }

    /// Validate configuration values
    fn validate(&self) -> Result<()> {
        if self.chain.id == 0 {
            anyhow::bail!("chain.id must be a positive integer");
        }
        if self.sync.start_block == 0 {
            anyhow::bail!("sync.start_block must be greater than 0");
        }
        if self.sync.parallel_workers == 0 {
            anyhow::bail!("sync.parallel_workers must be at least 1");
        }
        if self.sync.parallel_workers > 32 {
            anyhow::bail!("sync.parallel_workers must be at most 32");
        }
        if self.contracts.is_empty() {
            anyhow::bail!("at least one contract must be configured");
        }

        // Validate schema names are safe SQL identifiers (prevents SQL injection)
        validate_sql_identifier(&self.schema.data_schema, "schema.data_schema")?;
        validate_sql_identifier(&self.schema.sync_schema, "schema.sync_schema")?;
        validate_sql_identifier(&self.schema.user_schema, "schema.user_schema")?;

        for contract in &self.contracts {
            if contract.name.is_empty() {
                anyhow::bail!("contract name cannot be empty");
            }
            // Validate contract names are safe SQL identifiers (used in table/view names)
            validate_sql_identifier(&contract.name, &format!("contract '{}'", contract.name))?;
            if contract.abi_path.is_empty() {
                anyhow::bail!("contract abi_path cannot be empty for '{}'", contract.name);
            }
            // Validate address or factory is set
            if contract.address.is_none() && contract.factory.is_none() {
                anyhow::bail!(
                    "contract '{}' must have either 'address' or 'factory' configured",
                    contract.name
                );
            }
            // Validate contract addresses are valid hex
            if let Some(ref addr) = contract.address {
                validate_eth_address(addr, &format!("contract '{}' address", contract.name))?;
            }
            // Validate factory child_contract_name if present
            if let Some(ref factory) = contract.factory {
                if let Some(ref child_name) = factory.child_contract_name {
                    validate_sql_identifier(child_name, &format!("contract '{}' factory.child_contract_name", contract.name))?;
                }
            }
        }

        // Warn if transfer:from/transfer:to requested without traces enabled
        if let Some(ref accounts) = self.accounts {
            if accounts.enabled {
                let traces_enabled = self
                    .traces
                    .as_ref()
                    .map(|t| t.enabled)
                    .unwrap_or(false);

                for addr_cfg in &accounts.addresses {
                    let has_transfer = addr_cfg
                        .events
                        .iter()
                        .any(|e| e.starts_with("transfer:"));
                    if has_transfer && !traces_enabled {
                        tracing::warn!(
                            address = addr_cfg.address.as_str(),
                            "Account '{}' has transfer:from/transfer:to events but traces are not enabled. \
                             Internal ETH transfers require call trace indexing to be enabled.",
                            addr_cfg.name
                        );
                    }
                }
            }
        }

        Ok(())
    }

    /// Resolve ABI file paths relative to the config file directory
    pub fn resolve_abi_paths(&mut self, config_path: &str) -> Result<()> {
        let config_dir = Path::new(config_path)
            .parent()
            .unwrap_or(Path::new("."));

        for contract in &mut self.contracts {
            let abi_path = config_dir.join(&contract.abi_path);
            contract.abi_path = abi_path
                .to_str()
                .context("Invalid ABI path")?
                .to_string();
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConfig {
    pub connection_string: String,
    #[serde(default = "default_pool_size")]
    pub pool_size: u32,
}

fn default_pool_size() -> u32 {
    20
}

#[derive(Debug, Clone, Deserialize)]
pub struct RedisConfig {
    pub url: String,
    #[serde(default = "default_batch_notification_interval")]
    pub batch_notification_interval: u64,
}

fn default_batch_notification_interval() -> u64 {
    1000
}

#[derive(Debug, Clone, Deserialize)]
pub struct SchemaConfig {
    /// Internal schema for raw events, factory children (shared across deployments)
    #[serde(default = "default_data_schema")]
    pub data_schema: String,
    /// Per-deployment schema for decoded tables and sync workers.
    /// Same name on restart → resume. New name → rebuild from raw_events.
    pub sync_schema: String,
    /// User-facing schema for views. Views are swapped to point to the active sync_schema
    /// when a deployment reaches live mode.
    pub user_schema: String,
}

fn default_data_schema() -> String {
    "kyomei_data".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChainConfig {
    pub id: u32,
    pub name: String,
}

/// Source configuration — discriminated union via `type` field
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SourceConfig {
    Rpc {
        url: String,
        #[serde(default = "default_rpc_finality")]
        finality: u64,
        fallback_rpc: Option<String>,
    },
    Erpc {
        url: String,
        #[serde(default = "default_rpc_finality")]
        finality: u64,
        project_id: Option<String>,
        fallback_rpc: Option<String>,
    },
    Hypersync {
        url: Option<String>,
        api_token: Option<String>,
        fallback_rpc: Option<String>,
    },
}

fn default_rpc_finality() -> u64 {
    65
}

impl SourceConfig {
    pub fn source_type(&self) -> &str {
        match self {
            SourceConfig::Rpc { .. } => "rpc",
            SourceConfig::Erpc { .. } => "erpc",
            SourceConfig::Hypersync { .. } => "hypersync",
        }
    }

    /// Get the RPC URL for eth_call (used by view function indexer).
    /// Returns the primary RPC URL, or fallback_rpc for HyperSync sources.
    pub fn rpc_url(&self) -> Option<&str> {
        match self {
            SourceConfig::Rpc { url, .. } => Some(url),
            SourceConfig::Erpc { url, .. } => Some(url),
            SourceConfig::Hypersync { fallback_rpc, .. } => fallback_rpc.as_deref(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SyncConfig {
    pub start_block: u64,
    #[serde(default = "default_parallel_workers")]
    pub parallel_workers: u32,
    #[serde(default = "default_blocks_per_request")]
    pub blocks_per_request: u64,
    #[serde(default = "default_blocks_per_worker")]
    pub blocks_per_worker: u64,
    #[serde(default = "default_event_buffer_size")]
    pub event_buffer_size: usize,
    #[serde(default = "default_checkpoint_interval")]
    pub checkpoint_interval: u64,
    /// Maximum consecutive errors in live sync before aborting (default: 100).
    /// Prevents silent hangs when RPC is permanently unreachable.
    #[serde(default = "default_max_live_consecutive_errors")]
    pub max_live_consecutive_errors: u32,
    /// Maximum concurrent RPC requests across all workers (default: parallel_workers * 2).
    /// Set to 0 to disable the concurrency limiter.
    #[serde(default)]
    pub max_concurrent_requests: Option<u32>,
    #[serde(default)]
    pub retry: RetryConfig,
    /// Number of fetch batches to buffer ahead of the inserter in the historic sync pipeline.
    /// Set to 0 to disable pipelining (sequential fetch-then-insert). Default: 2.
    #[serde(default = "default_pipeline_buffer")]
    pub pipeline_buffer: usize,
    /// When true, historic sync falls back to staging+ON CONFLICT on unique violation
    /// instead of skipping the batch. Useful for debugging or non-exclusive block ranges.
    #[serde(default)]
    pub safe_indexing: bool,
    /// Maximum contract addresses per RPC/source request within a single worker.
    /// When a worker has more addresses than this limit, it splits them into parallel
    /// batches within the same worker (does not create new workers). Default: 500.
    #[serde(default = "default_addresses_per_batch")]
    pub addresses_per_batch: usize,
}

/// Retry configuration for block fetching operations
#[derive(Debug, Clone, Deserialize)]
pub struct RetryConfig {
    /// Maximum number of retry attempts before giving up
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// Initial delay before the first retry (in milliseconds)
    #[serde(default = "default_initial_backoff_ms")]
    pub initial_backoff_ms: u64,
    /// Maximum delay between retries (in milliseconds)
    #[serde(default = "default_max_backoff_ms")]
    pub max_backoff_ms: u64,
    /// Enable logsBloom validation to detect invalid RPC responses
    #[serde(default = "default_validate_logs_bloom")]
    pub validate_logs_bloom: bool,
    /// Maximum retries specifically for logsBloom validation failures
    #[serde(default = "default_bloom_validation_retries")]
    pub bloom_validation_retries: u32,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: default_max_retries(),
            initial_backoff_ms: default_initial_backoff_ms(),
            max_backoff_ms: default_max_backoff_ms(),
            validate_logs_bloom: default_validate_logs_bloom(),
            bloom_validation_retries: default_bloom_validation_retries(),
        }
    }
}

fn default_max_retries() -> u32 {
    5
}
fn default_initial_backoff_ms() -> u64 {
    1000
}
fn default_max_backoff_ms() -> u64 {
    30_000
}
fn default_validate_logs_bloom() -> bool {
    true
}
fn default_bloom_validation_retries() -> u32 {
    3
}

fn default_parallel_workers() -> u32 {
    4
}
fn default_blocks_per_request() -> u64 {
    1000
}
fn default_blocks_per_worker() -> u64 {
    250_000
}
fn default_event_buffer_size() -> usize {
    10_000
}
fn default_checkpoint_interval() -> u64 {
    5000
}

fn default_max_live_consecutive_errors() -> u32 {
    100
}
fn default_pipeline_buffer() -> usize {
    2
}
fn default_addresses_per_batch() -> usize {
    500
}

#[derive(Debug, Clone, Deserialize)]
pub struct ContractConfig {
    pub name: String,
    pub address: Option<String>,
    pub factory: Option<FactoryContractConfig>,
    pub abi_path: String,
    pub start_block: Option<u64>,
    /// Event filters to apply before storage (optional)
    #[serde(default)]
    pub filters: Vec<EventFilterConfig>,
    /// View functions to periodically call and store results (optional)
    #[serde(default)]
    pub views: Vec<ViewFunctionConfig>,
}

/// Filter configuration for a specific event
#[derive(Debug, Clone, Deserialize)]
pub struct EventFilterConfig {
    /// Event name to filter (e.g., "Transfer")
    pub event: String,
    /// Filter conditions (all must match for the event to be stored)
    pub conditions: Vec<FilterCondition>,
}

/// A single filter condition on a decoded event parameter
#[derive(Debug, Clone, Deserialize)]
pub struct FilterCondition {
    /// Decoded parameter field name (e.g., "value", "amount0In")
    pub field: String,
    /// Comparison operator: eq, neq, gt, gte, lt, lte, contains
    pub op: String,
    /// Value to compare against (as string — numeric comparison for numeric fields)
    pub value: String,
}

/// Configuration for a view function to periodically call and store
#[derive(Debug, Clone, Deserialize)]
pub struct ViewFunctionConfig {
    /// Solidity function name (e.g., "totalSupply", "balanceOf")
    pub function: String,
    /// Call this function every N blocks
    pub interval_blocks: u64,
    /// Static parameters to pass (as hex-encoded strings)
    #[serde(default)]
    pub params: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FactoryContractConfig {
    pub address: String,
    pub event: String,
    pub parameter: FactoryParameter,
    pub child_abi_path: Option<String>,
    pub child_contract_name: Option<String>,
}

/// Factory parameter can be a single string or an array of strings
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum FactoryParameter {
    Single(String),
    Multiple(Vec<String>),
}

impl FactoryParameter {
    pub fn as_vec(&self) -> Vec<&str> {
        match self {
            FactoryParameter::Single(s) => vec![s.as_str()],
            FactoryParameter::Multiple(v) => v.iter().map(|s| s.as_str()).collect(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReorgConfig {
    #[serde(default = "default_max_reorg_depth")]
    pub max_reorg_depth: u64,
    pub finality_blocks: Option<u64>,
}

impl Default for ReorgConfig {
    fn default() -> Self {
        Self {
            max_reorg_depth: 1000,
            finality_blocks: None,
        }
    }
}

fn default_max_reorg_depth() -> u64 {
    1000
}

#[derive(Debug, Clone, Deserialize)]
pub struct ApiConfig {
    #[serde(default = "default_api_host")]
    pub host: String,
    #[serde(default = "default_api_port")]
    pub port: u16,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            host: default_api_host(),
            port: default_api_port(),
        }
    }
}

fn default_api_host() -> String {
    "0.0.0.0".to_string()
}
fn default_api_port() -> u16 {
    8080
}

#[derive(Debug, Clone, Deserialize)]
pub struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    #[serde(default = "default_log_format")]
    pub format: String,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: default_log_format(),
        }
    }
}

fn default_log_level() -> String {
    "info".to_string()
}
fn default_log_format() -> String {
    "pretty".to_string()
}

/// Configuration for TimescaleDB continuous aggregates (auto-refreshing rollups)
#[derive(Debug, Clone, Deserialize)]
pub struct AggregationConfig {
    /// Enable continuous aggregates for decoded event tables
    #[serde(default)]
    pub enabled: bool,
    /// Time bucket intervals to create (e.g., ["1h", "1d"])
    #[serde(default = "default_aggregation_intervals")]
    pub intervals: Vec<String>,
    /// Custom aggregate definitions per decoded event table.
    /// Each entry specifies a table and a list of SQL aggregate expressions
    /// that will be added to the continuous aggregate alongside the default
    /// event_count and tx_count columns.
    #[serde(default)]
    pub custom: Vec<CustomAggregateConfig>,
}

impl Default for AggregationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            intervals: default_aggregation_intervals(),
            custom: Vec::new(),
        }
    }
}

/// Custom aggregate definition for a specific decoded event table.
///
/// Allows defining arbitrary SQL aggregate expressions over the decoded
/// parameter columns (prefixed with `p_`) in each event table.
///
/// # Example YAML
/// ```yaml
/// custom:
///   - table: "event_uniswap_v2_pair_swap"
///     metrics:
///       - name: "total_amount0_in"
///         expr: "SUM(p_amount0_in::numeric)"
///       - name: "avg_amount0_out"
///         expr: "AVG(p_amount0_out::numeric)"
///       - name: "unique_senders"
///         expr: "COUNT(DISTINCT p_sender)"
///       - name: "max_amount1_out"
///         expr: "MAX(p_amount1_out::numeric)"
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct CustomAggregateConfig {
    /// The decoded event table name (e.g., "event_uniswap_v2_pair_swap")
    pub table: String,
    /// List of custom metric definitions
    pub metrics: Vec<CustomMetricConfig>,
}

/// A single custom metric expression for a continuous aggregate.
#[derive(Debug, Clone, Deserialize)]
pub struct CustomMetricConfig {
    /// Column alias in the aggregate view (e.g., "total_volume")
    pub name: String,
    /// SQL aggregate expression (e.g., "SUM(p_amount0_in::numeric)")
    /// Must be a valid PostgreSQL aggregate expression referencing columns
    /// from the decoded event table.
    pub expr: String,
}

fn default_aggregation_intervals() -> Vec<String> {
    vec!["1h".to_string(), "1d".to_string()]
}

/// Configuration for CSV export of decoded events
#[derive(Debug, Clone, Deserialize)]
pub struct ExportConfig {
    /// CSV export settings (optional)
    pub csv: Option<CsvExportConfig>,
}

impl Default for ExportConfig {
    fn default() -> Self {
        Self { csv: None }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CsvExportConfig {
    /// Enable CSV export
    #[serde(default)]
    pub enabled: bool,
    /// Output directory for CSV files
    #[serde(default = "default_csv_output_dir")]
    pub output_dir: String,
}

fn default_csv_output_dir() -> String {
    "./data/csv".to_string()
}

/// Configuration for a webhook endpoint that receives decoded events
#[derive(Debug, Clone, Deserialize)]
pub struct WebhookConfig {
    /// URL to POST events to
    pub url: String,
    /// Optional event name filter — only send matching events. Empty/absent = all events.
    #[serde(default)]
    pub events: Vec<String>,
    /// Number of retry attempts on failure (default: 3)
    #[serde(default = "default_webhook_retries")]
    pub retry_attempts: u32,
    /// Request timeout in seconds (default: 10)
    #[serde(default = "default_webhook_timeout")]
    pub timeout_secs: u64,
    /// Optional custom headers (e.g., Authorization)
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,
}

fn default_webhook_retries() -> u32 {
    3
}
fn default_webhook_timeout() -> u64 {
    10
}

/// Configuration for call trace indexing
#[derive(Debug, Clone, Deserialize)]
pub struct TraceConfig {
    #[serde(default)]
    pub enabled: bool,
    /// RPC method to use: "debug" (debug_traceBlockByNumber) or "parity" (trace_block)
    #[serde(default = "default_trace_method")]
    pub method: String,
    /// Tracer to use with debug method (e.g., "callTracer")
    #[serde(default = "default_tracer")]
    pub tracer: String,
    /// Contracts whose inbound calls should be traced
    #[serde(default)]
    pub contracts: Vec<TraceContractConfig>,
}

fn default_trace_method() -> String {
    "debug".to_string()
}
fn default_tracer() -> String {
    "callTracer".to_string()
}

/// A contract to trace (can reference a factory-discovered contract)
#[derive(Debug, Clone, Deserialize)]
pub struct TraceContractConfig {
    pub name: String,
    /// Fixed contract address (mutually exclusive with factory_ref)
    pub address: Option<String>,
    /// Reference to a top-level contracts[] entry for factory-discovered addresses
    pub factory_ref: Option<String>,
    /// Optional ABI for function decoding (without it, only raw traces are stored)
    pub abi_path: Option<String>,
    pub start_block: Option<u64>,
}

/// Configuration for account/transaction indexing
#[derive(Debug, Clone, Deserialize)]
pub struct AccountConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Addresses to track transactions and transfers for
    #[serde(default)]
    pub addresses: Vec<AccountAddressConfig>,
}

/// A single tracked address for account indexing
#[derive(Debug, Clone, Deserialize)]
pub struct AccountAddressConfig {
    pub name: String,
    pub address: String,
    pub start_block: Option<u64>,
    /// Which event types to track (default: all four)
    #[serde(default = "default_account_events")]
    pub events: Vec<String>,
}

fn default_account_events() -> Vec<String> {
    vec![
        "transaction:from".to_string(),
        "transaction:to".to_string(),
        "transfer:from".to_string(),
        "transfer:to".to_string(),
    ]
}

/// Parse a human-readable interval string ("1h", "1d", "15m") to a PostgreSQL interval string
pub fn parse_interval(interval: &str) -> Option<(&str, &str)> {
    // Returns (pg_interval, suffix) e.g. ("1 hour", "hourly")
    match interval {
        "15m" => Some(("15 minutes", "15m")),
        "30m" => Some(("30 minutes", "30m")),
        "1h" => Some(("1 hour", "hourly")),
        "4h" => Some(("4 hours", "4h")),
        "1d" => Some(("1 day", "daily")),
        "1w" => Some(("1 week", "weekly")),
        _ => None,
    }
}

/// Get the refresh policy offsets for a given interval
/// Returns (start_offset, end_offset, schedule_interval) as PostgreSQL interval strings
pub fn refresh_policy_for_interval(interval: &str) -> Option<(&str, &str, &str)> {
    match interval {
        "15m" => Some(("30 minutes", "5 minutes", "15 minutes")),
        "30m" => Some(("1 hour", "5 minutes", "30 minutes")),
        "1h" => Some(("2 hours", "10 minutes", "1 hour")),
        "4h" => Some(("8 hours", "30 minutes", "4 hours")),
        "1d" => Some(("2 days", "1 hour", "1 day")),
        "1w" => Some(("2 weeks", "1 day", "1 week")),
        _ => None,
    }
}

/// Multi-chain configuration format — one indexer process for multiple chains.
/// Shared database/redis, per-chain source/sync/contracts.
#[derive(Debug, Clone, Deserialize)]
pub struct MultiChainConfig {
    pub database: DatabaseConfig,
    #[serde(default)]
    pub redis: Option<RedisConfig>,
    #[serde(default)]
    pub api: ApiConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    /// Per-chain configurations
    pub chains: Vec<ChainInstanceConfig>,
}

/// Per-chain section within a multi-chain config
#[derive(Debug, Clone, Deserialize)]
pub struct ChainInstanceConfig {
    pub chain: ChainConfig,
    pub schema: SchemaConfig,
    pub source: SourceConfig,
    pub sync: SyncConfig,
    pub contracts: Vec<ContractConfig>,
    #[serde(default)]
    pub reorg: ReorgConfig,
    #[serde(default)]
    pub aggregations: AggregationConfig,
    #[serde(default)]
    pub export: ExportConfig,
    #[serde(default)]
    pub webhooks: Vec<WebhookConfig>,
    #[serde(default)]
    pub traces: Option<TraceConfig>,
    #[serde(default)]
    pub accounts: Option<AccountConfig>,
}

impl IndexerConfig {
    /// Load configuration from a YAML file, supporting both single-chain and multi-chain formats.
    /// Returns a Vec<IndexerConfig> — single-chain wraps in a one-element vec.
    pub fn from_file_multi(path: &str) -> Result<Vec<Self>> {
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path))?;

        let expanded = expand_env_vars(&contents);

        // Try multi-chain format first (has "chains" key)
        let yaml_value: serde_yaml::Value = serde_yaml::from_str(&expanded)
            .with_context(|| format!("Failed to parse YAML: {}", path))?;

        if yaml_value.get("chains").is_some() {
            let multi: MultiChainConfig = serde_yaml::from_str(&expanded)
                .with_context(|| format!("Failed to parse multi-chain config: {}", path))?;

            if multi.chains.is_empty() {
                anyhow::bail!("multi-chain config must have at least one chain");
            }

            let configs: Vec<IndexerConfig> = multi
                .chains
                .into_iter()
                .map(|chain_instance| {
                    let config = IndexerConfig {
                        database: multi.database.clone(),
                        redis: multi.redis.clone(),
                        schema: chain_instance.schema,
                        chain: chain_instance.chain,
                        source: chain_instance.source,
                        sync: chain_instance.sync,
                        contracts: chain_instance.contracts,
                        reorg: chain_instance.reorg,
                        api: multi.api.clone(),
                        logging: multi.logging.clone(),
                        aggregations: chain_instance.aggregations,
                        export: chain_instance.export,
                        webhooks: chain_instance.webhooks,
                        traces: chain_instance.traces,
                        accounts: chain_instance.accounts,
                    };
                    config.validate()?;
                    Ok(config)
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(configs)
        } else {
            // Single-chain format
            let config: IndexerConfig = serde_yaml::from_str(&expanded)
                .with_context(|| format!("Failed to parse config file: {}", path))?;
            config.validate()?;
            Ok(vec![config])
        }
    }
}

/// Initialize tracing subscriber based on config
pub fn init_tracing(config: &LoggingConfig) -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&config.level));

    match config.format.as_str() {
        "json" => {
            fmt()
                .json()
                .with_env_filter(filter)
                .with_target(true)
                .with_thread_ids(false)
                .init();
        }
        _ => {
            fmt()
                .with_env_filter(filter)
                .with_target(true)
                .with_thread_ids(false)
                .init();
        }
    }

    Ok(())
}

/// Expand environment variables in the format ${VAR_NAME} within a string
/// Validate that a string is a safe SQL identifier (alphanumeric + underscore only).
/// Prevents SQL injection via schema names, contract names, and table names
/// that are interpolated into dynamic SQL.
pub fn validate_sql_identifier(name: &str, context: &str) -> Result<()> {
    if name.is_empty() {
        anyhow::bail!("{} cannot be empty", context);
    }
    if name.len() > 63 {
        anyhow::bail!("{} '{}' exceeds PostgreSQL identifier limit of 63 characters", context, name);
    }
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        anyhow::bail!(
            "{} '{}' contains invalid characters — only ASCII alphanumeric and underscore are allowed",
            context, name
        );
    }
    if name.chars().next().map_or(true, |c| c.is_ascii_digit()) {
        anyhow::bail!("{} '{}' must not start with a digit", context, name);
    }
    Ok(())
}

/// Validate that a string looks like a valid Ethereum hex address.
/// Accepts with or without 0x prefix. Does not validate checksum.
pub fn validate_eth_address(addr: &str, context: &str) -> Result<()> {
    let hex = addr.strip_prefix("0x").or_else(|| addr.strip_prefix("0X")).unwrap_or(addr);
    if hex.len() != 40 {
        anyhow::bail!("{} '{}' is not a valid Ethereum address (expected 40 hex chars after 0x)", context, addr);
    }
    if !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        anyhow::bail!("{} '{}' contains non-hex characters", context, addr);
    }
    Ok(())
}

fn expand_env_vars(input: &str) -> String {
    let mut result = input.to_string();
    let re = regex_lite::Regex::new(r"\$\{([^}]+)\}").unwrap();

    for cap in re.captures_iter(input) {
        let var_name = &cap[1];
        if let Ok(value) = std::env::var(var_name) {
            result = result.replace(&cap[0], &value);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_config() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_schema_sync"
  user_schema: "test_schema"
chain:
  id: 1
  name: "ethereum"
source:
  type: rpc
  url: "http://localhost:8545"
sync:
  start_block: 100
contracts:
  - name: "TestContract"
    address: "0x1234567890abcdef1234567890abcdef12345678"
    abi_path: "./abis/test.json"
"#;
        let config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.chain.id, 1);
        assert_eq!(config.chain.name, "ethereum");
        assert_eq!(config.sync.start_block, 100);
        assert_eq!(config.sync.parallel_workers, 4); // default
        assert_eq!(config.contracts.len(), 1);
        assert_eq!(config.contracts[0].name, "TestContract");
    }

    #[test]
    fn test_parse_hypersync_source() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "ethereum"
source:
  type: hypersync
  url: "https://eth.hypersync.xyz"
  fallback_rpc: "https://eth.llamarpc.com"
sync:
  start_block: 100
contracts:
  - name: "Test"
    address: "0x1234567890abcdef1234567890abcdef12345678"
    abi_path: "./test.json"
"#;
        let config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        match &config.source {
            SourceConfig::Hypersync { url, fallback_rpc, .. } => {
                assert_eq!(url.as_deref(), Some("https://eth.hypersync.xyz"));
                assert_eq!(fallback_rpc.as_deref(), Some("https://eth.llamarpc.com"));
            }
            _ => panic!("Expected HyperSync source"),
        }
    }

    #[test]
    fn test_parse_factory_contract() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "ethereum"
source:
  type: rpc
  url: "http://localhost:8545"
sync:
  start_block: 100
contracts:
  - name: "UniswapV2Pair"
    factory:
      address: "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"
      event: "PairCreated"
      parameter: "pair"
    abi_path: "./abis/UniswapV2Pair.json"
"#;
        let config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        let contract = &config.contracts[0];
        assert!(contract.factory.is_some());
        let factory = contract.factory.as_ref().unwrap();
        assert_eq!(factory.event, "PairCreated");
        match &factory.parameter {
            FactoryParameter::Single(s) => assert_eq!(s, "pair"),
            _ => panic!("Expected single parameter"),
        }
    }

    #[test]
    fn test_parse_factory_multiple_parameters() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "ethereum"
source:
  type: rpc
  url: "http://localhost:8545"
sync:
  start_block: 100
contracts:
  - name: "Test"
    factory:
      address: "0x1234567890abcdef1234567890abcdef12345678"
      event: "Created"
      parameter:
        - "token0"
        - "token1"
    abi_path: "./test.json"
"#;
        let config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        let factory = config.contracts[0].factory.as_ref().unwrap();
        match &factory.parameter {
            FactoryParameter::Multiple(v) => {
                assert_eq!(v.len(), 2);
                assert_eq!(v[0], "token0");
                assert_eq!(v[1], "token1");
            }
            _ => panic!("Expected multiple parameters"),
        }
    }

    #[test]
    fn test_validation_no_contracts() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "ethereum"
source:
  type: rpc
  url: "http://localhost:8545"
sync:
  start_block: 100
contracts: []
"#;
        let config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_too_many_workers() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "ethereum"
source:
  type: rpc
  url: "http://localhost:8545"
sync:
  start_block: 100
  parallel_workers: 64
contracts:
  - name: "Test"
    address: "0x1234567890abcdef1234567890abcdef12345678"
    abi_path: "./test.json"
"#;
        let config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_expand_env_vars() {
        std::env::set_var("TEST_DB_URL", "postgresql://test:test@localhost/test");
        let input = "connection_string: \"${TEST_DB_URL}\"";
        let result = expand_env_vars(input);
        assert_eq!(result, "connection_string: \"postgresql://test:test@localhost/test\"");
        std::env::remove_var("TEST_DB_URL");
    }

    #[test]
    fn test_default_values() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "ethereum"
source:
  type: rpc
  url: "http://localhost:8545"
sync:
  start_block: 100
contracts:
  - name: "Test"
    address: "0x1234567890abcdef1234567890abcdef12345678"
    abi_path: "./test.json"
"#;
        let config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.database.pool_size, 20);
        assert_eq!(config.redis.as_ref().unwrap().batch_notification_interval, 1000);
        assert_eq!(config.schema.data_schema, "kyomei_data");
        assert_eq!(config.sync.parallel_workers, 4);
        assert_eq!(config.sync.blocks_per_request, 1000);
        assert_eq!(config.sync.blocks_per_worker, 250_000);
        assert_eq!(config.sync.event_buffer_size, 10_000);
        assert_eq!(config.sync.checkpoint_interval, 5000);
        assert_eq!(config.api.host, "0.0.0.0");
        assert_eq!(config.api.port, 8080);
        assert_eq!(config.logging.level, "info");
        assert_eq!(config.logging.format, "pretty");
    }

    #[test]
    fn test_redis_optional_in_config() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "ethereum"
source:
  type: rpc
  url: "http://localhost:8545"
sync:
  start_block: 100
contracts:
  - name: "Test"
    address: "0x1234567890abcdef1234567890abcdef12345678"
    abi_path: "./test.json"
"#;
        let config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.redis.is_none());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_redis_present_in_config() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "ethereum"
source:
  type: rpc
  url: "http://localhost:8545"
sync:
  start_block: 100
contracts:
  - name: "Test"
    address: "0x1234567890abcdef1234567890abcdef12345678"
    abi_path: "./test.json"
"#;
        let config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.redis.is_some());
        assert_eq!(config.redis.unwrap().url, "redis://localhost:6379");
    }

    #[test]
    fn test_source_type_display() {
        let rpc = SourceConfig::Rpc {
            url: "http://localhost".to_string(),
            finality: 65,
            fallback_rpc: None,
        };
        assert_eq!(rpc.source_type(), "rpc");

        let erpc = SourceConfig::Erpc {
            url: "http://localhost".to_string(),
            finality: 65,
            project_id: None,
            fallback_rpc: None,
        };
        assert_eq!(erpc.source_type(), "erpc");

        let hs = SourceConfig::Hypersync {
            url: None,
            api_token: None,
            fallback_rpc: None,
        };
        assert_eq!(hs.source_type(), "hypersync");
    }

    // === Additional validation edge case tests ===

    #[test]
    fn test_validation_chain_id_zero() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 0
  name: "test"
source:
  type: rpc
  url: "http://localhost:8545"
sync:
  start_block: 100
contracts:
  - name: "Test"
    address: "0x1234567890abcdef1234567890abcdef12345678"
    abi_path: "./test.json"
"#;
        let config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("chain.id"));
    }

    #[test]
    fn test_validation_start_block_zero() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "test"
source:
  type: rpc
  url: "http://localhost:8545"
sync:
  start_block: 0
contracts:
  - name: "Test"
    address: "0x1234567890abcdef1234567890abcdef12345678"
    abi_path: "./test.json"
"#;
        let config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("start_block"));
    }

    #[test]
    fn test_validation_zero_workers() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "test"
source:
  type: rpc
  url: "http://localhost:8545"
sync:
  start_block: 100
  parallel_workers: 0
contracts:
  - name: "Test"
    address: "0x1234567890abcdef1234567890abcdef12345678"
    abi_path: "./test.json"
"#;
        let config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("parallel_workers"));
    }

    #[test]
    fn test_validation_empty_contract_name() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "test"
source:
  type: rpc
  url: "http://localhost:8545"
sync:
  start_block: 100
contracts:
  - name: ""
    address: "0x1234567890abcdef1234567890abcdef12345678"
    abi_path: "./test.json"
"#;
        let config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("contract name cannot be empty"));
    }

    #[test]
    fn test_validation_empty_abi_path() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "test"
source:
  type: rpc
  url: "http://localhost:8545"
sync:
  start_block: 100
contracts:
  - name: "Test"
    address: "0x1234567890abcdef1234567890abcdef12345678"
    abi_path: ""
"#;
        let config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("abi_path cannot be empty"));
    }

    #[test]
    fn test_validation_contract_no_address_or_factory() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "test"
source:
  type: rpc
  url: "http://localhost:8545"
sync:
  start_block: 100
contracts:
  - name: "Test"
    abi_path: "./test.json"
"#;
        let config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("must have either 'address' or 'factory'"));
    }

    #[test]
    fn test_validation_boundary_32_workers_ok() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "test"
source:
  type: rpc
  url: "http://localhost:8545"
sync:
  start_block: 100
  parallel_workers: 32
contracts:
  - name: "Test"
    address: "0x1234567890abcdef1234567890abcdef12345678"
    abi_path: "./test.json"
"#;
        let config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validation_boundary_33_workers_err() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "test"
source:
  type: rpc
  url: "http://localhost:8545"
sync:
  start_block: 100
  parallel_workers: 33
contracts:
  - name: "Test"
    address: "0x1234567890abcdef1234567890abcdef12345678"
    abi_path: "./test.json"
"#;
        let config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_expand_env_vars_no_vars() {
        let input = "no variables here";
        assert_eq!(expand_env_vars(input), "no variables here");
    }

    #[test]
    fn test_expand_env_vars_undefined_var_remains() {
        let input = "url: ${TOTALLY_UNDEFINED_VAR_XYZ_12345}";
        let result = expand_env_vars(input);
        assert_eq!(result, "url: ${TOTALLY_UNDEFINED_VAR_XYZ_12345}");
    }

    #[test]
    fn test_expand_env_vars_multiple() {
        std::env::set_var("KYOMEI_TEST_HOST", "localhost");
        std::env::set_var("KYOMEI_TEST_PORT", "5432");
        let input = "postgresql://${KYOMEI_TEST_HOST}:${KYOMEI_TEST_PORT}/db";
        let result = expand_env_vars(input);
        assert_eq!(result, "postgresql://localhost:5432/db");
        std::env::remove_var("KYOMEI_TEST_HOST");
        std::env::remove_var("KYOMEI_TEST_PORT");
    }

    #[test]
    fn test_factory_parameter_as_vec_single() {
        let param = FactoryParameter::Single("pair".to_string());
        assert_eq!(param.as_vec(), vec!["pair"]);
    }

    #[test]
    fn test_factory_parameter_as_vec_multiple() {
        let param = FactoryParameter::Multiple(vec![
            "token0".to_string(),
            "token1".to_string(),
        ]);
        assert_eq!(param.as_vec(), vec!["token0", "token1"]);
    }

    #[test]
    fn test_aggregation_config_default() {
        let config = AggregationConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.intervals, vec!["1h", "1d"]);
    }

    #[test]
    fn test_aggregation_config_from_yaml() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "ethereum"
source:
  type: rpc
  url: "http://localhost:8545"
sync:
  start_block: 100
contracts:
  - name: "Test"
    address: "0x1234567890abcdef1234567890abcdef12345678"
    abi_path: "./test.json"
aggregations:
  enabled: true
  intervals: ["1h", "4h", "1d"]
"#;
        let config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.aggregations.enabled);
        assert_eq!(config.aggregations.intervals, vec!["1h", "4h", "1d"]);
    }

    #[test]
    fn test_aggregation_config_omitted() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "ethereum"
source:
  type: rpc
  url: "http://localhost:8545"
sync:
  start_block: 100
contracts:
  - name: "Test"
    address: "0x1234567890abcdef1234567890abcdef12345678"
    abi_path: "./test.json"
"#;
        let config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(!config.aggregations.enabled);
        assert_eq!(config.aggregations.intervals, vec!["1h", "1d"]);
    }

    #[test]
    fn test_parse_interval() {
        assert_eq!(parse_interval("1h"), Some(("1 hour", "hourly")));
        assert_eq!(parse_interval("1d"), Some(("1 day", "daily")));
        assert_eq!(parse_interval("15m"), Some(("15 minutes", "15m")));
        assert_eq!(parse_interval("4h"), Some(("4 hours", "4h")));
        assert_eq!(parse_interval("1w"), Some(("1 week", "weekly")));
        assert_eq!(parse_interval("invalid"), None);
    }

    #[test]
    fn test_refresh_policy_for_interval() {
        let (start, end, schedule) = refresh_policy_for_interval("1h").unwrap();
        assert_eq!(start, "2 hours");
        assert_eq!(end, "10 minutes");
        assert_eq!(schedule, "1 hour");

        let (start, end, schedule) = refresh_policy_for_interval("1d").unwrap();
        assert_eq!(start, "2 days");
        assert_eq!(end, "1 hour");
        assert_eq!(schedule, "1 day");

        assert!(refresh_policy_for_interval("invalid").is_none());
    }

    #[test]
    fn test_reorg_config_default() {
        let config = ReorgConfig::default();
        assert_eq!(config.max_reorg_depth, 1000);
        assert!(config.finality_blocks.is_none());
    }

    #[test]
    fn test_parse_erpc_with_project_id() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "ethereum"
source:
  type: erpc
  url: "http://erpc.example.com"
  project_id: "my-project"
sync:
  start_block: 100
contracts:
  - name: "Test"
    address: "0x1234567890abcdef1234567890abcdef12345678"
    abi_path: "./test.json"
"#;
        let config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        match &config.source {
            SourceConfig::Erpc { url, project_id, finality, .. } => {
                assert_eq!(url, "http://erpc.example.com");
                assert_eq!(project_id.as_deref(), Some("my-project"));
                assert_eq!(*finality, 65);
            }
            _ => panic!("Expected eRPC source"),
        }
    }

    #[test]
    fn test_resolve_abi_paths_relative() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "test"
source:
  type: rpc
  url: "http://localhost:8545"
sync:
  start_block: 100
contracts:
  - name: "Test"
    address: "0x1234567890abcdef1234567890abcdef12345678"
    abi_path: "./abis/test.json"
"#;
        let mut config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        config.resolve_abi_paths("/some/dir/config.yaml").unwrap();
        // Path::join preserves the relative component
        assert!(config.contracts[0].abi_path.contains("/some/dir/"));
        assert!(config.contracts[0].abi_path.contains("abis/test.json"));
    }

    // === Fallback RPC config tests ===

    #[test]
    fn test_parse_rpc_with_fallback() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "ethereum"
source:
  type: rpc
  url: "http://primary:8545"
  fallback_rpc: "http://backup:8545"
sync:
  start_block: 100
contracts:
  - name: "Test"
    address: "0x1234567890abcdef1234567890abcdef12345678"
    abi_path: "./test.json"
"#;
        let config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        match &config.source {
            SourceConfig::Rpc { url, fallback_rpc, .. } => {
                assert_eq!(url, "http://primary:8545");
                assert_eq!(fallback_rpc.as_deref(), Some("http://backup:8545"));
            }
            _ => panic!("Expected RPC source"),
        }
    }

    #[test]
    fn test_parse_rpc_without_fallback() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "ethereum"
source:
  type: rpc
  url: "http://primary:8545"
sync:
  start_block: 100
contracts:
  - name: "Test"
    address: "0x1234567890abcdef1234567890abcdef12345678"
    abi_path: "./test.json"
"#;
        let config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        match &config.source {
            SourceConfig::Rpc { fallback_rpc, .. } => {
                assert!(fallback_rpc.is_none());
            }
            _ => panic!("Expected RPC source"),
        }
    }

    #[test]
    fn test_parse_erpc_with_fallback() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "ethereum"
source:
  type: erpc
  url: "http://erpc:4000"
  project_id: "kyomei"
  fallback_rpc: "http://backup:8545"
sync:
  start_block: 100
contracts:
  - name: "Test"
    address: "0x1234567890abcdef1234567890abcdef12345678"
    abi_path: "./test.json"
"#;
        let config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        match &config.source {
            SourceConfig::Erpc { url, project_id, fallback_rpc, .. } => {
                assert_eq!(url, "http://erpc:4000");
                assert_eq!(project_id.as_deref(), Some("kyomei"));
                assert_eq!(fallback_rpc.as_deref(), Some("http://backup:8545"));
            }
            _ => panic!("Expected eRPC source"),
        }
    }

    #[test]
    fn test_parse_erpc_without_fallback() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "ethereum"
source:
  type: erpc
  url: "http://erpc:4000"
sync:
  start_block: 100
contracts:
  - name: "Test"
    address: "0x1234567890abcdef1234567890abcdef12345678"
    abi_path: "./test.json"
"#;
        let config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        match &config.source {
            SourceConfig::Erpc { fallback_rpc, .. } => {
                assert!(fallback_rpc.is_none());
            }
            _ => panic!("Expected eRPC source"),
        }
    }

    // === Retry config tests ===

    #[test]
    fn test_retry_config_defaults() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.initial_backoff_ms, 1000);
        assert_eq!(config.max_backoff_ms, 30_000);
        assert!(config.validate_logs_bloom);
        assert_eq!(config.bloom_validation_retries, 3);
    }

    #[test]
    fn test_retry_config_from_yaml_defaults() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "ethereum"
source:
  type: rpc
  url: "http://localhost:8545"
sync:
  start_block: 100
contracts:
  - name: "Test"
    address: "0x1234567890abcdef1234567890abcdef12345678"
    abi_path: "./test.json"
"#;
        let config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.sync.retry.max_retries, 5);
        assert_eq!(config.sync.retry.initial_backoff_ms, 1000);
        assert_eq!(config.sync.retry.max_backoff_ms, 30_000);
        assert!(config.sync.retry.validate_logs_bloom);
        assert_eq!(config.sync.retry.bloom_validation_retries, 3);
    }

    #[test]
    fn test_retry_config_from_yaml_custom() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "ethereum"
source:
  type: rpc
  url: "http://localhost:8545"
sync:
  start_block: 100
  retry:
    max_retries: 10
    initial_backoff_ms: 500
    max_backoff_ms: 60000
    validate_logs_bloom: false
    bloom_validation_retries: 5
contracts:
  - name: "Test"
    address: "0x1234567890abcdef1234567890abcdef12345678"
    abi_path: "./test.json"
"#;
        let config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.sync.retry.max_retries, 10);
        assert_eq!(config.sync.retry.initial_backoff_ms, 500);
        assert_eq!(config.sync.retry.max_backoff_ms, 60_000);
        assert!(!config.sync.retry.validate_logs_bloom);
        assert_eq!(config.sync.retry.bloom_validation_retries, 5);
    }

    #[test]
    fn test_retry_config_partial_yaml() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "ethereum"
source:
  type: rpc
  url: "http://localhost:8545"
sync:
  start_block: 100
  retry:
    max_retries: 3
contracts:
  - name: "Test"
    address: "0x1234567890abcdef1234567890abcdef12345678"
    abi_path: "./test.json"
"#;
        let config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.sync.retry.max_retries, 3);
        // Other fields should have defaults
        assert_eq!(config.sync.retry.initial_backoff_ms, 1000);
        assert!(config.sync.retry.validate_logs_bloom);
    }

    #[test]
    fn test_multi_chain_config_parsing() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
chains:
  - chain:
      id: 1
      name: "ethereum"
    schema:
      sync_schema: "eth_sync"
      user_schema: "eth"
    source:
      type: rpc
      url: "http://localhost:8545"
    sync:
      start_block: 18000000
      parallel_workers: 4
    contracts:
      - name: "USDC"
        address: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        abi_path: "./abis/ERC20.json"
  - chain:
      id: 137
      name: "polygon"
    schema:
      sync_schema: "poly_sync"
      user_schema: "poly"
    source:
      type: hypersync
      fallback_rpc: "https://polygon-rpc.com"
    sync:
      start_block: 50000000
    contracts:
      - name: "USDC"
        address: "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
        abi_path: "./abis/ERC20.json"
"#;
        let multi: MultiChainConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(multi.chains.len(), 2);
        assert_eq!(multi.chains[0].chain.id, 1);
        assert_eq!(multi.chains[0].chain.name, "ethereum");
        assert_eq!(multi.chains[0].schema.sync_schema, "eth_sync");
        assert_eq!(multi.chains[1].chain.id, 137);
        assert_eq!(multi.chains[1].chain.name, "polygon");
        assert_eq!(multi.chains[1].schema.sync_schema, "poly_sync");
    }

    #[test]
    fn test_multi_chain_expands_to_indexer_configs() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
api:
  host: "0.0.0.0"
  port: 9090
chains:
  - chain:
      id: 1
      name: "ethereum"
    schema:
      sync_schema: "eth_sync"
      user_schema: "eth"
    source:
      type: rpc
      url: "http://localhost:8545"
    sync:
      start_block: 18000000
    contracts:
      - name: "USDC"
        address: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        abi_path: "./abis/ERC20.json"
  - chain:
      id: 137
      name: "polygon"
    schema:
      sync_schema: "poly_sync"
      user_schema: "poly"
    source:
      type: rpc
      url: "http://localhost:8546"
    sync:
      start_block: 50000000
    contracts:
      - name: "USDC"
        address: "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
        abi_path: "./abis/ERC20.json"
"#;
        // Parse as multi-chain and expand
        let yaml_value: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();
        assert!(yaml_value.get("chains").is_some());

        let multi: MultiChainConfig = serde_yaml::from_str(yaml).unwrap();
        let configs: Vec<IndexerConfig> = multi
            .chains
            .into_iter()
            .map(|chain_instance| {
                IndexerConfig {
                    database: multi.database.clone(),
                    redis: multi.redis.clone(),
                    schema: chain_instance.schema,
                    chain: chain_instance.chain,
                    source: chain_instance.source,
                    sync: chain_instance.sync,
                    contracts: chain_instance.contracts,
                    reorg: chain_instance.reorg,
                    api: multi.api.clone(),
                    logging: multi.logging.clone(),
                    aggregations: chain_instance.aggregations,
                    export: chain_instance.export,
                    webhooks: chain_instance.webhooks,
                    traces: chain_instance.traces,
                    accounts: chain_instance.accounts,
                }
            })
            .collect();

        assert_eq!(configs.len(), 2);

        // Both share the same database
        assert_eq!(configs[0].database.connection_string, configs[1].database.connection_string);

        // Each has its own chain config
        assert_eq!(configs[0].chain.id, 1);
        assert_eq!(configs[0].chain.name, "ethereum");
        assert_eq!(configs[1].chain.id, 137);
        assert_eq!(configs[1].chain.name, "polygon");

        // Shared API config
        assert_eq!(configs[0].api.port, 9090);
        assert_eq!(configs[1].api.port, 9090);

        // Per-chain schemas
        assert_eq!(configs[0].schema.sync_schema, "eth_sync");
        assert_eq!(configs[1].schema.sync_schema, "poly_sync");
    }

    #[test]
    fn test_single_chain_yaml_has_no_chains_key() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "ethereum"
source:
  type: rpc
  url: "http://localhost:8545"
sync:
  start_block: 100
contracts:
  - name: "Test"
    address: "0x1234567890abcdef1234567890abcdef12345678"
    abi_path: "./test.json"
"#;
        let yaml_value: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();
        assert!(yaml_value.get("chains").is_none());
    }

    #[test]
    fn test_multi_chain_per_chain_features() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
chains:
  - chain:
      id: 1
      name: "ethereum"
    schema:
      sync_schema: "eth_sync"
      user_schema: "eth"
    source:
      type: rpc
      url: "http://localhost:8545"
    sync:
      start_block: 18000000
    contracts:
      - name: "USDC"
        address: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        abi_path: "./abis/ERC20.json"
    aggregations:
      enabled: true
      intervals: ["1h"]
    webhooks:
      - url: "https://example.com/eth-events"
  - chain:
      id: 137
      name: "polygon"
    schema:
      sync_schema: "poly_sync"
      user_schema: "poly"
    source:
      type: rpc
      url: "http://localhost:8546"
    sync:
      start_block: 50000000
    contracts:
      - name: "USDC"
        address: "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
        abi_path: "./abis/ERC20.json"
"#;
        let multi: MultiChainConfig = serde_yaml::from_str(yaml).unwrap();
        // Ethereum has aggregations enabled
        assert!(multi.chains[0].aggregations.enabled);
        assert_eq!(multi.chains[0].aggregations.intervals, vec!["1h"]);
        assert_eq!(multi.chains[0].webhooks.len(), 1);
        // Polygon uses defaults (no aggregations, no webhooks)
        assert!(!multi.chains[1].aggregations.enabled);
        assert!(multi.chains[1].webhooks.is_empty());
    }

    #[test]
    fn test_multi_chain_shared_database_config() {
        let yaml = r#"
database:
  connection_string: "postgresql://shared_db/indexer"
  pool_size: 50
redis:
  url: "redis://shared_redis:6379"
chains:
  - chain:
      id: 1
      name: "ethereum"
    schema:
      sync_schema: "eth_sync"
      user_schema: "eth"
    source:
      type: rpc
      url: "http://eth-rpc:8545"
    sync:
      start_block: 100
    contracts:
      - name: "Token"
        address: "0x1234567890abcdef1234567890abcdef12345678"
        abi_path: "./test.json"
  - chain:
      id: 137
      name: "polygon"
    schema:
      sync_schema: "poly_sync"
      user_schema: "poly"
    source:
      type: rpc
      url: "http://poly-rpc:8545"
    sync:
      start_block: 200
    contracts:
      - name: "Token"
        address: "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        abi_path: "./test.json"
"#;
        let multi: MultiChainConfig = serde_yaml::from_str(yaml).unwrap();
        // Both chains share same database
        assert_eq!(multi.database.connection_string, "postgresql://shared_db/indexer");
        assert_eq!(multi.database.pool_size, 50);
        assert_eq!(multi.redis.as_ref().unwrap().url, "redis://shared_redis:6379");
        // But have different schemas, sources, and start blocks
        assert_eq!(multi.chains[0].schema.sync_schema, "eth_sync");
        assert_eq!(multi.chains[1].schema.sync_schema, "poly_sync");
        assert_eq!(multi.chains[0].source.source_type(), "rpc");
        assert_eq!(multi.chains[1].source.source_type(), "rpc");
        assert_eq!(multi.chains[0].sync.start_block, 100);
        assert_eq!(multi.chains[1].sync.start_block, 200);
    }

    #[test]
    fn test_multi_chain_with_different_source_types() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
chains:
  - chain:
      id: 1
      name: "ethereum"
    schema:
      sync_schema: "eth_sync"
      user_schema: "eth"
    source:
      type: rpc
      url: "http://localhost:8545"
    sync:
      start_block: 100
    contracts:
      - name: "Token"
        address: "0x1234567890abcdef1234567890abcdef12345678"
        abi_path: "./test.json"
  - chain:
      id: 42161
      name: "arbitrum"
    schema:
      sync_schema: "arb_sync"
      user_schema: "arb"
    source:
      type: hypersync
      url: "https://arb.hypersync.xyz"
      fallback_rpc: "https://arb1.arbitrum.io/rpc"
    sync:
      start_block: 200
      parallel_workers: 8
    contracts:
      - name: "Token"
        address: "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        abi_path: "./test.json"
"#;
        let multi: MultiChainConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(multi.chains[0].source.source_type(), "rpc");
        assert_eq!(multi.chains[1].source.source_type(), "hypersync");
        // Verify fallback_rpc is set on hypersync source
        match &multi.chains[1].source {
            SourceConfig::Hypersync { fallback_rpc, .. } => {
                assert_eq!(fallback_rpc.as_deref(), Some("https://arb1.arbitrum.io/rpc"));
            }
            _ => panic!("Expected HyperSync source"),
        }
        assert_eq!(multi.chains[1].sync.parallel_workers, 8);
    }

    #[test]
    fn test_multi_chain_per_chain_filters_and_views() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
chains:
  - chain:
      id: 1
      name: "ethereum"
    schema:
      sync_schema: "eth_sync"
      user_schema: "eth"
    source:
      type: rpc
      url: "http://localhost:8545"
    sync:
      start_block: 100
    contracts:
      - name: "USDC"
        address: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        abi_path: "./abis/ERC20.json"
        filters:
          - event: "Transfer"
            conditions:
              - field: "value"
                op: "gte"
                value: "1000000"
        views:
          - function: "totalSupply"
            interval_blocks: 100
"#;
        let multi: MultiChainConfig = serde_yaml::from_str(yaml).unwrap();
        let contract = &multi.chains[0].contracts[0];
        assert_eq!(contract.filters.len(), 1);
        assert_eq!(contract.filters[0].event, "Transfer");
        assert_eq!(contract.filters[0].conditions[0].op, "gte");
        assert_eq!(contract.views.len(), 1);
        assert_eq!(contract.views[0].function, "totalSupply");
        assert_eq!(contract.views[0].interval_blocks, 100);
    }

    #[test]
    fn test_multi_chain_single_chain_yaml_wraps_in_vec() {
        // Single-chain YAML should NOT have a `chains` key
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "ethereum"
source:
  type: rpc
  url: "http://localhost:8545"
sync:
  start_block: 100
contracts:
  - name: "Token"
    address: "0x1234567890abcdef1234567890abcdef12345678"
    abi_path: "./test.json"
"#;
        let raw: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();
        // Verify single-chain format has no `chains` key
        assert!(raw.get("chains").is_none());
        assert!(raw.get("chain").is_some());
        // Can parse as IndexerConfig
        let config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.chain.id, 1);
    }

    #[test]
    fn test_trace_config_from_yaml() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "ethereum"
source:
  type: rpc
  url: "http://localhost:8545"
sync:
  start_block: 100
contracts:
  - name: "Token"
    address: "0x1234567890abcdef1234567890abcdef12345678"
    abi_path: "./test.json"
traces:
  enabled: true
  method: "parity"
  tracer: "callTracer"
  contracts:
    - name: "UniswapV2Router"
      address: "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D"
      abi_path: "./abis/Router.json"
      start_block: 10000835
    - name: "UniswapV2Pair"
      factory_ref: "UniswapV2Pair"
"#;
        let config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        let traces = config.traces.unwrap();
        assert!(traces.enabled);
        assert_eq!(traces.method, "parity");
        assert_eq!(traces.contracts.len(), 2);
        assert_eq!(traces.contracts[0].name, "UniswapV2Router");
        assert!(traces.contracts[0].address.is_some());
        assert!(traces.contracts[0].abi_path.is_some());
        assert_eq!(traces.contracts[0].start_block, Some(10000835));
        assert_eq!(traces.contracts[1].name, "UniswapV2Pair");
        assert!(traces.contracts[1].address.is_none());
        assert_eq!(traces.contracts[1].factory_ref.as_deref(), Some("UniswapV2Pair"));
    }

    #[test]
    fn test_trace_config_defaults() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "ethereum"
source:
  type: rpc
  url: "http://localhost:8545"
sync:
  start_block: 100
contracts:
  - name: "Token"
    address: "0x1234567890abcdef1234567890abcdef12345678"
    abi_path: "./test.json"
traces:
  enabled: true
  contracts:
    - name: "Router"
      address: "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D"
"#;
        let config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        let traces = config.traces.unwrap();
        assert_eq!(traces.method, "debug");
        assert_eq!(traces.tracer, "callTracer");
    }

    #[test]
    fn test_trace_config_not_set() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "ethereum"
source:
  type: rpc
  url: "http://localhost:8545"
sync:
  start_block: 100
contracts:
  - name: "Token"
    address: "0x1234567890abcdef1234567890abcdef12345678"
    abi_path: "./test.json"
"#;
        let config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.traces.is_none());
        assert!(config.accounts.is_none());
    }

    #[test]
    fn test_account_config_from_yaml() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "ethereum"
source:
  type: rpc
  url: "http://localhost:8545"
sync:
  start_block: 100
contracts:
  - name: "Token"
    address: "0x1234567890abcdef1234567890abcdef12345678"
    abi_path: "./test.json"
accounts:
  enabled: true
  addresses:
    - name: "Treasury"
      address: "0xdead000000000000000000000000000000000001"
      start_block: 10000835
      events: ["transaction:from", "transaction:to"]
    - name: "Vitalik"
      address: "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
"#;
        let config: IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        let accounts = config.accounts.unwrap();
        assert!(accounts.enabled);
        assert_eq!(accounts.addresses.len(), 2);
        assert_eq!(accounts.addresses[0].name, "Treasury");
        assert_eq!(accounts.addresses[0].events, vec!["transaction:from", "transaction:to"]);
        assert_eq!(accounts.addresses[0].start_block, Some(10000835));
        // Second address gets default events (all four)
        assert_eq!(accounts.addresses[1].name, "Vitalik");
        assert_eq!(accounts.addresses[1].events.len(), 4);
        assert!(accounts.addresses[1].events.contains(&"transfer:from".to_string()));
        assert!(accounts.addresses[1].events.contains(&"transfer:to".to_string()));
    }

    #[test]
    fn test_multi_chain_with_traces_and_accounts() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
chains:
  - chain:
      id: 1
      name: "ethereum"
    schema:
      sync_schema: "eth_sync"
      user_schema: "eth"
    source:
      type: rpc
      url: "http://localhost:8545"
    sync:
      start_block: 100
    contracts:
      - name: "Token"
        address: "0x1234567890abcdef1234567890abcdef12345678"
        abi_path: "./test.json"
    traces:
      enabled: true
      contracts:
        - name: "Router"
          address: "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D"
    accounts:
      enabled: true
      addresses:
        - name: "Whale"
          address: "0xdead000000000000000000000000000000000001"
"#;
        let multi: MultiChainConfig = serde_yaml::from_str(yaml).unwrap();
        let chain = &multi.chains[0];
        assert!(chain.traces.as_ref().unwrap().enabled);
        assert_eq!(chain.traces.as_ref().unwrap().contracts.len(), 1);
        assert!(chain.accounts.as_ref().unwrap().enabled);
        assert_eq!(chain.accounts.as_ref().unwrap().addresses.len(), 1);
    }
}
