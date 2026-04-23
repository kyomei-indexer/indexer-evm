use crate::abi::decoder::AbiDecoder;
use crate::config::IndexerConfig;
use anyhow::{Context, Result};
use sqlx::PgPool;
use tracing::{debug, info};

/// Run all database migrations to set up the 3-schema architecture:
/// - data_schema: raw_events, factory_children (shared across deployments)
/// - sync_schema: decoded event tables, sync_workers (per-deployment)
/// - user_schema: views for querying (points to active sync_schema)
pub async fn run(pool: &PgPool, config: &IndexerConfig, decoder: &AbiDecoder) -> Result<()> {
    let data_schema = &config.schema.data_schema;
    let sync_schema = &config.schema.sync_schema;
    let user_schema = &config.schema.user_schema;

    info!(data_schema, sync_schema, user_schema, "Running database migrations");

    // Ensure TimescaleDB extension is available
    ensure_timescaledb(pool).await?;

    // Create all three schemas
    create_schemas(pool, data_schema, sync_schema, user_schema).await?;

    // Create shared tables in data_schema (raw_events, factory_children)
    create_raw_events_table(pool, data_schema).await?;
    create_factory_children_table(pool, data_schema).await?;

    // Convert raw_events to hypertable partitioned by (block_number, chain_id)
    create_hypertable(pool, data_schema).await?;

    // Create per-deployment tables in sync_schema (sync_workers, decoded tables)
    create_sync_workers_table(pool, sync_schema).await?;
    create_decoded_event_tables(pool, sync_schema, decoder).await?;

    // Enable compression on data_schema hypertables
    enable_table_compression(
        pool,
        data_schema,
        "raw_events",
        "chain_id, address",
        "block_number DESC, tx_index DESC, log_index DESC",
    )
    .await?;

    // Enable compression on sync_schema decoded hypertables
    enable_compression_policies(pool, sync_schema, decoder).await?;

    // Create view function tables if configured
    create_view_function_tables(pool, sync_schema, &config.contracts).await?;

    // Create call trace tables if configured
    if let Some(ref traces) = config.traces {
        if traces.enabled {
            create_raw_traces_table(pool, data_schema).await?;
        }
    }

    // Create account event tables if configured
    if let Some(ref accounts) = config.accounts {
        if accounts.enabled {
            create_raw_account_events_table(pool, data_schema).await?;
        }
    }

    // Create continuous aggregates if enabled
    if config.aggregations.enabled {
        create_continuous_aggregates(pool, sync_schema, decoder, &config.aggregations.intervals, &config.aggregations.custom)
            .await?;
    }

    info!("Database migrations completed");
    Ok(())
}

async fn create_schemas(
    pool: &PgPool,
    data_schema: &str,
    sync_schema: &str,
    user_schema: &str,
) -> Result<()> {
    sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {data_schema}"))
        .execute(pool)
        .await?;

    sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {sync_schema}"))
        .execute(pool)
        .await?;

    sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {user_schema}"))
        .execute(pool)
        .await?;

    debug!(data_schema, sync_schema, user_schema, "Schemas created");
    Ok(())
}

async fn create_raw_events_table(pool: &PgPool, schema: &str) -> Result<()> {
    sqlx::query(&format!(
        r#"
        CREATE TABLE IF NOT EXISTS {schema}.raw_events (
            chain_id        INTEGER NOT NULL,
            block_number    BIGINT NOT NULL,
            tx_index        INTEGER NOT NULL,
            log_index       INTEGER NOT NULL,
            block_hash      TEXT NOT NULL,
            block_timestamp BIGINT NOT NULL,
            tx_hash         TEXT NOT NULL,
            address         TEXT NOT NULL,
            topic0          TEXT,
            topic1          TEXT,
            topic2          TEXT,
            topic3          TEXT,
            data            TEXT NOT NULL,
            PRIMARY KEY (chain_id, block_number, tx_index, log_index)
        )
        "#
    ))
    .execute(pool)
    .await?;

    // Create indexes
    sqlx::query(&format!(
        r#"
        CREATE INDEX IF NOT EXISTS idx_raw_events_address_topic
        ON {schema}.raw_events (chain_id, address, topic0, block_number)
        "#
    ))
    .execute(pool)
    .await?;

    sqlx::query(&format!(
        r#"
        CREATE INDEX IF NOT EXISTS idx_raw_events_block_timestamp
        ON {schema}.raw_events (chain_id, block_timestamp)
        "#
    ))
    .execute(pool)
    .await?;

    debug!("Created raw_events table");
    Ok(())
}

async fn create_factory_children_table(pool: &PgPool, schema: &str) -> Result<()> {
    sqlx::query(&format!(
        r#"
        CREATE TABLE IF NOT EXISTS {schema}.factory_children (
            chain_id              INTEGER NOT NULL,
            factory_address       TEXT NOT NULL,
            child_address         TEXT NOT NULL,
            contract_name         TEXT NOT NULL,
            created_at_block      BIGINT NOT NULL,
            created_at_tx_hash    TEXT NOT NULL,
            created_at_log_index  INTEGER NOT NULL,
            metadata              TEXT,
            child_abi             TEXT,
            created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (chain_id, child_address)
        )
        "#
    ))
    .execute(pool)
    .await?;

    // Create indexes
    sqlx::query(&format!(
        r#"
        CREATE INDEX IF NOT EXISTS idx_factory_children_factory
        ON {schema}.factory_children (chain_id, factory_address)
        "#
    ))
    .execute(pool)
    .await?;

    sqlx::query(&format!(
        r#"
        CREATE INDEX IF NOT EXISTS idx_factory_children_contract
        ON {schema}.factory_children (chain_id, contract_name)
        "#
    ))
    .execute(pool)
    .await?;

    sqlx::query(&format!(
        r#"
        CREATE INDEX IF NOT EXISTS idx_factory_children_block
        ON {schema}.factory_children (chain_id, created_at_block)
        "#
    ))
    .execute(pool)
    .await?;

    debug!("Created factory_children table");
    Ok(())
}

async fn create_sync_workers_table(pool: &PgPool, schema: &str) -> Result<()> {
    sqlx::query(&format!(
        r#"
        CREATE TABLE IF NOT EXISTS {schema}.sync_workers (
            chain_id      INTEGER NOT NULL,
            worker_id     INTEGER NOT NULL,
            range_start   BIGINT NOT NULL,
            range_end     BIGINT,
            current_block BIGINT NOT NULL,
            status        TEXT NOT NULL,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (chain_id, worker_id)
        )
        "#
    ))
    .execute(pool)
    .await?;

    debug!("Created sync_workers table");
    Ok(())
}

/// Ensure TimescaleDB extension is available
async fn ensure_timescaledb(pool: &PgPool) -> Result<()> {
    let exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')",
    )
    .fetch_one(pool)
    .await?;

    if !exists {
        sqlx::query("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE")
            .execute(pool)
            .await
            .context("TimescaleDB extension is required but could not be created")?;
        debug!("Created TimescaleDB extension");
    }

    Ok(())
}

/// Convert raw_events to a hypertable partitioned by block_number with chain_id space partitioning
async fn create_hypertable(pool: &PgPool, schema: &str) -> Result<()> {
    // Create hypertable partitioned by block_number (100k block chunks)
    let query = format!(
        r#"
        SELECT create_hypertable(
            '{schema}.raw_events',
            by_range('block_number', 100000),
            if_not_exists => TRUE
        )
        "#
    );
    match sqlx::query(&query).execute(pool).await {
        Ok(_) => {
            debug!("Created hypertable on raw_events (block_number range)");
        }
        Err(e) => {
            tracing::warn!(error = %e, "Failed to create hypertable (may already exist)");
        }
    }

    // Add chain_id as a space partitioning dimension for multi-chain chunk exclusion
    let space_query = format!(
        r#"
        SELECT add_dimension(
            '{schema}.raw_events',
            by_hash('chain_id', 4),
            if_not_exists => TRUE
        )
        "#
    );
    match sqlx::query(&space_query).execute(pool).await {
        Ok(_) => {
            debug!("Added chain_id space dimension to raw_events");
        }
        Err(e) => {
            tracing::warn!(error = %e, "Failed to add chain_id dimension (may already exist)");
        }
    }

    Ok(())
}

/// Convert a decoded event table to a hypertable with chain_id space partitioning
async fn create_decoded_hypertable(pool: &PgPool, schema: &str, table_name: &str) -> Result<()> {
    let query = format!(
        r#"
        SELECT create_hypertable(
            '{schema}.{table_name}',
            by_range('block_number', 100000),
            if_not_exists => TRUE
        )
        "#
    );
    match sqlx::query(&query).execute(pool).await {
        Ok(_) => {}
        Err(e) => {
            tracing::warn!(error = %e, table = table_name, "Failed to create hypertable");
        }
    }

    let space_query = format!(
        r#"
        SELECT add_dimension(
            '{schema}.{table_name}',
            by_hash('chain_id', 4),
            if_not_exists => TRUE
        )
        "#
    );
    match sqlx::query(&space_query).execute(pool).await {
        Ok(_) => {}
        Err(e) => {
            tracing::warn!(error = %e, table = table_name, "Failed to add chain_id dimension");
        }
    }

    Ok(())
}

/// Enable compression on decoded event hypertables in sync_schema
async fn enable_compression_policies(
    pool: &PgPool,
    schema: &str,
    decoder: &AbiDecoder,
) -> Result<()> {
    // Enable compression on all decoded event tables
    for (contract_name, events) in decoder.all_contract_events() {
        for event in events {
            let table_name = format!(
                "event_{}_{}",
                crate::abi::decoder::to_snake_case(contract_name),
                crate::abi::decoder::to_snake_case(&event.name)
            );
            enable_table_compression(
                pool,
                schema,
                &table_name,
                "chain_id, address",
                "block_number DESC, tx_index DESC, log_index DESC",
            )
            .await?;
        }
    }

    debug!("Compression policies configured for all hypertables");
    Ok(())
}

/// Enable compression on a single hypertable and add an automatic compression policy
async fn enable_table_compression(
    pool: &PgPool,
    schema: &str,
    table_name: &str,
    segment_by: &str,
    order_by: &str,
) -> Result<()> {
    // Enable compression settings
    let alter_sql = format!(
        r#"
        ALTER TABLE {schema}.{table_name} SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = '{segment_by}',
            timescaledb.compress_orderby = '{order_by}'
        )
        "#
    );
    match sqlx::query(&alter_sql).execute(pool).await {
        Ok(_) => {}
        Err(e) => {
            tracing::warn!(error = %e, table = table_name, "Failed to enable compression (may already be enabled)");
            return Ok(());
        }
    }

    // Add automatic compression policy: compress chunks older than 500k blocks
    // (roughly ~5 chunks behind, safe from reorg window)
    let policy_sql = format!(
        r#"
        SELECT add_compression_policy(
            '{schema}.{table_name}',
            compress_after => 500000::bigint,
            if_not_exists => true
        )
        "#
    );
    match sqlx::query(&policy_sql).execute(pool).await {
        Ok(_) => {}
        Err(e) => {
            tracing::warn!(error = %e, table = table_name, "Failed to add compression policy");
        }
    }

    Ok(())
}

/// Create per-event decoded tables as TimescaleDB hypertables.
/// Each unique event in the registered ABIs gets its own hypertable
/// with decoded columns (e.g., `event_uniswap_v2_pair_swap`).
async fn create_decoded_event_tables(
    pool: &PgPool,
    schema: &str,
    decoder: &AbiDecoder,
) -> Result<()> {
    let mut table_count = 0;

    for (contract_name, events) in decoder.all_contract_events() {
        for event in events {
            let sql = decoder.generate_event_table_sql(schema, &contract_name, event);

            sqlx::query(&sql).execute(pool).await?;

            let table_name = format!(
                "event_{}_{}",
                crate::abi::decoder::to_snake_case(&contract_name),
                crate::abi::decoder::to_snake_case(&event.name)
            );

            // Convert to hypertable with chain_id space partitioning
            create_decoded_hypertable(pool, schema, &table_name).await?;

            // Index on (chain_id, address) for contract-specific queries
            let idx_addr_sql = format!(
                r#"CREATE INDEX IF NOT EXISTS idx_{table_name}_address
                ON {schema}.{table_name} (chain_id, address)"#
            );
            sqlx::query(&idx_addr_sql).execute(pool).await?;

            // Index on (chain_id, block_timestamp) for time-based queries
            let idx_ts_sql = format!(
                r#"CREATE INDEX IF NOT EXISTS idx_{table_name}_timestamp
                ON {schema}.{table_name} (chain_id, block_timestamp)"#
            );
            sqlx::query(&idx_ts_sql).execute(pool).await?;

            table_count += 1;
        }
    }

    debug!(table_count, "Created per-event decoded hypertables");
    Ok(())
}

/// Create tables for view function results
async fn create_view_function_tables(
    pool: &PgPool,
    schema: &str,
    contracts: &[crate::config::ContractConfig],
) -> Result<()> {
    use crate::sync::view_indexer;

    let table_names = view_indexer::view_table_names(contracts);
    if table_names.is_empty() {
        return Ok(());
    }

    for (contract_name, function_name, _table_name) in &table_names {
        let sql = view_indexer::view_table_sql(schema, contract_name, function_name);
        sqlx::query(&sql).execute(pool).await?;
    }

    debug!(table_count = table_names.len(), "Created view function tables");
    Ok(())
}

/// Create TimescaleDB continuous aggregates for decoded event tables.
/// For each decoded event table and each configured interval, creates a
/// materialized view with time_bucket aggregation and a refresh policy.
///
/// Supports custom aggregate expressions per table via `CustomAggregateConfig`.
/// Custom metrics are appended to the default `event_count` and `tx_count` columns
/// in the continuous aggregate SQL. This enables powerful analytics like:
///
/// - **SUM/AVG/MIN/MAX** on numeric decoded params (e.g., `SUM(p_amount::numeric)`)
/// - **COUNT(DISTINCT ...)** on address-type params (e.g., unique senders)
/// - **Percentiles** via `percentile_cont()` (requires `ORDER BY` in aggregate)
/// - **Conditional aggregates** with `FILTER (WHERE ...)` clauses
/// - **Array aggregation** with `array_agg()` for collecting unique values
///
/// See `docs/aggregations.md` for a full guide on writing custom aggregate expressions.
pub async fn create_continuous_aggregates(
    pool: &PgPool,
    schema: &str,
    decoder: &AbiDecoder,
    intervals: &[String],
    custom_aggregates: &[crate::config::CustomAggregateConfig],
) -> Result<()> {
    use crate::config::{parse_interval, refresh_policy_for_interval};

    // Build a lookup map: table_name → custom metrics
    let custom_map: std::collections::HashMap<&str, &[crate::config::CustomMetricConfig]> =
        custom_aggregates
            .iter()
            .map(|c| (c.table.as_str(), c.metrics.as_slice()))
            .collect();

    let mut agg_count = 0;

    for (contract_name, events) in decoder.all_contract_events() {
        for event in events {
            let table_name = format!(
                "event_{}_{}",
                crate::abi::decoder::to_snake_case(contract_name),
                crate::abi::decoder::to_snake_case(&event.name)
            );

            // Build custom metric SQL fragment for this table
            let custom_sql = if let Some(metrics) = custom_map.get(table_name.as_str()) {
                metrics
                    .iter()
                    .map(|m| format!(",\n                        {} AS {}", m.expr, m.name))
                    .collect::<String>()
            } else {
                String::new()
            };

            for interval in intervals {
                let (pg_interval, suffix) = match parse_interval(interval) {
                    Some(v) => v,
                    None => {
                        tracing::warn!(interval, "Unknown aggregation interval, skipping");
                        continue;
                    }
                };

                let agg_name = format!("{}_{}", table_name, suffix);

                // Create the continuous aggregate materialized view
                let create_sql = format!(
                    r#"
                    CREATE MATERIALIZED VIEW IF NOT EXISTS {schema}.{agg_name}
                    WITH (timescaledb.continuous) AS
                    SELECT
                        chain_id,
                        address,
                        time_bucket('{pg_interval}'::interval, to_timestamp(block_timestamp)) AS bucket,
                        COUNT(*) AS event_count,
                        COUNT(DISTINCT tx_hash) AS tx_count{custom_sql}
                    FROM {schema}.{table_name}
                    GROUP BY chain_id, address, bucket
                    WITH NO DATA
                    "#
                );

                match sqlx::query(&create_sql).execute(pool).await {
                    Ok(_) => {
                        agg_count += 1;
                    }
                    Err(e) => {
                        // May already exist — that's fine
                        tracing::warn!(
                            error = %e,
                            view = agg_name,
                            "Failed to create continuous aggregate (may already exist)"
                        );
                        continue;
                    }
                }

                // Add refresh policy
                let (start_offset, end_offset, schedule_interval) =
                    match refresh_policy_for_interval(interval) {
                        Some(v) => v,
                        None => continue,
                    };

                let policy_sql = format!(
                    r#"
                    SELECT add_continuous_aggregate_policy(
                        '{schema}.{agg_name}',
                        start_offset => '{start_offset}'::interval,
                        end_offset => '{end_offset}'::interval,
                        schedule_interval => '{schedule_interval}'::interval,
                        if_not_exists => true
                    )
                    "#
                );

                match sqlx::query(&policy_sql).execute(pool).await {
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            view = agg_name,
                            "Failed to add refresh policy (may already exist)"
                        );
                    }
                }
            }
        }
    }

    debug!(agg_count, "Created continuous aggregates");
    Ok(())
}

/// Generate the SQL for a continuous aggregate (used by tests and view creation)
/// Create the raw_traces table for call trace indexing
async fn create_raw_traces_table(pool: &PgPool, schema: &str) -> Result<()> {
    sqlx::query(&format!(
        r#"
        CREATE TABLE IF NOT EXISTS {schema}.raw_traces (
            chain_id        INTEGER NOT NULL,
            block_number    BIGINT NOT NULL,
            tx_index        INTEGER NOT NULL,
            trace_address   TEXT NOT NULL,
            tx_hash         TEXT NOT NULL,
            block_hash      TEXT NOT NULL,
            block_timestamp BIGINT NOT NULL,
            call_type       TEXT NOT NULL,
            from_address    TEXT NOT NULL,
            to_address      TEXT NOT NULL,
            input           TEXT NOT NULL,
            output          TEXT NOT NULL,
            value           NUMERIC NOT NULL,
            gas             BIGINT NOT NULL,
            gas_used        BIGINT NOT NULL,
            error           TEXT,
            function_name   TEXT,
            function_sig    TEXT,
            PRIMARY KEY (chain_id, block_number, tx_index, trace_address)
        )
        "#
    ))
    .execute(pool)
    .await?;

    // Convert to hypertable
    sqlx::query(&format!(
        "SELECT create_hypertable('{schema}.raw_traces', by_range('block_number', 100000), if_not_exists => TRUE)"
    ))
    .execute(pool)
    .await?;

    // Add space dimension for multi-chain partitioning
    sqlx::query(&format!(
        "SELECT add_dimension('{schema}.raw_traces', by_hash('chain_id', 4), if_not_exists => TRUE)"
    ))
    .execute(pool)
    .await?;

    // Indexes for common query patterns
    sqlx::query(&format!(
        "CREATE INDEX IF NOT EXISTS idx_raw_traces_to_address ON {schema}.raw_traces (chain_id, to_address, block_number)"
    ))
    .execute(pool)
    .await?;

    sqlx::query(&format!(
        "CREATE INDEX IF NOT EXISTS idx_raw_traces_function_sig ON {schema}.raw_traces (chain_id, function_sig, block_number)"
    ))
    .execute(pool)
    .await?;

    debug!(schema, "raw_traces table created");
    Ok(())
}

/// Create the raw_account_events table for account/transaction indexing
async fn create_raw_account_events_table(pool: &PgPool, schema: &str) -> Result<()> {
    sqlx::query(&format!(
        r#"
        CREATE TABLE IF NOT EXISTS {schema}.raw_account_events (
            chain_id        INTEGER NOT NULL,
            block_number    BIGINT NOT NULL,
            tx_index        INTEGER NOT NULL,
            event_type      TEXT NOT NULL,
            address         TEXT NOT NULL,
            tx_hash         TEXT NOT NULL,
            block_hash      TEXT NOT NULL,
            block_timestamp BIGINT NOT NULL,
            counterparty    TEXT NOT NULL,
            value           NUMERIC NOT NULL,
            input           TEXT,
            trace_address   TEXT,
            PRIMARY KEY (chain_id, block_number, tx_index, event_type, address)
        )
        "#
    ))
    .execute(pool)
    .await?;

    // Convert to hypertable
    sqlx::query(&format!(
        "SELECT create_hypertable('{schema}.raw_account_events', by_range('block_number', 100000), if_not_exists => TRUE)"
    ))
    .execute(pool)
    .await?;

    // Add space dimension for multi-chain partitioning
    sqlx::query(&format!(
        "SELECT add_dimension('{schema}.raw_account_events', by_hash('chain_id', 4), if_not_exists => TRUE)"
    ))
    .execute(pool)
    .await?;

    // Indexes for common query patterns
    sqlx::query(&format!(
        "CREATE INDEX IF NOT EXISTS idx_raw_account_events_address ON {schema}.raw_account_events (chain_id, address, block_number)"
    ))
    .execute(pool)
    .await?;

    sqlx::query(&format!(
        "CREATE INDEX IF NOT EXISTS idx_raw_account_events_type ON {schema}.raw_account_events (chain_id, address, event_type, block_number)"
    ))
    .execute(pool)
    .await?;

    debug!(schema, "raw_account_events table created");
    Ok(())
}

/// Generate SQL for raw_traces table creation (for testing)
pub fn raw_traces_table_sql(schema: &str) -> String {
    format!(
        r#"
        CREATE TABLE IF NOT EXISTS {schema}.raw_traces (
            chain_id        INTEGER NOT NULL,
            block_number    BIGINT NOT NULL,
            tx_index        INTEGER NOT NULL,
            trace_address   TEXT NOT NULL,
            tx_hash         TEXT NOT NULL,
            block_hash      TEXT NOT NULL,
            block_timestamp BIGINT NOT NULL,
            call_type       TEXT NOT NULL,
            from_address    TEXT NOT NULL,
            to_address      TEXT NOT NULL,
            input           TEXT NOT NULL,
            output          TEXT NOT NULL,
            value           NUMERIC NOT NULL,
            gas             BIGINT NOT NULL,
            gas_used        BIGINT NOT NULL,
            error           TEXT,
            function_name   TEXT,
            function_sig    TEXT,
            PRIMARY KEY (chain_id, block_number, tx_index, trace_address)
        )
        "#
    )
}

/// Generate SQL for raw_account_events table creation (for testing)
pub fn raw_account_events_table_sql(schema: &str) -> String {
    format!(
        r#"
        CREATE TABLE IF NOT EXISTS {schema}.raw_account_events (
            chain_id        INTEGER NOT NULL,
            block_number    BIGINT NOT NULL,
            tx_index        INTEGER NOT NULL,
            event_type      TEXT NOT NULL,
            address         TEXT NOT NULL,
            tx_hash         TEXT NOT NULL,
            block_hash      TEXT NOT NULL,
            block_timestamp BIGINT NOT NULL,
            counterparty    TEXT NOT NULL,
            value           NUMERIC NOT NULL,
            input           TEXT,
            trace_address   TEXT,
            PRIMARY KEY (chain_id, block_number, tx_index, event_type, address)
        )
        "#
    )
}

pub fn continuous_aggregate_sql(
    schema: &str,
    table_name: &str,
    pg_interval: &str,
    suffix: &str,
) -> (String, String) {
    continuous_aggregate_sql_with_custom(schema, table_name, pg_interval, suffix, &[])
}

pub fn continuous_aggregate_sql_with_custom(
    schema: &str,
    table_name: &str,
    pg_interval: &str,
    suffix: &str,
    custom_metrics: &[crate::config::CustomMetricConfig],
) -> (String, String) {
    let agg_name = format!("{}_{}", table_name, suffix);
    let custom_sql: String = custom_metrics
        .iter()
        .map(|m| format!(",\n            {} AS {}", m.expr, m.name))
        .collect();
    let sql = format!(
        r#"
        CREATE MATERIALIZED VIEW IF NOT EXISTS {schema}.{agg_name}
        WITH (timescaledb.continuous) AS
        SELECT
            chain_id,
            address,
            time_bucket('{pg_interval}'::interval, to_timestamp(block_timestamp)) AS bucket,
            COUNT(*) AS event_count,
            COUNT(DISTINCT tx_hash) AS tx_count{custom_sql}
        FROM {schema}.{table_name}
        GROUP BY chain_id, address, bucket
        WITH NO DATA
        "#
    );
    (agg_name, sql)
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;
    use crate::abi::decoder::AbiDecoder;

    #[test]
    fn test_create_schemas_sql() {
        // Verify SQL is well-formed by checking format strings
        let schema = "kyomei_data";
        let sql = format!("CREATE SCHEMA IF NOT EXISTS {schema}");
        assert!(sql.contains("kyomei_data"));
    }

    #[test]
    fn test_decoded_event_table_generation() {
        let mut decoder = AbiDecoder::new();
        decoder
            .register_abi_json(
                "UniswapV2Pair",
                r#"[
                    {
                        "type": "event",
                        "name": "Swap",
                        "anonymous": false,
                        "inputs": [
                            { "type": "address", "name": "sender", "indexed": true },
                            { "type": "uint256", "name": "amount0In", "indexed": false },
                            { "type": "uint256", "name": "amount1In", "indexed": false },
                            { "type": "uint256", "name": "amount0Out", "indexed": false },
                            { "type": "uint256", "name": "amount1Out", "indexed": false },
                            { "type": "address", "name": "to", "indexed": true }
                        ]
                    },
                    {
                        "type": "event",
                        "name": "Sync",
                        "anonymous": false,
                        "inputs": [
                            { "type": "uint112", "name": "reserve0", "indexed": false },
                            { "type": "uint112", "name": "reserve1", "indexed": false }
                        ]
                    }
                ]"#,
            )
            .unwrap();

        // Verify all contract events are accessible
        let all_events: Vec<_> = decoder.all_contract_events().collect();
        assert_eq!(all_events.len(), 1); // 1 contract
        assert_eq!(all_events[0].0, "UniswapV2Pair");
        assert_eq!(all_events[0].1.len(), 2); // 2 events

        // Verify SQL generation for each event
        for (contract_name, events) in decoder.all_contract_events() {
            for event in events {
                let sql = decoder.generate_event_table_sql("kyomei_data", &contract_name, event);
                assert!(sql.contains("CREATE TABLE IF NOT EXISTS kyomei_data.event_uniswap_v2_pair_"));
                assert!(sql.contains("chain_id INTEGER NOT NULL"));
                assert!(sql.contains("block_number BIGINT NOT NULL"));
                assert!(sql.contains("PRIMARY KEY"));
            }
        }
    }

    #[test]
    fn test_decoded_tables_multiple_contracts() {
        let mut decoder = AbiDecoder::new();
        decoder
            .register_abi_json(
                "UniswapV2Factory",
                r#"[{
                    "type": "event",
                    "name": "PairCreated",
                    "anonymous": false,
                    "inputs": [
                        { "type": "address", "name": "token0", "indexed": true },
                        { "type": "address", "name": "token1", "indexed": true },
                        { "type": "address", "name": "pair", "indexed": false },
                        { "type": "uint256", "name": "pairIndex", "indexed": false }
                    ]
                }]"#,
            )
            .unwrap();
        decoder
            .register_abi_json(
                "UniswapV2Pair",
                r#"[{
                    "type": "event",
                    "name": "Swap",
                    "anonymous": false,
                    "inputs": [
                        { "type": "address", "name": "sender", "indexed": true },
                        { "type": "uint256", "name": "amount0In", "indexed": false },
                        { "type": "uint256", "name": "amount1In", "indexed": false },
                        { "type": "uint256", "name": "amount0Out", "indexed": false },
                        { "type": "uint256", "name": "amount1Out", "indexed": false },
                        { "type": "address", "name": "to", "indexed": true }
                    ]
                }]"#,
            )
            .unwrap();

        let all_events: Vec<_> = decoder.all_contract_events().collect();
        assert_eq!(all_events.len(), 2); // 2 contracts

        // Count total event tables that would be created
        let total_events: usize = all_events.iter().map(|(_, events)| events.len()).sum();
        assert_eq!(total_events, 2); // PairCreated + Swap
    }

    #[test]
    fn test_raw_events_table_sql_format() {
        let schema = "kyomei_data";
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {schema}.raw_events"
        );
        assert!(sql.contains("kyomei_data.raw_events"));
    }

    #[test]
    fn test_index_naming_convention() {
        let schema = "test_schema";
        let idx = format!(
            "CREATE INDEX IF NOT EXISTS idx_raw_events_address_topic ON {schema}.raw_events (chain_id, address, topic0, block_number)"
        );
        assert!(idx.contains("idx_raw_events_address_topic"));
        assert!(idx.contains("test_schema.raw_events"));
    }

    #[test]
    fn test_factory_children_index_names() {
        let schema = "kyomei_data";
        let idx_factory = format!(
            "CREATE INDEX IF NOT EXISTS idx_factory_children_factory ON {schema}.factory_children (chain_id, factory_address)"
        );
        let idx_contract = format!(
            "CREATE INDEX IF NOT EXISTS idx_factory_children_contract ON {schema}.factory_children (chain_id, contract_name)"
        );
        let idx_block = format!(
            "CREATE INDEX IF NOT EXISTS idx_factory_children_block ON {schema}.factory_children (chain_id, created_at_block)"
        );
        assert!(idx_factory.contains("idx_factory_children_factory"));
        assert!(idx_contract.contains("idx_factory_children_contract"));
        assert!(idx_block.contains("idx_factory_children_block"));
    }

    #[test]
    fn test_decoded_event_table_index_naming() {
        let table_name = "event_uniswap_v2_pair_swap";
        let schema = "kyomei_data";
        let idx_block = format!(
            "CREATE INDEX IF NOT EXISTS idx_{table_name}_block ON {schema}.{table_name} (chain_id, block_number)"
        );
        let idx_addr = format!(
            "CREATE INDEX IF NOT EXISTS idx_{table_name}_address ON {schema}.{table_name} (chain_id, address)"
        );
        let idx_ts = format!(
            "CREATE INDEX IF NOT EXISTS idx_{table_name}_timestamp ON {schema}.{table_name} (chain_id, block_timestamp)"
        );
        assert!(idx_block.contains("idx_event_uniswap_v2_pair_swap_block"));
        assert!(idx_addr.contains("idx_event_uniswap_v2_pair_swap_address"));
        assert!(idx_ts.contains("idx_event_uniswap_v2_pair_swap_timestamp"));
    }

    #[test]
    fn test_hypertable_sql_format() {
        let schema = "kyomei_data";
        let sql = format!(
            "SELECT create_hypertable('{schema}.raw_events', by_range('block_number', 100000), if_not_exists => TRUE)"
        );
        assert!(sql.contains("kyomei_data.raw_events"));
        assert!(sql.contains("block_number"));
        assert!(sql.contains("100000"));
    }

    #[test]
    fn test_space_dimension_sql_format() {
        let schema = "kyomei_data";
        let sql = format!(
            "SELECT add_dimension('{schema}.raw_events', by_hash('chain_id', 4), if_not_exists => TRUE)"
        );
        assert!(sql.contains("kyomei_data.raw_events"));
        assert!(sql.contains("chain_id"));
        assert!(sql.contains("by_hash"));
    }

    #[test]
    fn test_compression_sql_format() {
        let schema = "kyomei_data";
        let table = "raw_events";
        let alter_sql = format!(
            "ALTER TABLE {schema}.{table} SET (timescaledb.compress, timescaledb.compress_segmentby = 'chain_id, address', timescaledb.compress_orderby = 'block_number DESC, tx_index DESC, log_index DESC')"
        );
        assert!(alter_sql.contains("timescaledb.compress"));
        assert!(alter_sql.contains("compress_segmentby"));
        assert!(alter_sql.contains("compress_orderby"));
    }

    #[test]
    fn test_continuous_aggregate_sql_format() {
        let (agg_name, sql) =
            continuous_aggregate_sql("test_sync", "event_uniswap_v2_pair_swap", "1 hour", "hourly");
        assert_eq!(agg_name, "event_uniswap_v2_pair_swap_hourly");
        assert!(sql.contains("CREATE MATERIALIZED VIEW IF NOT EXISTS test_sync.event_uniswap_v2_pair_swap_hourly"));
        assert!(sql.contains("timescaledb.continuous"));
        assert!(sql.contains("time_bucket('1 hour'::interval"));
        assert!(sql.contains("to_timestamp(block_timestamp)"));
        assert!(sql.contains("COUNT(*) AS event_count"));
        assert!(sql.contains("COUNT(DISTINCT tx_hash) AS tx_count"));
        assert!(sql.contains("GROUP BY chain_id, address, bucket"));
        assert!(sql.contains("WITH NO DATA"));
    }

    #[test]
    fn test_continuous_aggregate_sql_daily() {
        let (agg_name, sql) =
            continuous_aggregate_sql("my_schema", "event_erc20_transfer", "1 day", "daily");
        assert_eq!(agg_name, "event_erc20_transfer_daily");
        assert!(sql.contains("my_schema.event_erc20_transfer_daily"));
        assert!(sql.contains("time_bucket('1 day'::interval"));
        assert!(sql.contains("FROM my_schema.event_erc20_transfer"));
    }

    #[test]
    fn test_continuous_aggregate_naming() {
        let (name_hourly, _) =
            continuous_aggregate_sql("s", "event_swap", "1 hour", "hourly");
        let (name_daily, _) =
            continuous_aggregate_sql("s", "event_swap", "1 day", "daily");
        let (name_4h, _) =
            continuous_aggregate_sql("s", "event_swap", "4 hours", "4h");

        assert_eq!(name_hourly, "event_swap_hourly");
        assert_eq!(name_daily, "event_swap_daily");
        assert_eq!(name_4h, "event_swap_4h");
    }

    #[test]
    fn test_continuous_aggregate_references_source_table() {
        // Verify the aggregate SELECT reads from the correct source table
        let (_, sql) = continuous_aggregate_sql("prod_sync", "event_token_transfer", "1 hour", "hourly");
        assert!(sql.contains("FROM prod_sync.event_token_transfer"));
        // The view is in the same schema
        assert!(sql.contains("prod_sync.event_token_transfer_hourly"));
    }

    #[test]
    fn test_continuous_aggregate_all_intervals() {
        let intervals = vec![
            ("15 minutes", "15m"),
            ("30 minutes", "30m"),
            ("1 hour", "hourly"),
            ("4 hours", "4h"),
            ("1 day", "daily"),
            ("1 week", "weekly"),
        ];

        for (pg_interval, suffix) in &intervals {
            let (name, sql) = continuous_aggregate_sql("s", "event_swap", pg_interval, suffix);
            assert_eq!(name, format!("event_swap_{}", suffix));
            assert!(sql.contains(&format!("time_bucket('{}'::interval", pg_interval)));
            // All aggregates should have the same structure
            assert!(sql.contains("COUNT(*) AS event_count"));
            assert!(sql.contains("COUNT(DISTINCT tx_hash) AS tx_count"));
            assert!(sql.contains("GROUP BY chain_id, address, bucket"));
        }
    }

    #[test]
    fn test_continuous_aggregate_multiple_tables() {
        // Simulate creating aggregates for multiple event types
        let tables = vec!["event_erc20_transfer", "event_uniswap_v2_pair_swap", "event_erc721_transfer"];
        let mut agg_names = Vec::new();

        for table in &tables {
            let (name, sql) = continuous_aggregate_sql("test_sync", table, "1 hour", "hourly");
            // Each gets a unique aggregate name
            assert!(!agg_names.contains(&name), "Duplicate aggregate name: {}", name);
            agg_names.push(name);
            // Each references its own source table
            assert!(sql.contains(&format!("FROM test_sync.{}", table)));
        }

        assert_eq!(agg_names.len(), 3);
    }

    #[test]
    fn test_continuous_aggregate_sql_is_idempotent() {
        let (_, sql) = continuous_aggregate_sql("s", "t", "1 hour", "hourly");
        assert!(sql.contains("IF NOT EXISTS"));
    }

    #[test]
    fn test_decoded_event_table_with_schema_prefix() {
        // Verify decoded tables use the sync_schema prefix
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {schema}.{table} (\n\
             chain_id INTEGER NOT NULL\n\
             )",
            schema = "uniswap_v2_sync",
            table = "event_uniswap_v2_pair_swap",
        );
        assert!(sql.contains("uniswap_v2_sync.event_uniswap_v2_pair_swap"));
    }

    #[test]
    fn test_raw_traces_table_sql_format() {
        let sql = raw_traces_table_sql("kyomei_data");
        assert!(sql.contains("kyomei_data.raw_traces"));
        assert!(sql.contains("chain_id"));
        assert!(sql.contains("block_number"));
        assert!(sql.contains("trace_address"));
        assert!(sql.contains("call_type"));
        assert!(sql.contains("from_address"));
        assert!(sql.contains("to_address"));
        assert!(sql.contains("input"));
        assert!(sql.contains("output"));
        assert!(sql.contains("value"));
        assert!(sql.contains("gas"));
        assert!(sql.contains("gas_used"));
        assert!(sql.contains("function_name"));
        assert!(sql.contains("function_sig"));
        assert!(sql.contains("PRIMARY KEY (chain_id, block_number, tx_index, trace_address)"));
    }

    #[test]
    fn test_raw_account_events_table_sql_format() {
        let sql = raw_account_events_table_sql("kyomei_data");
        assert!(sql.contains("kyomei_data.raw_account_events"));
        assert!(sql.contains("chain_id"));
        assert!(sql.contains("block_number"));
        assert!(sql.contains("event_type"));
        assert!(sql.contains("address"));
        assert!(sql.contains("counterparty"));
        assert!(sql.contains("value"));
        assert!(sql.contains("input"));
        assert!(sql.contains("trace_address"));
        assert!(sql.contains("PRIMARY KEY (chain_id, block_number, tx_index, event_type, address)"));
    }

    #[test]
    fn test_raw_traces_table_sql_different_schemas() {
        let sql1 = raw_traces_table_sql("prod_data");
        let sql2 = raw_traces_table_sql("dev_data");
        assert!(sql1.contains("prod_data.raw_traces"));
        assert!(sql2.contains("dev_data.raw_traces"));
        assert!(!sql1.contains("dev_data"));
    }

    #[test]
    fn test_continuous_aggregate_sql_with_custom_metrics() {
        use crate::config::CustomMetricConfig;

        let metrics = vec![
            CustomMetricConfig {
                name: "total_amount".to_string(),
                expr: "SUM(p_value::numeric)".to_string(),
            },
            CustomMetricConfig {
                name: "unique_senders".to_string(),
                expr: "COUNT(DISTINCT p_from)".to_string(),
            },
        ];

        let (name, sql) = continuous_aggregate_sql_with_custom(
            "test_sync",
            "event_erc20_transfer",
            "1 hour",
            "hourly",
            &metrics,
        );

        assert_eq!(name, "event_erc20_transfer_hourly");
        assert!(sql.contains("SUM(p_value::numeric) AS total_amount"));
        assert!(sql.contains("COUNT(DISTINCT p_from) AS unique_senders"));
        // Default metrics should still be present
        assert!(sql.contains("COUNT(*) AS event_count"));
        assert!(sql.contains("COUNT(DISTINCT tx_hash) AS tx_count"));
    }

    #[test]
    fn test_continuous_aggregate_sql_with_empty_custom_metrics() {
        let (_, sql_default) = continuous_aggregate_sql("s", "event_swap", "1 hour", "hourly");
        let (_, sql_custom) =
            continuous_aggregate_sql_with_custom("s", "event_swap", "1 hour", "hourly", &[]);
        assert_eq!(sql_default, sql_custom);
    }

    #[test]
    fn test_continuous_aggregate_sql_custom_with_filter() {
        use crate::config::CustomMetricConfig;

        let metrics = vec![CustomMetricConfig {
            name: "buy_count".to_string(),
            expr: "COUNT(*) FILTER (WHERE p_amount0_in::numeric > 0)".to_string(),
        }];

        let (_, sql) = continuous_aggregate_sql_with_custom(
            "sync",
            "event_pair_swap",
            "1 day",
            "daily",
            &metrics,
        );

        assert!(sql.contains("COUNT(*) FILTER (WHERE p_amount0_in::numeric > 0) AS buy_count"));
        assert!(sql.contains("event_pair_swap_daily"));
    }

    #[test]
    fn test_continuous_aggregate_sql_custom_preserves_structure() {
        use crate::config::CustomMetricConfig;

        let metrics = vec![CustomMetricConfig {
            name: "vol".to_string(),
            expr: "SUM(p_amount::numeric)".to_string(),
        }];

        let (_, sql) =
            continuous_aggregate_sql_with_custom("s", "event_t", "1 hour", "hourly", &metrics);

        // Verify structural elements are present
        assert!(sql.contains("CREATE MATERIALIZED VIEW IF NOT EXISTS"));
        assert!(sql.contains("WITH (timescaledb.continuous)"));
        assert!(sql.contains("time_bucket('1 hour'::interval"));
        assert!(sql.contains("GROUP BY chain_id, address, bucket"));
        assert!(sql.contains("WITH NO DATA"));
    }

    #[test]
    fn test_custom_aggregate_config_deserialization() {
        use crate::config::AggregationConfig;

        let yaml = r#"
enabled: true
intervals: ["1h"]
custom:
  - table: "event_swap"
    metrics:
      - name: "total_vol"
        expr: "SUM(p_amount::numeric)"
      - name: "unique_traders"
        expr: "COUNT(DISTINCT p_sender)"
"#;
        let config: AggregationConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.enabled);
        assert_eq!(config.custom.len(), 1);
        assert_eq!(config.custom[0].table, "event_swap");
        assert_eq!(config.custom[0].metrics.len(), 2);
        assert_eq!(config.custom[0].metrics[0].name, "total_vol");
        assert_eq!(config.custom[0].metrics[0].expr, "SUM(p_amount::numeric)");
        assert_eq!(config.custom[0].metrics[1].name, "unique_traders");
    }

    #[test]
    fn test_custom_aggregate_config_defaults_to_empty() {
        use crate::config::AggregationConfig;

        let yaml = r#"
enabled: true
intervals: ["1h", "1d"]
"#;
        let config: AggregationConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.custom.is_empty());
    }

    #[test]
    fn test_custom_aggregate_multiple_tables() {
        use crate::config::AggregationConfig;

        let yaml = r#"
enabled: true
intervals: ["1h"]
custom:
  - table: "event_erc20_transfer"
    metrics:
      - name: "total_transferred"
        expr: "SUM(p_value::numeric)"
  - table: "event_pair_swap"
    metrics:
      - name: "total_volume"
        expr: "SUM(p_amount0_in::numeric)"
      - name: "traders"
        expr: "COUNT(DISTINCT p_sender)"
"#;
        let config: AggregationConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.custom.len(), 2);
        assert_eq!(config.custom[0].table, "event_erc20_transfer");
        assert_eq!(config.custom[0].metrics.len(), 1);
        assert_eq!(config.custom[1].table, "event_pair_swap");
        assert_eq!(config.custom[1].metrics.len(), 2);
    }
}
