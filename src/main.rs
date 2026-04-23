mod abi;
mod api;
mod config;
mod db;
mod export;
mod factory;
mod filter;
mod metrics;
mod queue;
mod reorg;
mod sources;
mod sync;
mod types;

use abi::decoder::AbiDecoder;
use anyhow::Context;
use clap::Parser;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

#[derive(Parser, Debug)]
#[command(name = "kyomei-indexer", about = "High-performance EVM blockchain indexer")]
struct Args {
    /// Path to the YAML configuration file
    #[arg(short, long, default_value = "config.yaml")]
    config: String,

    /// Rebuild decoded event tables from existing raw_events without re-fetching from source.
    /// Useful after a crash that left decoded tables incomplete, or after ABI changes.
    #[arg(long)]
    rebuild_decoded: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load .env if present
    dotenvy::dotenv().ok();

    let args = Args::parse();

    // Load configuration (supports both single-chain and multi-chain formats)
    let mut configs = config::IndexerConfig::from_file_multi(&args.config)?;

    // Initialize tracing from the first config's logging settings
    config::init_tracing(&configs[0].logging)?;

    let chain_count = configs.len();
    if chain_count > 1 {
        info!(
            chains = chain_count,
            "Starting kyomei-indexer in multi-chain mode"
        );
    }

    for config in &configs {
        debug!(
            chain_id = config.chain.id,
            chain_name = %config.chain.name,
            source_type = %config.source.source_type(),
            "Configured chain"
        );
    }

    // Resolve ABI paths relative to config file directory (for all chains)
    for config in &mut configs {
        config.resolve_abi_paths(&args.config)?;
    }

    // Load ABIs for all chains
    let decoders: Vec<AbiDecoder> = configs
        .iter()
        .map(|config| load_abis(config))
        .collect::<anyhow::Result<Vec<_>>>()?;

    // Initialize shared database pool (all chains share the same database)
    info!("Connecting to database...");
    let db_pool = db::create_pool(&configs[0].database).await?;

    // Run migrations and create views for all chains
    for (config, decoder) in configs.iter().zip(decoders.iter()) {
        debug!(
            chain_id = config.chain.id,
            chain_name = %config.chain.name,
            "Running database migrations..."
        );
        db::migrations::run(&db_pool, config, decoder).await?;

        debug!(
            chain_id = config.chain.id,
            "Creating user schema views..."
        );
        create_views(&db_pool, config, decoder).await?;
    }
    info!("Database migrations completed for all chains");

    // Rebuild decoded tables if requested (for all chains)
    if args.rebuild_decoded {
        for (config, decoder) in configs.iter().zip(decoders.iter()) {
            info!(
                chain_id = config.chain.id,
                chain_name = %config.chain.name,
                "Rebuilding decoded tables from raw_events..."
            );
            rebuild_decoded_tables(&db_pool, config, decoder).await?;
        }
        info!("Decoded tables rebuilt successfully for all chains");
        return Ok(());
    }

    // Auto-detect new sync_schemas and rebuild from raw_events if needed
    for (config, decoder) in configs.iter().zip(decoders.iter()) {
        if should_rebuild_from_raw(&db_pool, config).await? {
            info!(
                chain_id = config.chain.id,
                sync_schema = %config.schema.sync_schema,
                "New sync_schema detected — rebuilding decoded tables from raw_events"
            );
            rebuild_decoded_tables(&db_pool, config, decoder).await?;
            info!(
                chain_id = config.chain.id,
                "Decoded tables rebuilt from raw_events for new sync_schema"
            );
        }
    }

    // Initialize shared Redis connection (optional)
    let redis_client = if let Some(ref redis_config) = configs[0].redis {
        debug!("Connecting to Redis...");
        Some(queue::create_client(redis_config)?)
    } else {
        debug!("Redis not configured — running without event streaming");
        None
    };

    // Initialize Prometheus metrics
    let metrics_handle = metrics::init();

    // Start HTTP API server (serves all chains)
    let api_config = &configs[0];
    info!(host = %api_config.api.host, port = api_config.api.port, "Starting HTTP API server...");
    let api_handle = api::start_server(
        &configs,
        db_pool.clone(),
        redis_client.clone(),
        metrics_handle,
    )
    .await?;
    debug!("HTTP API server started");

    // Set up graceful shutdown via CancellationToken (shared across all chains)
    let cancel = CancellationToken::new();
    {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            info!("Received shutdown signal (SIGINT/SIGTERM), initiating graceful shutdown...");
            cancel.cancel();
        });
    }

    // Spawn one ChainSyncer per chain
    let mut syncer_handles = Vec::new();

    for (config, decoder) in configs.into_iter().zip(decoders.into_iter()) {
        let pool = db_pool.clone();
        let redis = redis_client.clone();
        let cancel = cancel.clone();
        let chain_id = config.chain.id;
        let chain_name = config.chain.name.clone();

        let handle = tokio::spawn(async move {
            let syncer = sync::ChainSyncer::new(config, pool, redis.clone(), decoder, cancel).await?;
            syncer.start().await
        });

        debug!(chain_id, chain_name = %chain_name, "Spawned chain syncer");
        syncer_handles.push((chain_id, chain_name, handle));
    }

    // Wait for all syncers to complete (or fail)
    for (chain_id, chain_name, handle) in syncer_handles {
        match handle.await {
            Ok(Ok(())) => {
                info!(chain_id, chain_name = %chain_name, "Chain syncer completed");
            }
            Ok(Err(e)) => {
                error!(chain_id, chain_name = %chain_name, error = %e, "Chain syncer failed");
            }
            Err(e) => {
                error!(chain_id, chain_name = %chain_name, error = %e, "Chain syncer task panicked");
            }
        }
    }

    // Wait for API server to finish
    api_handle.await?;

    Ok(())
}

/// Load and register all contract ABIs from the config
fn load_abis(config: &config::IndexerConfig) -> anyhow::Result<AbiDecoder> {
    let mut decoder = AbiDecoder::new();

    for contract in &config.contracts {
        decoder
            .register_abi_file(&contract.name, &contract.abi_path)
            .with_context(|| {
                format!(
                    "Failed to load ABI for contract '{}' from '{}'",
                    contract.name, contract.abi_path
                )
            })?;

        debug!(
            contract = %contract.name,
            abi_path = %contract.abi_path,
            "Loaded contract ABI"
        );
    }

    let sig_count = decoder.all_signatures().len();
    info!(
        contracts = config.contracts.len(),
        event_signatures = sig_count,
        "All ABIs loaded"
    );

    Ok(decoder)
}

/// Create SQL views in the user schema for each contract's events
async fn create_views(
    pool: &sqlx::PgPool,
    config: &config::IndexerConfig,
    decoder: &AbiDecoder,
) -> anyhow::Result<()> {
    let view_creator = db::ViewCreator::new(
        pool.clone(),
        config.schema.data_schema.clone(),
        config.schema.sync_schema.clone(),
        config.schema.user_schema.clone(),
    );

    let chain_id = config.chain.id as i32;

    for contract in &config.contracts {
        let events = match decoder.get_events(&contract.name) {
            Some(events) => events,
            None => continue,
        };

        let is_factory = contract.factory.is_some();
        let address = contract.address.as_deref();

        for event in events {
            view_creator
                .create_event_view(
                    &contract.name,
                    &event.name,
                    &event.signature,
                    chain_id,
                    address,
                    is_factory,
                    contract.factory.as_ref().and_then(|f| f.child_contract_name.as_deref()),
                )
                .await
                .with_context(|| {
                    format!(
                        "Failed to create view for {}.{}",
                        contract.name, event.name
                    )
                })?;

            // Create aggregate views if aggregations are enabled
            if config.aggregations.enabled {
                for interval in &config.aggregations.intervals {
                    if let Some((_, suffix)) = config::parse_interval(interval) {
                        view_creator
                            .create_aggregate_view(
                                &contract.name,
                                &event.name,
                                suffix,
                                chain_id,
                                address,
                                is_factory,
                                contract.factory.as_ref().and_then(|f| f.child_contract_name.as_deref()),
                            )
                            .await
                            .with_context(|| {
                                format!(
                                    "Failed to create aggregate view for {}.{} ({})",
                                    contract.name, event.name, interval
                                )
                            })?;
                    }
                }
            }
        }
    }

    // Create views for traces if enabled
    if let Some(ref traces) = config.traces {
        if traces.enabled {
            view_creator
                .create_traces_view(chain_id)
                .await
                .context("Failed to create raw traces view")?;
        }
    }

    // Create views for account events if enabled
    if let Some(ref accounts) = config.accounts {
        if accounts.enabled {
            view_creator
                .create_account_events_view(chain_id)
                .await
                .context("Failed to create account events view")?;
        }
    }

    info!("User schema views created");
    Ok(())
}

/// Check if we should auto-rebuild decoded tables for a new sync_schema.
/// Returns true if the sync_schema has no workers AND the data_schema has raw_events.
async fn should_rebuild_from_raw(
    pool: &sqlx::PgPool,
    config: &config::IndexerConfig,
) -> anyhow::Result<bool> {
    let chain_id = config.chain.id as i32;
    let sync_schema = &config.schema.sync_schema;
    let data_schema = &config.schema.data_schema;

    // Check if sync_workers has any rows (existing deployment).
    // Migrations run before this function, so the table must exist.
    let has_workers = sqlx::query_scalar::<_, bool>(&format!(
        "SELECT EXISTS(SELECT 1 FROM {}.sync_workers WHERE chain_id = $1)",
        sync_schema
    ))
    .bind(chain_id)
    .fetch_one(pool)
    .await
    .context("failed to check sync_workers table")?;

    if has_workers {
        return Ok(false);
    }

    // Check if raw_events has data (otherwise nothing to rebuild from)
    let has_raw = sqlx::query_scalar::<_, bool>(&format!(
        "SELECT EXISTS(SELECT 1 FROM {}.raw_events WHERE chain_id = $1 LIMIT 1)",
        data_schema
    ))
    .bind(chain_id)
    .fetch_one(pool)
    .await
    .context("failed to check raw_events table")?;

    Ok(has_raw)
}

/// Rebuild decoded event tables from existing raw_events in the database.
/// Reads raw events in batches and re-decodes them into per-event tables.
/// Uses ON CONFLICT DO NOTHING so already-decoded rows are safely skipped.
async fn rebuild_decoded_tables(
    pool: &sqlx::PgPool,
    config: &config::IndexerConfig,
    decoder: &AbiDecoder,
) -> anyhow::Result<()> {
    let chain_id = config.chain.id as i32;
    let event_repo = db::events::EventRepository::new(pool.clone(), config.schema.data_schema.clone());
    let decoded_repo = db::decoded::DecodedEventRepository::new(pool.clone(), config.schema.sync_schema.clone());

    let earliest = event_repo.get_earliest_block(chain_id).await?;
    let latest = event_repo.get_latest_block(chain_id).await?;

    let (earliest, latest) = match (earliest, latest) {
        (Some(e), Some(l)) => (e, l),
        _ => {
            info!("No raw events found, nothing to rebuild");
            return Ok(());
        }
    };

    let total_blocks = latest - earliest + 1;
    info!(
        chain_id,
        earliest_block = earliest,
        latest_block = latest,
        total_blocks,
        "Rebuilding decoded tables from raw_events"
    );

    let batch_size = config.sync.blocks_per_request as i64;
    let mut current = earliest;
    let mut total_decoded = 0u64;
    let mut last_logged_milestone = 0u32;

    while current <= latest {
        let batch_end = std::cmp::min(current + batch_size - 1, latest);

        let events = event_repo
            .get_raw_events_range(chain_id, current, batch_end)
            .await?;

        if !events.is_empty() {
            let decoded = decoded_repo.insert_batch(&events, decoder).await?;
            total_decoded += decoded;
        }

        let progress = ((batch_end - earliest + 1) as f64 / total_blocks as f64 * 100.0) as u32;
        let milestone = progress / 10 * 10;
        if milestone > last_logged_milestone || batch_end == latest {
            info!(
                block = batch_end,
                progress = format!("{}%", progress),
                decoded_so_far = total_decoded,
                "Rebuild progress"
            );
            last_logged_milestone = milestone;
        }

        current = batch_end + 1;
    }

    info!(
        chain_id,
        total_decoded,
        "Decoded table rebuild complete"
    );

    Ok(())
}
