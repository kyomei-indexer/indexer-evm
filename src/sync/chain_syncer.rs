use crate::abi::decoder::AbiDecoder;
use crate::config::IndexerConfig;
use crate::db::decoded::DecodedEventRepository;
use crate::db::events::EventRepository;
use crate::db::factory::FactoryRepository;
use crate::db::workers::WorkerRepository;
use crate::factory::backfill::FactoryBackfiller;
use crate::factory::watcher::FactoryWatcher;
use crate::queue::publisher::EventPublisher;
use crate::reorg::detector::ReorgDetector;
use crate::reorg::finality::finality_blocks;
use crate::sources::BlockSource;
use crate::types::{
    BlockRange, LogFilter, SyncWorker, SyncWorkerStatus, WorkerChunk,
};
use anyhow::{Context, Result};
use sqlx::PgPool;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Shared progress state for aggregated logging across workers.
/// Each worker updates its own slot; a reporter task reads all slots periodically.
struct WorkerProgress {
    /// Current block per worker (indexed by worker_id - 1)
    current_blocks: Vec<AtomicU64>,
    /// Total events per worker
    total_events: Vec<AtomicU64>,
    /// Chunk boundaries per worker (from_block, to_block)
    ranges: Vec<(u64, u64)>,
    /// Overall range
    global_from: u64,
    global_to: u64,
}

impl WorkerProgress {
    fn new(chunks: &[WorkerChunk], global_from: u64, global_to: u64) -> Self {
        let n = chunks.len();
        let mut current_blocks = Vec::with_capacity(n);
        let mut total_events = Vec::with_capacity(n);
        let mut ranges = Vec::with_capacity(n);
        for chunk in chunks {
            current_blocks.push(AtomicU64::new(chunk.from_block));
            total_events.push(AtomicU64::new(0));
            ranges.push((chunk.from_block, chunk.to_block));
        }
        Self {
            current_blocks,
            total_events,
            ranges,
            global_from,
            global_to,
        }
    }

    fn update(&self, worker_idx: usize, current_block: u64, events: u64) {
        self.current_blocks[worker_idx].store(current_block, Ordering::Relaxed);
        self.total_events[worker_idx].store(events, Ordering::Relaxed);
    }

    fn overall_progress(&self) -> (f64, u64) {
        let total_range = self.global_to.saturating_sub(self.global_from) as f64;
        if total_range == 0.0 {
            return (100.0, 0);
        }
        let mut blocks_done = 0u64;
        let mut events = 0u64;
        for i in 0..self.ranges.len() {
            let current = self.current_blocks[i].load(Ordering::Relaxed);
            let (from, _to) = self.ranges[i];
            blocks_done += current.saturating_sub(from);
            events += self.total_events[i].load(Ordering::Relaxed);
        }
        let pct = (blocks_done as f64 / total_range * 100.0).min(100.0);
        (pct, events)
    }

    fn worker_percentages(&self) -> Vec<f64> {
        self.ranges
            .iter()
            .enumerate()
            .map(|(i, (from, to))| {
                let range = to.saturating_sub(*from) as f64;
                if range == 0.0 {
                    return 100.0;
                }
                let current = self.current_blocks[i].load(Ordering::Relaxed);
                (current.saturating_sub(*from) as f64 / range * 100.0).min(100.0)
            })
            .collect()
    }
}

/// Spawn a background task that periodically logs aggregated progress.
/// Returns the join handle so the caller can cancel it when workers finish.
fn spawn_progress_reporter(
    chain_id: i32,
    progress: Arc<WorkerProgress>,
    cancel: CancellationToken,
    phase_label: &str,
    interval_secs: u64,
) -> tokio::task::JoinHandle<()> {
    let label = phase_label.to_string();
    let is_single = progress.ranges.len() == 1;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
        interval.tick().await; // skip first immediate tick
        loop {
            interval.tick().await;
            if cancel.is_cancelled() {
                break;
            }
            let (overall_pct, total_events) = progress.overall_progress();
            if is_single {
                info!(
                    chain_id,
                    progress = format!("{:.1}%", overall_pct),
                    total_events,
                    "{label} progress",
                );
            } else {
                let worker_pcts = progress.worker_percentages();
                let workers_str: String = worker_pcts
                    .iter()
                    .enumerate()
                    .map(|(i, pct)| format!("W{}: {:.0}%", i + 1, pct))
                    .collect::<Vec<_>>()
                    .join(" | ");
                info!(
                    chain_id,
                    progress = format!("{:.1}%", overall_pct),
                    total_events,
                    workers = workers_str,
                    "{label} progress",
                );
            }
        }
    })
}

/// Orchestrates parallel historic sync and single live sync.
pub struct ChainSyncer {
    config: IndexerConfig,
    pool: PgPool,
    redis: Option<redis::Client>,
    decoder: Arc<AbiDecoder>,
    cancel: CancellationToken,
    concurrency: Option<Arc<crate::sources::concurrency::AdaptiveConcurrency>>,
    csv_exporter: Option<Arc<crate::export::CsvExporter>>,
    filter_engine: Option<Arc<crate::filter::EventFilterEngine>>,
    webhook_sender: Option<Arc<crate::queue::webhook::WebhookSender>>,
    view_indexer: Option<crate::sync::view_indexer::ViewFunctionIndexer>,
    trace_syncer: Option<crate::sync::trace_syncer::TraceSyncer>,
    account_syncer: Option<crate::sync::account_syncer::AccountSyncer>,
}

impl ChainSyncer {
    pub async fn new(
        config: IndexerConfig,
        pool: PgPool,
        redis: Option<redis::Client>,
        decoder: AbiDecoder,
        cancel: CancellationToken,
    ) -> Result<Self> {
        // Create shared concurrency limiter if configured
        let max_concurrent = config.sync.max_concurrent_requests
            .unwrap_or(config.sync.parallel_workers * 2);
        let concurrency = if max_concurrent > 0 {
            debug!(max_concurrent, "Creating adaptive concurrency limiter");
            Some(Arc::new(crate::sources::concurrency::AdaptiveConcurrency::new(max_concurrent)))
        } else {
            None
        };

        // Create CSV exporter if configured
        let csv_exporter = if let Some(csv_config) = &config.export.csv {
            if csv_config.enabled {
                Some(Arc::new(crate::export::CsvExporter::new(&csv_config.output_dir)?))
            } else {
                None
            }
        } else {
            None
        };

        // Create event filter engine if any contracts have filters
        let filter_engine = {
            let engine = crate::filter::EventFilterEngine::from_contracts(&config.contracts);
            if engine.has_filters() {
                Some(Arc::new(engine))
            } else {
                None
            }
        };

        // Create webhook sender if configured
        let webhook_sender = crate::queue::webhook::create_sender(&config.webhooks)
            .context("failed to create webhook HTTP client")?;

        // Create view function indexer if any contracts have views
        let has_views = config.contracts.iter().any(|c| !c.views.is_empty());
        let view_indexer = if has_views {
            if let Some(rpc_url) = config.source.rpc_url() {
                Some(crate::sync::view_indexer::ViewFunctionIndexer::new(
                    pool.clone(),
                    config.schema.sync_schema.clone(),
                    config.chain.id as i32,
                    &config.contracts,
                    rpc_url,
                ))
            } else {
                warn!("View functions configured but no RPC URL available (HyperSync without fallback_rpc)");
                None
            }
        } else {
            None
        };

        // Create trace syncer if configured
        let trace_syncer = if let Some(ref trace_config) = config.traces {
            if trace_config.enabled {
                if let Some(rpc_url) = config.source.rpc_url() {
                    // Load ABIs for function decoding if any trace contracts have abi_path
                    let function_decoder = {
                        let mut fd = crate::abi::function_decoder::FunctionDecoder::new();
                        for tc in &trace_config.contracts {
                            if let Some(ref abi_path) = tc.abi_path {
                                if let Err(e) = fd.register_abi_file(&tc.name, abi_path) {
                                    warn!(
                                        contract = %tc.name,
                                        abi_path = %abi_path,
                                        error = %e,
                                        "Failed to load ABI for trace function decoding"
                                    );
                                }
                            }
                        }
                        if fd.is_empty() { None } else { Some(Arc::new(fd)) }
                    };

                    match crate::sync::trace_syncer::TraceSyncer::new(
                        pool.clone(),
                        &config.schema.data_schema,
                        config.chain.id as i32,
                        trace_config,
                        rpc_url,
                        config.sync.retry.clone(),
                        function_decoder,
                        cancel.clone(),
                    ) {
                        Ok(ts) => Some(ts),
                        Err(e) => {
                            warn!(error = %e, "Failed to create TraceSyncer");
                            None
                        }
                    }
                } else {
                    warn!("Traces configured but no RPC URL available");
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        // Create account syncer if configured
        let account_syncer = if let Some(ref account_config) = config.accounts {
            if account_config.enabled {
                if let Some(rpc_url) = config.source.rpc_url() {
                    Some(crate::sync::account_syncer::AccountSyncer::new(
                        pool.clone(),
                        &config.schema.data_schema,
                        config.chain.id as i32,
                        account_config,
                        rpc_url,
                        config.sync.retry.clone(),
                        cancel.clone(),
                    ))
                } else {
                    warn!("Accounts configured but no RPC URL available");
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        Ok(Self {
            config,
            pool,
            redis,
            decoder: Arc::new(decoder),
            cancel,
            concurrency,
            csv_exporter,
            filter_engine,
            webhook_sender,
            view_indexer,
            trace_syncer,
            account_syncer,
        })
    }

    /// Start the sync process. Resumes from the last checkpoint or starts fresh.
    pub async fn start(&self) -> Result<()> {
        let chain_id = self.config.chain.id as i32;
        let worker_repo = WorkerRepository::new(self.pool.clone(), self.config.schema.sync_schema.clone());

        // Check existing worker state
        let existing_workers = worker_repo.get_workers(chain_id).await?;

        if existing_workers.is_empty() {
            debug!(chain_id, "No existing workers found, starting fresh sync");
            self.start_fresh().await?;
        } else {
            // Check if we have a live worker
            let has_live = existing_workers.iter().any(|w| w.status == SyncWorkerStatus::Live);
            let all_historic_done = existing_workers
                .iter()
                .filter(|w| w.status == SyncWorkerStatus::Historical)
                .all(|w| w.current_block >= w.range_end.unwrap_or(0));

            if has_live {
                info!(chain_id, "[PHASE: LIVE] Resuming live sync");
                self.run_live_sync().await?;
            } else if all_historic_done {
                info!(chain_id, "[PHASE: TRANSITION] Historic sync complete, transitioning to live");
                worker_repo.delete_all_workers(chain_id).await?;
                self.transition_to_live().await?;
            } else if self.config.sync.parallel_workers == 1 {
                // Single-worker resume: find the worker's checkpoint and continue from there
                let worker = existing_workers
                    .iter()
                    .find(|w| w.status == SyncWorkerStatus::Historical)
                    .expect("Should have at least one historical worker");

                let resume_block = (worker.current_block as u64).max(self.config.sync.start_block);
                let source = self.create_block_source().await?;
                let target_block = source.get_latest_block_number().await?;

                info!(
                    chain_id,
                    resume_block,
                    original_start = self.config.sync.start_block,
                    target_block,
                    checkpointed_block = worker.current_block,
                    ">> Resuming single-worker sync from checkpoint"
                );

                if resume_block >= target_block {
                    debug!("Already at or past target block, transitioning to live sync");
                    self.transition_to_live().await?;
                } else {
                    self.run_single_worker_sync(resume_block, target_block, source, Some(self.config.sync.start_block)).await?;
                    self.run_historic_trace_and_account_sync(resume_block, target_block).await?;
                    self.transition_to_live().await?;
                }
            } else {
                info!(
                    chain_id,
                    workers = existing_workers.len(),
                    ">> Resuming historic sync"
                );
                self.resume_historic_sync(&existing_workers).await?;
            }
        }

        Ok(())
    }

    /// Start a fresh sync from the configured start block.
    /// Dispatches to single-worker or multi-worker strategy based on `parallel_workers`.
    async fn start_fresh(&self) -> Result<()> {
        let chain_id = self.config.chain.id as i32;

        let source = self.create_block_source().await?;
        let target_block = source.get_latest_block_number().await?;
        let start_block = self.config.sync.start_block;

        info!(
            chain_id,
            start_block,
            target_block,
            parallel_workers = self.config.sync.parallel_workers,
            "[PHASE: INIT] Starting historic sync"
        );

        if start_block >= target_block {
            debug!("Already at or past target block, going directly to live sync");
            self.transition_to_live().await?;
            return Ok(());
        }

        if self.config.sync.parallel_workers == 1 {
            self.run_single_worker_sync(start_block, target_block, source, None).await?;
        } else {
            self.run_multi_worker_sync(start_block, target_block, source).await?;
        }

        // Run historic trace and account sync after event sync completes
        self.run_historic_trace_and_account_sync(start_block, target_block).await?;

        self.transition_to_live().await?;

        Ok(())
    }

    /// Run historic trace and account sync for the given block range.
    /// Called after event workers complete during start_fresh and resume_historic_sync.
    async fn run_historic_trace_and_account_sync(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<()> {
        if self.cancel.is_cancelled() {
            return Ok(());
        }

        info!(
            chain_id = self.config.chain.id as i32,
            "[PHASE: TRACES & ACCOUNTS] Running historic trace and account sync"
        );

        // Run trace and account sync concurrently
        let trace_handle = if let Some(ref trace_syncer) = self.trace_syncer {
            let from = from_block;
            let to = to_block;
            // TraceSyncer is not Send-safe for spawning, so run sequentially
            Some(trace_syncer.run_historic(from, to).await)
        } else {
            None
        };

        if let Some(result) = trace_handle {
            match result {
                Ok(count) => {
                    if count > 0 {
                        info!(
                            chain_id = self.config.chain.id,
                            traces = count,
                            "Historic trace sync completed"
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        chain_id = self.config.chain.id,
                        error = %e,
                        "Historic trace sync failed (continuing)"
                    );
                }
            }
        }

        if self.cancel.is_cancelled() {
            return Ok(());
        }

        if let Some(ref account_syncer) = self.account_syncer {
            match account_syncer.run_historic(from_block, to_block).await {
                Ok(count) => {
                    if count > 0 {
                        info!(
                            chain_id = self.config.chain.id,
                            events = count,
                            "Historic account sync completed"
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        chain_id = self.config.chain.id,
                        error = %e,
                        "Historic account sync failed (continuing)"
                    );
                }
            }
        }

        Ok(())
    }

    /// Run historic sync workers in parallel with aggregated progress reporting.
    async fn run_historic_workers(
        &self,
        chunks: Vec<WorkerChunk>,
        source: Arc<Box<dyn BlockSource>>,
        phase_label: &str,
    ) -> Result<()> {
        let chain_id = self.config.chain.id as i32;
        let event_repo = Arc::new(EventRepository::new(self.pool.clone(), self.config.schema.data_schema.clone()));
        let decoded_repo = Arc::new(DecodedEventRepository::new(self.pool.clone(), self.config.schema.sync_schema.clone()));
        let worker_repo = Arc::new(WorkerRepository::new(self.pool.clone(), self.config.schema.sync_schema.clone()));
        let factory_repo = Arc::new(FactoryRepository::new(self.pool.clone(), self.config.schema.data_schema.clone()));

        let publisher = if let Some(ref redis) = self.redis {
            Some(Arc::new(Mutex::new(
                EventPublisher::new(redis, chain_id).await?,
            )))
        } else {
            None
        };

        let batch_interval = self.config.redis.as_ref()
            .map_or(1000, |r| r.batch_notification_interval);
        let blocks_per_request = self.config.sync.blocks_per_request;
        let checkpoint_interval = self.config.sync.checkpoint_interval;
        let pipeline_buffer = self.config.sync.pipeline_buffer;
        let addresses_per_batch = self.config.sync.addresses_per_batch;

        // Build log filter including factory addresses + known children from DB
        let log_filter = self.build_live_log_filter().await?;

        // Create factory watcher (shared across workers)
        let factory_watcher = Arc::new(
            FactoryWatcher::new(&self.config.contracts, &self.decoder, chain_id)?
        );

        let decoder = Arc::clone(&self.decoder);
        let cancel = self.cancel.clone();

        // Compute global range for progress
        let global_from = chunks.iter().map(|c| c.from_block).min().unwrap_or(0);
        let global_to = chunks.iter().map(|c| c.to_block).max().unwrap_or(0);

        // Shared progress state
        let progress = Arc::new(WorkerProgress::new(&chunks, global_from, global_to));

        // Spawn aggregated progress reporter (logs every 10 seconds)
        let reporter_cancel = CancellationToken::new();
        let reporter = spawn_progress_reporter(
            chain_id,
            Arc::clone(&progress),
            reporter_cancel.clone(),
            phase_label,
            10,
        );

        let mut handles = Vec::new();

        for (idx, chunk) in chunks.into_iter().enumerate() {
            let source = Arc::clone(&source);
            let event_repo = Arc::clone(&event_repo);
            let decoded_repo = Arc::clone(&decoded_repo);
            let worker_repo = Arc::clone(&worker_repo);
            let factory_repo = Arc::clone(&factory_repo);
            let publisher = publisher.clone();
            let factory_watcher = Arc::clone(&factory_watcher);
            let decoder = Arc::clone(&decoder);
            let filter = log_filter.clone();
            let cancel = cancel.clone();
            let csv_exporter = self.csv_exporter.clone();
            let filter_engine = self.filter_engine.clone();
            let webhook_sender = self.webhook_sender.clone();
            let progress = Arc::clone(&progress);

            let safe_indexing = self.config.sync.safe_indexing;
            let worker_id = chunk.worker_id;
            let handle = tokio::spawn(async move {
                if let Err(e) = run_historic_worker(
                    chain_id,
                    chunk,
                    source,
                    event_repo,
                    decoded_repo,
                    worker_repo,
                    factory_repo,
                    publisher,
                    factory_watcher,
                    decoder,
                    filter,
                    blocks_per_request,
                    checkpoint_interval,
                    batch_interval,
                    pipeline_buffer,
                    cancel,
                    csv_exporter,
                    filter_engine,
                    webhook_sender,
                    safe_indexing,
                    addresses_per_batch,
                    Some((progress, idx)),
                )
                .await
                {
                    error!(
                        chain_id,
                        worker_id,
                        error = %e,
                        "Historic worker failed"
                    );
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await?;
        }

        // Stop the reporter and log final state
        reporter_cancel.cancel();
        let _ = reporter.await;

        let (final_pct, final_events) = progress.overall_progress();
        info!(
            chain_id,
            progress = format!("{:.1}%", final_pct),
            total_events = final_events,
            "{phase_label} complete",
        );

        Ok(())
    }

    /// Resume historic sync from checkpointed state
    async fn resume_historic_sync(&self, workers: &[SyncWorker]) -> Result<()> {
        let chunks: Vec<WorkerChunk> = workers
            .iter()
            .filter(|w| w.status == SyncWorkerStatus::Historical)
            .filter(|w| w.current_block < w.range_end.unwrap_or(0))
            .map(|w| WorkerChunk {
                worker_id: w.worker_id,
                from_block: w.current_block as u64,
                to_block: w.range_end.unwrap_or(0) as u64,
            })
            .collect();

        if chunks.is_empty() {
            debug!("No remaining historic work, transitioning to live");
            self.transition_to_live().await?;
            return Ok(());
        }

        let source: Arc<Box<dyn BlockSource>> = Arc::new(self.create_block_source().await?);
        self.run_historic_workers(chunks, source, "Historic sync").await?;
        self.transition_to_live().await?;
        Ok(())
    }

    /// Transition from historic to live sync
    async fn transition_to_live(&self) -> Result<()> {
        let chain_id = self.config.chain.id as i32;
        let worker_repo = WorkerRepository::new(self.pool.clone(), self.config.schema.sync_schema.clone());

        worker_repo.delete_all_workers(chain_id).await?;

        let mut publisher = if let Some(ref redis) = self.redis {
            Some(EventPublisher::new(redis, chain_id).await?)
        } else {
            None
        };

        let event_repo = EventRepository::new(self.pool.clone(), self.config.schema.data_schema.clone());
        let latest_block = event_repo.get_latest_block(chain_id).await?.unwrap_or(0);

        let live_worker = SyncWorker {
            chain_id,
            worker_id: 0,
            range_start: latest_block,
            range_end: None,
            current_block: latest_block,
            status: SyncWorkerStatus::Live,
        };
        worker_repo.set_worker(&live_worker).await?;

        let source = self.create_block_source().await?;
        let target = source.get_latest_block_number().await?;

        if let Some(ref mut publisher) = publisher {
            publisher
                .publish_sync_status(true, latest_block, target as i64)
                .await?;
        }

        let pending = (target as i64).saturating_sub(latest_block);
        info!(
            chain_id,
            current_block = latest_block,
            chain_tip = target,
            pending_blocks = pending,
            "[PHASE: TRANSITION] Historic sync complete — transitioning to realtime"
        );

        self.run_live_sync().await
    }

    /// Run live sync (single worker, per-block processing with reorg detection)
    async fn run_live_sync(&self) -> Result<()> {
        let chain_id = self.config.chain.id as i32;
        let source = self.create_block_source().await?;
        let event_repo = EventRepository::new(self.pool.clone(), self.config.schema.data_schema.clone());
        let decoded_repo = DecodedEventRepository::new(self.pool.clone(), self.config.schema.sync_schema.clone());
        let worker_repo = WorkerRepository::new(self.pool.clone(), self.config.schema.sync_schema.clone());
        let factory_repo = FactoryRepository::new(self.pool.clone(), self.config.schema.data_schema.clone());
        let mut publisher = if let Some(ref redis) = self.redis {
            Some(EventPublisher::new(redis, chain_id).await?)
        } else {
            None
        };
        let mut log_filter = self.build_live_log_filter().await?;

        // Factory integration
        let factory_watcher = FactoryWatcher::new(&self.config.contracts, &self.decoder, chain_id)?;
        let child_topic_filters = self.child_event_signatures();
        let backfiller = FactoryBackfiller::new(chain_id, self.config.sync.blocks_per_request);

        let finality = self
            .config
            .reorg
            .finality_blocks
            .unwrap_or_else(|| finality_blocks(self.config.chain.id));
        let mut reorg_detector = ReorgDetector::new(
            self.config.reorg.max_reorg_depth as usize,
            finality,
        );

        let live_worker = worker_repo
            .get_live_worker(chain_id)
            .await?
            .context("Live worker not found")?;
        let mut current_block = live_worker.current_block as u64 + 1;

        info!(chain_id, current_block, "[PHASE: LIVE] Starting realtime sync");

        let mut consecutive_errors = 0u32;
        let mut empty_block_retries = 0u32;
        let mut last_status_log = std::time::Instant::now();
        let status_log_interval = std::time::Duration::from_secs(30);

        let max_consecutive = self.config.sync.max_live_consecutive_errors;

        loop {
            // Check for graceful shutdown
            if self.cancel.is_cancelled() {
                info!(chain_id, current_block, "Shutdown signal received, stopping live sync");
                break;
            }

            let latest = match source.get_latest_block_number().await {
                Ok(n) => {
                    consecutive_errors = 0;
                    n
                }
                Err(e) => {
                    consecutive_errors += 1;
                    crate::metrics::consecutive_errors(self.config.chain.id, consecutive_errors);
                    if consecutive_errors >= max_consecutive {
                        error!(
                            chain_id,
                            consecutive_errors,
                            max_consecutive,
                            "Live sync exceeded max consecutive errors, aborting"
                        );
                        anyhow::bail!(
                            "Live sync aborted: {} consecutive errors (max: {})",
                            consecutive_errors,
                            max_consecutive
                        );
                    }
                    let backoff = std::cmp::min(2u64.saturating_pow(consecutive_errors), 30);
                    warn!(
                        chain_id,
                        current_block,
                        consecutive_errors,
                        backoff_secs = backoff,
                        error = %e,
                        "Failed to get latest block number in live sync, retrying"
                    );
                    tokio::time::sleep(tokio::time::Duration::from_secs(backoff)).await;
                    continue;
                }
            };

            let pending_blocks = latest.saturating_sub(current_block);

            if current_block > latest {
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                continue;
            }

            if pending_blocks > 0 && last_status_log.elapsed() >= status_log_interval {
                info!(
                    chain_id,
                    current_block,
                    chain_tip = latest,
                    pending_blocks,
                    "Realtime sync catching up"
                );
                last_status_log = std::time::Instant::now();
            }

            let range = BlockRange {
                from: current_block,
                to: current_block,
            };

            let blocks = match fetch_blocks_with_addr_batching(
                &*source, range, &log_filter, self.config.sync.addresses_per_batch,
            ).await {
                Ok(b) => {
                    consecutive_errors = 0;
                    b
                }
                Err(e) => {
                    consecutive_errors += 1;
                    if consecutive_errors >= max_consecutive {
                        error!(
                            chain_id,
                            consecutive_errors,
                            max_consecutive,
                            "Live sync exceeded max consecutive errors, aborting"
                        );
                        anyhow::bail!(
                            "Live sync aborted: {} consecutive errors (max: {})",
                            consecutive_errors,
                            max_consecutive
                        );
                    }
                    let backoff = std::cmp::min(2u64.saturating_pow(consecutive_errors), 30);
                    warn!(
                        chain_id,
                        current_block,
                        consecutive_errors,
                        backoff_secs = backoff,
                        error = %e,
                        "Failed to fetch block in live sync, retrying"
                    );
                    tokio::time::sleep(tokio::time::Duration::from_secs(backoff)).await;
                    continue;
                }
            };

            if let Some(block_data) = blocks.first() {
                empty_block_retries = 0;

                // Check for reorg
                let reorg_result = reorg_detector.validate_block(
                    block_data.block.number,
                    &block_data.block.hash,
                    &block_data.block.parent_hash,
                );

                match reorg_result {
                    crate::reorg::detector::ReorgCheckResult::Reorg {
                        common_ancestor_block,
                        depth,
                        ..
                    } => {
                        warn!(
                            chain_id,
                            common_ancestor_block,
                            depth,
                            "Reorg detected during live sync"
                        );

                        let from = common_ancestor_block as i64 + 1;
                        let to = current_block as i64;
                        event_repo.delete_range(chain_id, from, to).await?;
                        decoded_repo.delete_range(chain_id, from, to, &self.decoder).await?;
                        factory_repo.delete_from_block(chain_id, from).await?;

                        // Also clean up trace and account data for reorged blocks
                        if let Some(ref trace_syncer) = self.trace_syncer {
                            trace_syncer.delete_range(from, to).await?;
                        }
                        if let Some(ref account_syncer) = self.account_syncer {
                            account_syncer.delete_range(from, to).await?;
                        }

                        reorg_detector.invalidate_from(common_ancestor_block + 1);

                        // Rebuild filter after reorg (children may have been removed)
                        log_filter = self.build_live_log_filter().await?;

                        crate::metrics::reorg_detected(self.config.chain.id);

                        if let Some(ref mut publisher) = publisher {
                            publisher
                                .publish_reorg(common_ancestor_block as i64, depth)
                                .await?;
                        }

                        current_block = common_ancestor_block + 1;
                        continue;
                    }
                    crate::reorg::detector::ReorgCheckResult::Ok => {}
                }

                // Insert events
                let events: Vec<_> = block_data
                    .logs
                    .iter()
                    .map(|log| log.to_raw_event(chain_id))
                    .collect();

                // Begin atomic transaction for raw events + decoded events + worker checkpoint
                let mut tx = self.pool.begin().await?;
                let inserted = event_repo.insert_batch_in_tx(&events, &mut tx).await?;
                decoded_repo.insert_batch_in_tx_filtered(
                    &events,
                    &self.decoder,
                    &mut tx,
                    self.filter_engine.as_deref(),
                ).await?;

                // Detect factory children from this block's logs
                if factory_watcher.has_factories() {
                    let new_children = factory_watcher.process_logs(&block_data.logs);
                    if !new_children.is_empty() {
                        info!(
                            chain_id,
                            block = block_data.block.number,
                            new_children = new_children.len(),
                            "Discovered new factory children"
                        );

                        let backfill_events = backfiller
                            .backfill(
                                &new_children,
                                current_block,
                                source.as_ref(),
                                &event_repo,
                                &decoded_repo,
                                &factory_repo,
                                &child_topic_filters,
                                &self.decoder,
                            )
                            .await?;

                        if backfill_events > 0 {
                            debug!(
                                chain_id,
                                backfill_events,
                                "Backfilled events for new factory children"
                            );
                        }

                        // Add new child addresses to the live filter (normalized)
                        for child in &new_children {
                            log_filter.add_address(&child.child_address);
                        }
                    }
                }

                reorg_detector.record_block(
                    block_data.block.number,
                    &block_data.block.hash,
                );

                let worker = SyncWorker {
                    chain_id,
                    worker_id: 0,
                    range_start: live_worker.range_start,
                    range_end: None,
                    current_block: block_data.block.number as i64,
                    status: SyncWorkerStatus::Live,
                };
                worker_repo.set_worker_in_tx(&worker, &mut tx).await?;
                tx.commit().await?;

                // Export to CSV and send webhooks AFTER commit to avoid
                // notifying external systems about data that wasn't persisted
                if !events.is_empty() {
                    if let Some(ref exporter) = self.csv_exporter {
                        let events_ref = events.clone();
                        let exporter = Arc::clone(exporter);
                        let decoder = Arc::clone(&self.decoder);
                        tokio::task::spawn_blocking(move || {
                            if let Err(e) = exporter.write_events(&events_ref, &decoder) {
                                tracing::warn!(error = %e, "CSV export failed");
                            }
                        });
                    }

                    if let Some(ref sender) = self.webhook_sender {
                        let payloads: Vec<_> = events.iter()
                            .filter_map(|e| self.decoder.decode_raw_event(e))
                            .map(|d| crate::queue::webhook::WebhookEventPayload::from_decoded(&d))
                            .collect();
                        if !payloads.is_empty() {
                            sender.send_events(&payloads).await;
                        }
                    }
                }

                // Record metrics
                let cid = self.config.chain.id;
                crate::metrics::blocks_indexed(cid, "live", 1);
                crate::metrics::events_stored(cid, inserted);
                crate::metrics::sync_current_block(cid, block_data.block.number);
                crate::metrics::sync_chain_tip(cid, latest);
                crate::metrics::consecutive_errors(cid, 0);

                if let Some(ref mut publisher) = publisher {
                    publisher
                        .publish_live_block(
                            block_data.block.number as i64,
                            &block_data.block.hash,
                            inserted,
                        )
                        .await?;
                }

                // Snapshot view functions at this block if configured
                if let Some(ref view_indexer) = self.view_indexer {
                    if let Err(e) = view_indexer
                        .snapshot_at_block(block_data.block.number, block_data.block.timestamp as i64)
                        .await
                    {
                        warn!(
                            chain_id,
                            block = block_data.block.number,
                            error = %e,
                            "View function snapshot failed"
                        );
                    }
                }

                // Index call traces for this block if configured
                if let Some(ref trace_syncer) = self.trace_syncer {
                    if let Err(e) = trace_syncer.index_block(block_data.block.number).await {
                        warn!(
                            chain_id,
                            block = block_data.block.number,
                            error = %e,
                            "Trace indexing failed for block"
                        );
                    }
                }

                // Index account events (transactions) for this block if configured
                if let Some(ref account_syncer) = self.account_syncer {
                    if let Err(e) = account_syncer.index_block(block_data.block.number).await {
                        warn!(
                            chain_id,
                            block = block_data.block.number,
                            error = %e,
                            "Account event indexing failed for block"
                        );
                    }
                }

                if inserted > 0 {
                    debug!(
                        chain_id,
                        block = block_data.block.number,
                        events = inserted,
                        pending_blocks = latest.saturating_sub(block_data.block.number),
                        "Processed live block"
                    );
                } else {
                    debug!(
                        chain_id,
                        block = block_data.block.number,
                        "Processed live block (no events)"
                    );
                }

                current_block = block_data.block.number + 1;
            } else {
                empty_block_retries += 1;
                if empty_block_retries >= 30 {
                    warn!(
                        chain_id,
                        current_block,
                        "Block unavailable after 30 retries, advancing"
                    );
                    empty_block_retries = 0;
                    current_block += 1;
                } else {
                    debug!(
                        chain_id,
                        current_block,
                        retry = empty_block_retries,
                        "Block not available yet, waiting"
                    );
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    continue;
                }
            }
        }

        Ok(())
    }

    /// Create a block source based on the config.
    /// If `fallback_rpc` is configured, wraps the primary source with a
    /// `FallbackBlockSource` that switches to the fallback RPC on failure.
    async fn create_block_source(&self) -> Result<Box<dyn BlockSource>> {
        let retry_config = self.config.sync.retry.clone();
        let chain_id = self.config.chain.id;
        let blocks_per_request = self.config.sync.blocks_per_request;

        debug!(
            chain_id,
            source_type = self.config.source.source_type(),
            max_retries = retry_config.max_retries,
            validate_logs_bloom = retry_config.validate_logs_bloom,
            bloom_validation_retries = retry_config.bloom_validation_retries,
            initial_backoff_ms = retry_config.initial_backoff_ms,
            max_backoff_ms = retry_config.max_backoff_ms,
            "Creating block source"
        );

        match &self.config.source {
            crate::config::SourceConfig::Rpc { url, fallback_rpc, .. } => {
                let mut source = crate::sources::rpc::RpcBlockSource::with_retry_config(
                    url,
                    chain_id,
                    blocks_per_request,
                    retry_config.clone(),
                )
                .await?;
                if let Some(c) = &self.concurrency {
                    source = source.with_concurrency(Arc::clone(c));
                }
                let primary: Box<dyn BlockSource> = Box::new(source);
                self.maybe_wrap_with_fallback(primary, fallback_rpc.as_deref(), chain_id, blocks_per_request, retry_config).await
            }
            crate::config::SourceConfig::Erpc { url, project_id, fallback_rpc, .. } => {
                let source = crate::sources::erpc::ErpcBlockSource::with_retry_config(
                    url,
                    chain_id,
                    blocks_per_request,
                    retry_config.clone(),
                    project_id.as_deref(),
                )
                .await?;
                let primary: Box<dyn BlockSource> = Box::new(source);
                self.maybe_wrap_with_fallback(primary, fallback_rpc.as_deref(), chain_id, blocks_per_request, retry_config).await
            }
            crate::config::SourceConfig::Hypersync {
                url,
                api_token,
                fallback_rpc,
                ..
            } => {
                let source =
                    crate::sources::hypersync::HyperSyncBlockSource::with_retry_config(
                        url.as_deref(),
                        api_token.as_deref(),
                        fallback_rpc.as_deref(),
                        chain_id,
                        blocks_per_request,
                        retry_config.clone(),
                    )
                    .await?;
                let primary: Box<dyn BlockSource> = Box::new(source);
                self.maybe_wrap_with_fallback(primary, fallback_rpc.as_deref(), chain_id, blocks_per_request, retry_config).await
            }
        }
    }

    /// Wrap a primary source with a fallback RPC source if `fallback_rpc` is configured.
    /// Returns the primary source unwrapped if no fallback is set.
    async fn maybe_wrap_with_fallback(
        &self,
        primary: Box<dyn BlockSource>,
        fallback_rpc: Option<&str>,
        chain_id: u32,
        blocks_per_request: u64,
        retry_config: crate::config::RetryConfig,
    ) -> Result<Box<dyn BlockSource>> {
        match fallback_rpc {
            Some(url) => {
                debug!(
                    chain_id,
                    fallback_rpc = url,
                    primary = primary.source_type(),
                    "Creating fallback RPC source"
                );
                let fallback = crate::sources::rpc::RpcBlockSource::with_retry_config(
                    url,
                    chain_id,
                    blocks_per_request,
                    retry_config,
                )
                .await?;
                Ok(Box::new(crate::sources::fallback::FallbackBlockSource::new(
                    primary,
                    Box::new(fallback),
                    chain_id,
                )))
            }
            None => Ok(primary),
        }
    }

    /// Build a log filter from configured contracts + known factory children from DB
    /// Build a log filter with static + factory addresses only (for historic Phase 1).
    async fn build_log_filter(&self) -> Result<LogFilter> {
        self.build_log_filter_inner(false).await
    }

    /// Build a log filter including all known factory children (for live sync).
    async fn build_live_log_filter(&self) -> Result<LogFilter> {
        self.build_log_filter_inner(true).await
    }

    async fn build_log_filter_inner(&self, include_children: bool) -> Result<LogFilter> {
        let chain_id = self.config.chain.id as i32;
        let mut addresses = Vec::new();

        for contract in &self.config.contracts {
            if let Some(addr) = &contract.address {
                addresses.push(addr.to_lowercase());
            }
            if let Some(factory) = &contract.factory {
                addresses.push(factory.address.to_lowercase());
            }
        }

        let mut child_count = 0usize;
        if include_children {
            let factory_repo = FactoryRepository::new(
                self.pool.clone(),
                self.config.schema.data_schema.clone(),
            );
            let children_map = factory_repo.get_all_children(chain_id).await?;
            for child_addrs in children_map.values() {
                for addr in child_addrs {
                    addresses.push(addr.clone());
                    child_count += 1;
                }
            }
        }

        let deduped: Vec<String> = addresses
            .into_iter()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        debug!(
            total_addresses = deduped.len(),
            factory_children = child_count,
            include_children,
            "Built log filter"
        );

        Ok(LogFilter { addresses: deduped, topics: Vec::new() })
    }

    /// Get the event topic0 signatures for factory child contracts (for backfill filtering)
    fn child_event_signatures(&self) -> Vec<String> {
        let mut sigs = Vec::new();
        for contract in &self.config.contracts {
            if let Some(factory) = &contract.factory {
                let child_name = factory
                    .child_contract_name
                    .as_deref()
                    .unwrap_or(&contract.name);
                if let Some(events) = self.decoder.get_events(child_name) {
                    for event in events {
                        sigs.push(event.signature.clone());
                    }
                }
            }
        }
        sigs
    }

    /// Compute the oldest (minimum) start_block across all configured contracts.
    /// Falls back to sync.start_block if no per-contract start_block is set.
    fn compute_oldest_start_block(&self) -> u64 {
        let global_start = self.config.sync.start_block;
        self.config
            .contracts
            .iter()
            .filter_map(|c| c.start_block)
            .min()
            .unwrap_or(global_start)
            .min(global_start)
    }

    /// Strategy 1: Single-worker historic sync with dynamic filter.
    ///
    /// Unlike multi-worker mode which uses a static filter + Phase 2 backfill,
    /// single-worker mode processes blocks sequentially and updates the LogFilter
    /// in-place whenever new factory children are discovered. This means child
    /// contract events are picked up immediately in the very next batch — no
    /// second pass required.
    async fn run_single_worker_sync(
        &self,
        start_block: u64,
        target_block: u64,
        source: Box<dyn BlockSource>,
        original_start_block: Option<u64>,
    ) -> Result<()> {
        let chain_id = self.config.chain.id as i32;
        let to_block = target_block - 1;

        let factory_watcher = FactoryWatcher::new(&self.config.contracts, &self.decoder, chain_id)?;
        let has_factories = factory_watcher.has_factories();

        info!(
            chain_id,
            start_block,
            to_block,
            has_factories,
            strategy = "single_worker_dynamic",
            ">> Starting single-worker sync (dynamic filter)"
        );

        let event_repo = EventRepository::new(self.pool.clone(), self.config.schema.data_schema.clone());
        let decoded_repo = DecodedEventRepository::new(self.pool.clone(), self.config.schema.sync_schema.clone());
        let worker_repo = WorkerRepository::new(self.pool.clone(), self.config.schema.sync_schema.clone());
        let factory_repo = FactoryRepository::new(self.pool.clone(), self.config.schema.data_schema.clone());
        let decoder = Arc::clone(&self.decoder);

        let mut publisher = if let Some(ref redis) = self.redis {
            Some(EventPublisher::new(redis, chain_id).await?)
        } else {
            None
        };

        let blocks_per_request = self.config.sync.blocks_per_request;
        let checkpoint_interval = self.config.sync.checkpoint_interval;
        let batch_notification_interval = self.config.redis.as_ref()
            .map_or(1000, |r| r.batch_notification_interval);
        let addresses_per_batch = self.config.sync.addresses_per_batch;
        let safe_indexing = self.config.sync.safe_indexing;

        // Dynamic filter: starts with configured addresses + known children from DB.
        // New factory children are added to this filter as they are discovered.
        let mut log_filter = self.build_live_log_filter().await?;

        // Set up worker record
        worker_repo.set_worker(&SyncWorker {
            chain_id,
            worker_id: 1,
            range_start: start_block as i64,
            range_end: Some(to_block as i64),
            current_block: start_block as i64,
            status: SyncWorkerStatus::Historical,
        }).await?;

        let mut current = start_block;
        let mut total_events = 0u64;
        let mut total_factory_children = 0u64;
        let mut blocks_since_checkpoint = 0u64;
        let mut blocks_since_notification = 0u64;
        let mut consecutive_errors = 0u32;
        let max_consecutive_errors = 10u32;

        // Use original start block for progress % so it reflects the full range, not just the resume range
        let progress_start = original_start_block.unwrap_or(start_block);
        let total_range = to_block.saturating_sub(progress_start) as f64;
        let mut last_log = std::time::Instant::now();
        let log_interval = std::time::Duration::from_secs(10);

        // Fetch first batch before entering the loop.
        // Subsequent fetches are overlapped with inserts via tokio::join!.
        let mut batch_end = std::cmp::min(current + blocks_per_request - 1, to_block);
        let mut pending_fetch: Option<Result<Vec<crate::types::BlockWithLogs>>> = Some(
            fetch_blocks_with_addr_batching(
                &*source, BlockRange { from: current, to: batch_end }, &log_filter, addresses_per_batch,
            ).await
        );

        while current <= to_block {
            if self.cancel.is_cancelled() {
                worker_repo.set_worker(&SyncWorker {
                    chain_id,
                    worker_id: 1,
                    range_start: start_block as i64,
                    range_end: Some(to_block as i64),
                    current_block: current as i64,
                    status: SyncWorkerStatus::Historical,
                }).await?;
                break;
            }

            // Take the pending fetch result (always Some at this point)
            let current_fetch_result = pending_fetch.take().unwrap();

            match current_fetch_result {
                Ok(blocks) => {
                    if consecutive_errors > 0 {
                        info!(chain_id, previous_errors = consecutive_errors, "Recovered after errors");
                    }
                    consecutive_errors = 0;

                    // Dynamic factory child discovery first (read-only borrow of blocks).
                    // Discover children and immediately add them to the filter
                    // so the next fetch includes their events.
                    if has_factories {
                        let mut new_children = Vec::new();
                        for block_data in &blocks {
                            new_children.extend(factory_watcher.process_logs(&block_data.logs));
                        }
                        let new_children = new_children;
                        if !new_children.is_empty() {
                            for child in &new_children {
                                debug!(
                                    chain_id,
                                    factory = %child.factory_address,
                                    child = %child.child_address,
                                    contract = %child.contract_name,
                                    block = child.created_at_block,
                                    tx = %child.created_at_tx_hash,
                                    log_index = child.created_at_log_index,
                                    "Factory child discovered"
                                );
                            }

                            factory_repo.insert_batch(&new_children).await?;

                            // Update filter in-place — next fetch will include these addresses
                            for child in &new_children {
                                log_filter.add_address(&child.child_address);
                            }

                            total_factory_children += new_children.len() as u64;

                            // debug!(
                            //     chain_id,
                            //     new_children = new_children.len(),
                            //     total_factory_children,
                            //     total_addresses = log_filter.addresses.len(),
                            //     block_range = format!("{}-{}", current, batch_end),
                            //     "[PHASE: HISTORIC] Discovered factory children — filter updated"
                            // );
                        }
                    }

                    // Convert logs to raw events (consuming blocks — moves strings, no cloning)
                    let cap: usize = blocks.iter().map(|b| b.logs.len()).sum();
                    let mut batch_events = Vec::with_capacity(cap);
                    for block_data in blocks {
                        for log in block_data.logs {
                            batch_events.push(log.into_raw_event(chain_id));
                        }
                    }

                    // Determine if there's a next batch to prefetch
                    let next_current = batch_end + 1;
                    let has_more = next_current <= to_block && !self.cancel.is_cancelled();

                    if !batch_events.is_empty() {
                        if has_more {
                            // Overlap: insert current batch + prefetch next batch concurrently.
                            // Factory filter is already updated, so next fetch sees new addresses.
                            let next_batch_end = std::cmp::min(next_current + blocks_per_request - 1, to_block);
                            let next_range = BlockRange { from: next_current, to: next_batch_end };

                            let insert_fut = insert_events_with_fallback(
                                &batch_events, &event_repo, &decoded_repo, &decoder,
                                self.filter_engine.as_deref(), safe_indexing,
                            );
                            let fetch_fut = fetch_blocks_with_addr_batching(
                                &*source, next_range, &log_filter, addresses_per_batch,
                            );
                            let (insert_result, next_fetch) = tokio::join!(insert_fut, fetch_fut);

                            let inserted = insert_result?;
                            pending_fetch = Some(next_fetch);

                            // CSV/webhook export
                            let shared_events = if self.csv_exporter.is_some() || self.webhook_sender.is_some() {
                                Some(Arc::new(batch_events))
                            } else {
                                None
                            };

                            if let Some(ref exporter) = self.csv_exporter {
                                let events_ref = Arc::clone(shared_events.as_ref().unwrap());
                                let exporter = Arc::clone(exporter);
                                let decoder_clone = Arc::clone(&decoder);
                                tokio::task::spawn_blocking(move || {
                                    if let Err(e) = exporter.write_events(&events_ref, &decoder_clone) {
                                        tracing::warn!(error = %e, "CSV export failed");
                                    }
                                });
                            }

                            if let Some(ref sender) = self.webhook_sender {
                                let events_ref = shared_events.as_ref().unwrap();
                                let payloads: Vec<_> = events_ref.iter()
                                    .filter_map(|e| decoder.decode_raw_event(e))
                                    .map(|d| crate::queue::webhook::WebhookEventPayload::from_decoded(&d))
                                    .collect();
                                if !payloads.is_empty() {
                                    sender.send_events(&payloads).await;
                                }
                            }

                            total_events += inserted;
                            batch_end = next_batch_end;
                        } else {
                            // Last batch — just insert, no more fetching needed
                            let inserted = insert_events_with_fallback(
                                &batch_events, &event_repo, &decoded_repo, &decoder,
                                self.filter_engine.as_deref(), safe_indexing,
                            ).await?;

                            let shared_events = if self.csv_exporter.is_some() || self.webhook_sender.is_some() {
                                Some(Arc::new(batch_events))
                            } else {
                                None
                            };

                            if let Some(ref exporter) = self.csv_exporter {
                                let events_ref = Arc::clone(shared_events.as_ref().unwrap());
                                let exporter = Arc::clone(exporter);
                                let decoder_clone = Arc::clone(&decoder);
                                tokio::task::spawn_blocking(move || {
                                    if let Err(e) = exporter.write_events(&events_ref, &decoder_clone) {
                                        tracing::warn!(error = %e, "CSV export failed");
                                    }
                                });
                            }

                            if let Some(ref sender) = self.webhook_sender {
                                let events_ref = shared_events.as_ref().unwrap();
                                let payloads: Vec<_> = events_ref.iter()
                                    .filter_map(|e| decoder.decode_raw_event(e))
                                    .map(|d| crate::queue::webhook::WebhookEventPayload::from_decoded(&d))
                                    .collect();
                                if !payloads.is_empty() {
                                    sender.send_events(&payloads).await;
                                }
                            }

                            total_events += inserted;
                        }
                    } else if has_more {
                        // Empty batch — just prefetch the next one
                        let next_batch_end = std::cmp::min(next_current + blocks_per_request - 1, to_block);
                        pending_fetch = Some(fetch_blocks_with_addr_batching(
                            &*source, BlockRange { from: next_current, to: next_batch_end },
                            &log_filter, addresses_per_batch,
                        ).await);
                        batch_end = next_batch_end;
                    }

                    // next_current = original batch_end + 1, so this correctly counts the current batch
                    let blocks_processed = next_current - current;
                    crate::metrics::blocks_indexed(chain_id as u32, "historic", blocks_processed);
                    crate::metrics::sync_current_block(chain_id as u32, next_current - 1);
                    blocks_since_checkpoint += blocks_processed;
                    blocks_since_notification += blocks_processed;
                    current = next_current;
                }
                Err(e) => {
                    consecutive_errors += 1;
                    let backoff_secs = std::cmp::min(2u64.saturating_pow(consecutive_errors), 60);

                    warn!(
                        chain_id,
                        from_block = current,
                        to_block = batch_end,
                        consecutive_errors,
                        max_consecutive_errors,
                        backoff_secs,
                        error = %e,
                        "[ERROR] Fetch failed, retrying with backoff"
                    );

                    if consecutive_errors >= max_consecutive_errors {
                        return Err(e).context(format!(
                            "Single worker aborted after {} consecutive errors at block {}",
                            consecutive_errors, current
                        ));
                    }

                    tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;

                    // Re-fetch the same range after backoff
                    pending_fetch = Some(fetch_blocks_with_addr_batching(
                        &*source, BlockRange { from: current, to: batch_end },
                        &log_filter, addresses_per_batch,
                    ).await);
                    continue;
                }
            }

            // Checkpoint
            if blocks_since_checkpoint >= checkpoint_interval {
                worker_repo.set_worker(&SyncWorker {
                    chain_id,
                    worker_id: 1,
                    range_start: start_block as i64,
                    range_end: Some(to_block as i64),
                    current_block: current as i64,
                    status: SyncWorkerStatus::Historical,
                }).await?;
                blocks_since_checkpoint = 0;
            }

            // Batch notifications
            if blocks_since_notification >= batch_notification_interval {
                if let Some(ref mut pub_ref) = publisher {
                    let notification_start = current - blocks_since_notification;
                    pub_ref
                        .publish_event_batch(
                            notification_start as i64,
                            current as i64,
                            total_events,
                        )
                        .await?;
                }
                blocks_since_notification = 0;
            }

            // Progress logging
            if last_log.elapsed() >= log_interval {
                let pct = if total_range > 0.0 {
                    (current.saturating_sub(progress_start) as f64 / total_range * 100.0).min(100.0)
                } else {
                    100.0
                };
                info!(
                    chain_id,
                    progress = format!("{:.1}%", pct),
                    block = current,
                    total_events,
                    factory_children = total_factory_children,
                    addresses = log_filter.addresses.len(),
                    ">> Sync progress"
                );
                last_log = std::time::Instant::now();
            }
        }

        // Final checkpoint
        worker_repo.set_worker(&SyncWorker {
            chain_id,
            worker_id: 1,
            range_start: start_block as i64,
            range_end: Some(to_block as i64),
            current_block: to_block as i64,
            status: SyncWorkerStatus::Historical,
        }).await?;

        info!(
            chain_id,
            total_events,
            factory_children = total_factory_children,
            ">> Single-worker sync complete"
        );

        Ok(())
    }

    /// Strategy 2: Multi-worker two-step parallel sync.
    /// Step 1: Split [oldest_start, current_block] across parallel_workers.
    ///         Each worker fetches ALL contracts + known factory children.
    /// Step 2: Backfill newly discovered factory children using the same
    ///         splitting strategy with address batching within workers.
    async fn run_multi_worker_sync(
        &self,
        start_block: u64,
        target_block: u64,
        source: Box<dyn BlockSource>,
    ) -> Result<()> {
        let chain_id = self.config.chain.id as i32;
        let effective_start = self.compute_oldest_start_block().min(start_block);

        info!(
            chain_id,
            effective_start,
            target_block,
            parallel_workers = self.config.sync.parallel_workers,
            strategy = "multi_worker",
            ">> Starting multi-worker two-step sync"
        );

        let chunks = divide_into_chunks(
            effective_start,
            target_block,
            self.config.sync.parallel_workers,
            self.config.sync.blocks_per_worker,
        );

        debug!(
            chain_id,
            worker_count = chunks.len(),
            "Created historic sync workers"
        );

        let worker_repo = WorkerRepository::new(
            self.pool.clone(),
            self.config.schema.sync_schema.clone(),
        );
        for chunk in &chunks {
            let worker = SyncWorker {
                chain_id,
                worker_id: chunk.worker_id,
                range_start: chunk.from_block as i64,
                range_end: Some(chunk.to_block as i64),
                current_block: chunk.from_block as i64,
                status: SyncWorkerStatus::Historical,
            };
            worker_repo.set_worker(&worker).await?;
        }

        let source = Arc::new(source);
        let factory_watcher = FactoryWatcher::new(&self.config.contracts, &self.decoder, chain_id)?;
        let has_factories = factory_watcher.has_factories();

        if has_factories {
            // Two-phase sync: events first, then factory children backfill
            info!(chain_id, workers = chunks.len(), "[PHASE: HISTORIC 1/2] Indexing events (parallel workers)");
            self.run_historic_workers(chunks, source.clone(), "Phase 1/2: Historic sync").await?;

            if !self.cancel.is_cancelled() {
                info!(chain_id, "[PHASE: HISTORIC 2/2] Backfilling factory children");
                self.backfill_all_factory_children(source).await?;
            }
        } else {
            // Single-phase sync: no factories, just index events
            info!(chain_id, workers = chunks.len(), " Indexing events (parallel workers)");
            self.run_historic_workers(chunks, source.clone(), "Historic sync").await?;
        }

        Ok(())
    }

    /// Phase 2 of historic sync: backfill events for all factory children discovered
    /// during Phase 1. Collects ALL child addresses into a single HyperSync filter
    /// and sweeps the block range once, splitting work across parallel workers by
    /// block-range chunks (same strategy as Phase 1). This avoids redundant passes
    /// over the same blocks.
    async fn backfill_all_factory_children(
        &self,
        source: Arc<Box<dyn BlockSource>>,
    ) -> Result<()> {
        let chain_id = self.config.chain.id as i32;
        let factory_repo = FactoryRepository::new(
            self.pool.clone(),
            self.config.schema.data_schema.clone(),
        );

        let children = factory_repo.get_all_children_with_blocks(chain_id).await?;
        if children.is_empty() {
            return Ok(());
        }

        let event_repo = Arc::new(EventRepository::new(
            self.pool.clone(),
            self.config.schema.data_schema.clone(),
        ));
        let decoded_repo = Arc::new(DecodedEventRepository::new(
            self.pool.clone(),
            self.config.schema.sync_schema.clone(),
        ));

        let latest_block = event_repo
            .get_latest_block(chain_id)
            .await?
            .unwrap_or(0) as u64;

        let children_to_backfill: Vec<_> = children
            .into_iter()
            .filter(|c| (c.created_at_block as u64) < latest_block)
            .collect();

        if children_to_backfill.is_empty() {
            return Ok(());
        }

        let earliest_block = children_to_backfill
            .iter()
            .map(|c| c.created_at_block as u64)
            .min()
            .unwrap_or(0);

        let all_addresses: Vec<String> = children_to_backfill
            .iter()
            .map(|c| c.child_address.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        let topic_sigs = self.child_event_signatures();
        let topic_filter_opts: Vec<Option<String>> = topic_sigs
            .iter()
            .map(|t| Some(t.clone()))
            .collect();

        let total_children = children_to_backfill.len();
        let unique_count = all_addresses.len();

        let addresses_per_batch = self.config.sync.addresses_per_batch;
        let addr_batches: Vec<Vec<String>> = all_addresses
            .chunks(addresses_per_batch)
            .map(|c| c.to_vec())
            .collect();

        info!(
            chain_id,
            total_children,
            unique_addresses = unique_count,
            address_batches = addr_batches.len(),
            earliest_block,
            target_block = latest_block,
            "[PHASE: HISTORIC 2/2] Factory children backfill starting (parallel)"
        );

        let blocks_per_request = self.config.sync.blocks_per_request;
        let parallel_workers = self.config.sync.parallel_workers;
        let decoder = Arc::clone(&self.decoder);
        let cancel = self.cancel.clone();
        let total_events = Arc::new(AtomicU64::new(0));
        let semaphore = Arc::new(tokio::sync::Semaphore::new(parallel_workers as usize));

        // Build all chunks across all address batches for progress tracking
        let all_chunks: Vec<(usize, Vec<WorkerChunk>)> = addr_batches
            .iter()
            .enumerate()
            .map(|(batch_idx, _)| {
                let chunks = divide_into_chunks(
                    earliest_block,
                    latest_block,
                    parallel_workers,
                    self.config.sync.blocks_per_worker,
                );
                (batch_idx, chunks)
            })
            .collect();

        // Flatten chunks for progress tracking
        let flat_chunks: Vec<WorkerChunk> = all_chunks
            .iter()
            .flat_map(|(_, chunks)| chunks.iter().cloned())
            .collect();

        let progress = Arc::new(WorkerProgress::new(&flat_chunks, earliest_block, latest_block));
        let reporter_cancel = CancellationToken::new();
        let reporter = spawn_progress_reporter(
            chain_id,
            Arc::clone(&progress),
            reporter_cancel.clone(),
            "Phase 2/2",
            10,
        );

        let mut handles = Vec::new();
        let mut flat_idx = 0usize;

        for (batch_idx, addr_batch) in addr_batches.into_iter().enumerate() {
            let filter = Arc::new(LogFilter {
                addresses: addr_batch,
                topics: topic_filter_opts.clone(),
            });

            let chunks = divide_into_chunks(
                earliest_block,
                latest_block,
                parallel_workers,
                self.config.sync.blocks_per_worker,
            );

            for chunk in chunks {
                let source = Arc::clone(&source);
                let event_repo = Arc::clone(&event_repo);
                let decoded_repo = Arc::clone(&decoded_repo);
                let decoder = Arc::clone(&decoder);
                let cancel = cancel.clone();
                let total_events = Arc::clone(&total_events);
                let filter = Arc::clone(&filter);
                let filter_engine = self.filter_engine.clone();
                let semaphore = Arc::clone(&semaphore);
                let safe_indexing = self.config.sync.safe_indexing;
                let progress = Arc::clone(&progress);
                let worker_progress_idx = flat_idx;
                flat_idx += 1;

                let handle = tokio::spawn(async move {
                    let _permit = semaphore.acquire().await.unwrap();
                    let mut current = chunk.from_block;
                    let mut worker_events = 0u64;

                    while current <= chunk.to_block {
                        if cancel.is_cancelled() {
                            break;
                        }

                        let batch_end = std::cmp::min(
                            current + blocks_per_request - 1,
                            chunk.to_block,
                        );
                        let range = BlockRange {
                            from: current,
                            to: batch_end,
                        };

                        match source.get_blocks(range, &filter).await {
                            Ok(blocks) => {
                                let mut events = Vec::new();
                                for block_data in &blocks {
                                    for log in &block_data.logs {
                                        events.push(log.to_raw_event(chain_id));
                                    }
                                }
                                if !events.is_empty() {
                                    match insert_events_with_fallback(
                                        &events,
                                        &event_repo,
                                        &decoded_repo,
                                        &decoder,
                                        filter_engine.as_deref(),
                                        safe_indexing,
                                    )
                                    .await
                                    {
                                        Ok(inserted) => {
                                            worker_events += inserted;
                                        }
                                        Err(e) => {
                                            warn!(
                                                chain_id,
                                                batch_idx,
                                                worker_id = chunk.worker_id,
                                                error = %e,
                                                "Phase 2/2: failed to insert events"
                                            );
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                warn!(
                                    chain_id,
                                    batch_idx,
                                    worker_id = chunk.worker_id,
                                    from = current,
                                    to = batch_end,
                                    error = %e,
                                    "Phase 2/2: failed to fetch blocks, skipping range"
                                );
                            }
                        }

                        let blocks_processed = batch_end - current + 1;
                        current = batch_end + 1;

                        // Update shared progress
                        progress.update(worker_progress_idx, current, worker_events);

                        // Still respect checkpoint_interval for not updating too often
                        let _ = blocks_processed;
                    }

                    total_events.fetch_add(worker_events, Ordering::Relaxed);
                });
                handles.push(handle);
            }
        }

        for handle in handles {
            handle.await?;
        }

        reporter_cancel.cancel();
        let _ = reporter.await;

        let events = total_events.load(Ordering::Relaxed);
        info!(
            chain_id,
            total_children,
            total_events = events,
            "[PHASE: HISTORIC 2/2] Factory children backfill complete"
        );

        Ok(())
    }
}


/// A fetched batch of block data ready for insertion
struct FetchedBatch {
    events: Vec<crate::types::RawEventRecord>,
    logs: Vec<crate::types::LogEntry>,
    range_start: u64,
    range_end: u64,
}

/// Check if an error is a PostgreSQL unique_violation (23505).
fn is_unique_violation(err: &anyhow::Error) -> bool {
    for cause in err.chain() {
        if let Some(sqlx::Error::Database(ref db_err)) = cause.downcast_ref::<sqlx::Error>() {
            if db_err.code().map_or(false, |c| c == "23505") {
                return true;
            }
        }
    }
    false
}

/// Insert events using direct COPY, falling back to staging+ON CONFLICT on unique violation
/// when safe_indexing is enabled. Skips the batch otherwise (data already committed).
async fn insert_events_with_fallback(
    events: &[crate::types::RawEventRecord],
    event_repo: &EventRepository,
    decoded_repo: &DecodedEventRepository,
    decoder: &AbiDecoder,
    filter_engine: Option<&crate::filter::EventFilterEngine>,
    safe_indexing: bool,
) -> Result<u64> {
    if events.is_empty() {
        return Ok(0);
    }

    let mut tx = event_repo.pool().begin().await?;
    match event_repo.insert_batch_direct_in_tx(events, &mut tx).await {
        Ok(_) => {
            decoded_repo
                .insert_batch_direct_in_tx_filtered(events, decoder, &mut tx, filter_engine)
                .await?;
            tx.commit().await?;
            Ok(events.len() as u64)
        }
        Err(e) if is_unique_violation(&e) => {
            tx.rollback().await.ok();
            if safe_indexing {
                debug!("Direct COPY hit unique violation, falling back to staging+ON CONFLICT");
                let mut tx = event_repo.pool().begin().await?;
                let inserted = event_repo.insert_batch_in_tx(events, &mut tx).await?;
                decoded_repo
                    .insert_batch_in_tx_filtered(events, decoder, &mut tx, filter_engine)
                    .await?;
                tx.commit().await?;
                Ok(inserted)
            } else {
                debug!(
                    events = events.len(),
                    "Batch skipped: data already exists (unique violation)"
                );
                Ok(0)
            }
        }
        Err(e) => {
            tx.rollback().await.ok();
            Err(e)
        }
    }
}

/// Merge block results from parallel address-batch fetches.
/// Combines logs from the same block number into a single BlockWithLogs.
fn merge_block_results(batches: Vec<Vec<crate::types::BlockWithLogs>>) -> Vec<crate::types::BlockWithLogs> {
    use std::collections::BTreeMap;
    let mut merged: BTreeMap<u64, crate::types::BlockWithLogs> = BTreeMap::new();
    for batch in batches {
        for block in batch {
            match merged.entry(block.block.number) {
                std::collections::btree_map::Entry::Occupied(mut e) => {
                    e.get_mut().logs.extend(block.logs); // move, not clone
                }
                std::collections::btree_map::Entry::Vacant(e) => {
                    e.insert(block);
                }
            }
        }
    }
    merged.into_values().collect()
}

/// Fetch blocks with address batching. When the filter has more addresses than
/// `addresses_per_batch`, splits into sequential sub-batches (each up to
/// `addresses_per_batch` addresses), fetches them one at a time, and merges
/// the results. This avoids overwhelming the source with too many addresses
/// in a single request while keeping concurrency bounded.
async fn fetch_blocks_with_addr_batching(
    source: &(dyn BlockSource + '_),
    range: BlockRange,
    filter: &LogFilter,
    addresses_per_batch: usize,
) -> Result<Vec<crate::types::BlockWithLogs>> {
    if addresses_per_batch == 0 || filter.addresses.len() <= addresses_per_batch {
        return source.get_blocks(range, filter).await;
    }

    let batch_filters: Vec<LogFilter> = filter
        .addresses
        .chunks(addresses_per_batch)
        .map(|chunk| LogFilter {
            addresses: chunk.to_vec(),
            topics: filter.topics.clone(),
        })
        .collect();

    let futures: Vec<_> = batch_filters.iter().map(|bf| {
        source.get_blocks(BlockRange { from: range.from, to: range.to }, bf)
    }).collect();
    let all_batches = futures::future::try_join_all(futures).await?;

    Ok(merge_block_results(all_batches))
}

/// Run a single historic sync worker.
/// When `pipeline_buffer > 0`, fetching and inserting run concurrently via a bounded channel.
/// When `pipeline_buffer == 0`, fetching and inserting are sequential (original behavior).
async fn run_historic_worker(
    chain_id: i32,
    chunk: WorkerChunk,
    source: Arc<Box<dyn BlockSource>>,
    event_repo: Arc<EventRepository>,
    decoded_repo: Arc<DecodedEventRepository>,
    worker_repo: Arc<WorkerRepository>,
    factory_repo: Arc<FactoryRepository>,
    publisher: Option<Arc<Mutex<EventPublisher>>>,
    factory_watcher: Arc<FactoryWatcher>,
    decoder: Arc<AbiDecoder>,
    filter: LogFilter,
    blocks_per_request: u64,
    checkpoint_interval: u64,
    batch_notification_interval: u64,
    pipeline_buffer: usize,
    cancel: CancellationToken,
    csv_exporter: Option<Arc<crate::export::CsvExporter>>,
    filter_engine: Option<Arc<crate::filter::EventFilterEngine>>,
    webhook_sender: Option<Arc<crate::queue::webhook::WebhookSender>>,
    safe_indexing: bool,
    addresses_per_batch: usize,
    shared_progress: Option<(Arc<WorkerProgress>, usize)>,
) -> Result<()> {
    let has_factories = factory_watcher.has_factories();

    debug!(
        chain_id,
        worker_id = chunk.worker_id,
        from = chunk.from_block,
        to = chunk.to_block,
        "Historic worker starting"
    );

    if pipeline_buffer == 0 {
        return run_historic_worker_sequential(
            chain_id, chunk, source, event_repo, decoded_repo, worker_repo,
            factory_repo, publisher, factory_watcher,
            decoder, filter, blocks_per_request, checkpoint_interval,
            batch_notification_interval, cancel, csv_exporter, filter_engine,
            webhook_sender, safe_indexing, addresses_per_batch, shared_progress,
        ).await;
    }

    // Pipeline mode: fetcher and inserter run concurrently
    let (tx, mut rx) = tokio::sync::mpsc::channel::<FetchedBatch>(pipeline_buffer);

    let fetch_cancel = cancel.clone();
    let fetch_chunk = chunk.clone();
    let fetch_source = Arc::clone(&source);
    let fetcher = tokio::spawn(async move {
        let mut current = fetch_chunk.from_block;
        let mut consecutive_errors = 0u32;
        let max_consecutive_errors = 10u32;

        while current <= fetch_chunk.to_block {
            if fetch_cancel.is_cancelled() {
                break;
            }

            let batch_end = std::cmp::min(current + blocks_per_request - 1, fetch_chunk.to_block);
            let range = BlockRange {
                from: current,
                to: batch_end,
            };

            match fetch_blocks_with_addr_batching(&**fetch_source, range, &filter, addresses_per_batch).await {
                Ok(blocks) => {
                    consecutive_errors = 0;

                    let cap: usize = blocks.iter().map(|b| b.logs.len()).sum();
                    let mut all_logs = Vec::new();
                    let mut batch_events = Vec::with_capacity(cap);
                    for block_data in blocks {
                        for log in block_data.logs {
                            if has_factories {
                                all_logs.push(log.clone());
                            }
                            batch_events.push(log.into_raw_event(chain_id));
                        }
                    }

                    let batch = FetchedBatch {
                        events: batch_events,
                        logs: all_logs,
                        range_start: current,
                        range_end: batch_end,
                    };

                    if tx.send(batch).await.is_err() {
                        break;
                    }

                    current = batch_end + 1;
                }
                Err(e) => {
                    consecutive_errors += 1;
                    let backoff_secs = std::cmp::min(2u64.saturating_pow(consecutive_errors), 60);

                    warn!(
                        chain_id,
                        worker_id = fetch_chunk.worker_id,
                        from_block = current,
                        to_block = batch_end,
                        consecutive_errors,
                        backoff_secs,
                        error = %e,
                        "Historic worker fetch failed, retrying with backoff"
                    );

                    if consecutive_errors >= max_consecutive_errors {
                        return Err::<(), anyhow::Error>(e);
                    }

                    tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
                }
            }
        }

        Ok(())
    });

    // Inserter runs in the current task
    let mut blocks_since_checkpoint = 0u64;
    let mut blocks_since_notification = 0u64;
    let mut total_events = 0u64;
    let mut current = chunk.from_block;

    while let Some(batch) = rx.recv().await {
        if cancel.is_cancelled() {
            let worker = SyncWorker {
                chain_id,
                worker_id: chunk.worker_id,
                range_start: chunk.from_block as i64,
                range_end: Some(chunk.to_block as i64),
                current_block: current as i64,
                status: SyncWorkerStatus::Historical,
            };
            worker_repo.set_worker(&worker).await?;
            break;
        }

        if !batch.events.is_empty() {
            let inserted = insert_events_with_fallback(
                &batch.events,
                &event_repo,
                &decoded_repo,
                &decoder,
                filter_engine.as_deref(),
                safe_indexing,
            )
            .await?;

            if let Some(ref exporter) = csv_exporter {
                let events_ref = batch.events.clone();
                let exporter = Arc::clone(exporter);
                let decoder_clone = Arc::clone(&decoder);
                tokio::task::spawn_blocking(move || {
                    if let Err(e) = exporter.write_events(&events_ref, &decoder_clone) {
                        tracing::warn!(error = %e, "CSV export failed");
                    }
                });
            }

            if let Some(ref sender) = webhook_sender {
                let payloads: Vec<_> = batch.events.iter()
                    .filter_map(|e| decoder.decode_raw_event(e))
                    .map(|d| crate::queue::webhook::WebhookEventPayload::from_decoded(&d))
                    .collect();
                if !payloads.is_empty() {
                    sender.send_events(&payloads).await;
                }
            }

            total_events += inserted;
        }

        if has_factories && !batch.logs.is_empty() {
            let new_children = factory_watcher.process_logs(&batch.logs);
            if !new_children.is_empty() {
                debug!(
                    chain_id,
                    worker_id = chunk.worker_id,
                    new_children = new_children.len(),
                    block_range = format!("{}-{}", batch.range_start, batch.range_end),
                    "Discovered factory children"
                );

                factory_repo.insert_batch(&new_children).await?;
            }
        }

        let blocks_processed = batch.range_end - batch.range_start + 1;
        crate::metrics::blocks_indexed(chain_id as u32, "historic", blocks_processed);
        crate::metrics::sync_current_block(chain_id as u32, batch.range_end);
        blocks_since_checkpoint += blocks_processed;
        blocks_since_notification += blocks_processed;
        current = batch.range_end + 1;

        // Update shared progress after every batch (atomic, cheap)
        if let Some((ref progress, idx)) = shared_progress {
            progress.update(idx, current, total_events);
        }

        if blocks_since_checkpoint >= checkpoint_interval {
            let worker = SyncWorker {
                chain_id,
                worker_id: chunk.worker_id,
                range_start: chunk.from_block as i64,
                range_end: Some(chunk.to_block as i64),
                current_block: current as i64,
                status: SyncWorkerStatus::Historical,
            };
            worker_repo.set_worker(&worker).await?;
            blocks_since_checkpoint = 0;
        }

        if blocks_since_notification >= batch_notification_interval {
            if let Some(ref publisher) = publisher {
                let notification_start = current - blocks_since_notification;
                let mut pub_lock = publisher.lock().await;
                pub_lock
                    .publish_event_batch(
                        notification_start as i64,
                        current as i64,
                        total_events,
                    )
                    .await?;
            }
            blocks_since_notification = 0;
        }
    }

    // Wait for fetcher and propagate errors
    match fetcher.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(e),
        Err(e) => return Err(anyhow::anyhow!("Fetcher task panicked: {}", e)),
    }

    // Final checkpoint
    let worker = SyncWorker {
        chain_id,
        worker_id: chunk.worker_id,
        range_start: chunk.from_block as i64,
        range_end: Some(chunk.to_block as i64),
        current_block: chunk.to_block as i64,
        status: SyncWorkerStatus::Historical,
    };
    worker_repo.set_worker(&worker).await?;

    // Final update to shared progress
    if let Some((ref progress, idx)) = shared_progress {
        progress.update(idx, chunk.to_block, total_events);
    }

    debug!(
        chain_id,
        worker_id = chunk.worker_id,
        total_events,
        "Historic worker completed"
    );

    Ok(())
}

/// Sequential historic worker (no pipelining). Used when pipeline_buffer == 0.
async fn run_historic_worker_sequential(
    chain_id: i32,
    chunk: WorkerChunk,
    source: Arc<Box<dyn BlockSource>>,
    event_repo: Arc<EventRepository>,
    decoded_repo: Arc<DecodedEventRepository>,
    worker_repo: Arc<WorkerRepository>,
    factory_repo: Arc<FactoryRepository>,
    publisher: Option<Arc<Mutex<EventPublisher>>>,
    factory_watcher: Arc<FactoryWatcher>,
    decoder: Arc<AbiDecoder>,
    filter: LogFilter,
    blocks_per_request: u64,
    checkpoint_interval: u64,
    batch_notification_interval: u64,
    cancel: CancellationToken,
    csv_exporter: Option<Arc<crate::export::CsvExporter>>,
    filter_engine: Option<Arc<crate::filter::EventFilterEngine>>,
    webhook_sender: Option<Arc<crate::queue::webhook::WebhookSender>>,
    safe_indexing: bool,
    addresses_per_batch: usize,
    shared_progress: Option<(Arc<WorkerProgress>, usize)>,
) -> Result<()> {
    let mut current = chunk.from_block;
    let mut blocks_since_checkpoint = 0u64;
    let mut blocks_since_notification = 0u64;
    let mut total_events = 0u64;

    let has_factories = factory_watcher.has_factories();

    let mut consecutive_errors = 0u32;
    let max_consecutive_errors = 10u32;

    while current <= chunk.to_block {
        if cancel.is_cancelled() {
            let worker = SyncWorker {
                chain_id,
                worker_id: chunk.worker_id,
                range_start: chunk.from_block as i64,
                range_end: Some(chunk.to_block as i64),
                current_block: current as i64,
                status: SyncWorkerStatus::Historical,
            };
            worker_repo.set_worker(&worker).await?;
            return Ok(());
        }

        let batch_end = std::cmp::min(current + blocks_per_request - 1, chunk.to_block);
        let range = BlockRange {
            from: current,
            to: batch_end,
        };

        match fetch_blocks_with_addr_batching(&**source, range, &filter, addresses_per_batch).await {
            Ok(blocks) => {
                consecutive_errors = 0;

                // Factory child discovery first (read-only borrow of blocks)
                if has_factories {
                    let mut new_children = Vec::new();
                    for block_data in &blocks {
                        new_children.extend(factory_watcher.process_logs(&block_data.logs));
                    }
                    if !new_children.is_empty() {
                        debug!(
                            chain_id,
                            worker_id = chunk.worker_id,
                            new_children = new_children.len(),
                            block_range = format!("{}-{}", current, batch_end),
                            "Discovered factory children"
                        );

                        factory_repo.insert_batch(&new_children).await?;
                    }
                }

                // Convert logs to raw events (consuming blocks — moves strings, no cloning)
                let cap: usize = blocks.iter().map(|b| b.logs.len()).sum();
                let mut batch_events = Vec::with_capacity(cap);
                for block_data in blocks {
                    for log in block_data.logs {
                        batch_events.push(log.into_raw_event(chain_id));
                    }
                }

                if !batch_events.is_empty() {
                    let inserted = insert_events_with_fallback(
                        &batch_events,
                        &event_repo,
                        &decoded_repo,
                        &decoder,
                        filter_engine.as_deref(),
                        safe_indexing,
                    )
                    .await?;

                    // Wrap in Arc to share between CSV export and webhook without cloning
                    let shared_events = if csv_exporter.is_some() || webhook_sender.is_some() {
                        Some(Arc::new(batch_events))
                    } else {
                        None
                    };

                    if let Some(ref exporter) = csv_exporter {
                        let events_ref = Arc::clone(shared_events.as_ref().unwrap());
                        let exporter = Arc::clone(exporter);
                        let decoder_clone = Arc::clone(&decoder);
                        tokio::task::spawn_blocking(move || {
                            if let Err(e) = exporter.write_events(&events_ref, &decoder_clone) {
                                tracing::warn!(error = %e, "CSV export failed");
                            }
                        });
                    }

                    if let Some(ref sender) = webhook_sender {
                        let events_ref = shared_events.as_ref().unwrap();
                        let payloads: Vec<_> = events_ref.iter()
                            .filter_map(|e| decoder.decode_raw_event(e))
                            .map(|d| crate::queue::webhook::WebhookEventPayload::from_decoded(&d))
                            .collect();
                        if !payloads.is_empty() {
                            sender.send_events(&payloads).await;
                        }
                    }

                    total_events += inserted;
                }

                let blocks_processed = batch_end - current + 1;
                crate::metrics::blocks_indexed(chain_id as u32, "historic", blocks_processed);
                crate::metrics::sync_current_block(chain_id as u32, batch_end);
                blocks_since_checkpoint += blocks_processed;
                blocks_since_notification += blocks_processed;
                current = batch_end + 1;
            }
            Err(e) => {
                consecutive_errors += 1;
                let backoff_secs = std::cmp::min(2u64.saturating_pow(consecutive_errors), 60);

                warn!(
                    chain_id,
                    worker_id = chunk.worker_id,
                    from_block = current,
                    to_block = batch_end,
                    consecutive_errors,
                    max_consecutive_errors,
                    backoff_secs,
                    error = %e,
                    "Historic worker batch fetch failed, retrying with backoff"
                );

                if consecutive_errors >= max_consecutive_errors {
                    return Err(e);
                }

                tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
                continue;
            }
        }

        // Update shared progress after every batch (atomic, cheap)
        if let Some((ref progress, idx)) = shared_progress {
            progress.update(idx, current, total_events);
        }

        if blocks_since_checkpoint >= checkpoint_interval {
            let worker = SyncWorker {
                chain_id,
                worker_id: chunk.worker_id,
                range_start: chunk.from_block as i64,
                range_end: Some(chunk.to_block as i64),
                current_block: current as i64,
                status: SyncWorkerStatus::Historical,
            };
            worker_repo.set_worker(&worker).await?;
            blocks_since_checkpoint = 0;
        }

        if blocks_since_notification >= batch_notification_interval {
            if let Some(ref publisher) = publisher {
                let notification_start = current - blocks_since_notification;
                let mut pub_lock = publisher.lock().await;
                pub_lock
                    .publish_event_batch(
                        notification_start as i64,
                        current as i64,
                        total_events,
                    )
                    .await?;
            }
            blocks_since_notification = 0;
        }
    }

    // Final checkpoint
    let worker = SyncWorker {
        chain_id,
        worker_id: chunk.worker_id,
        range_start: chunk.from_block as i64,
        range_end: Some(chunk.to_block as i64),
        current_block: chunk.to_block as i64,
        status: SyncWorkerStatus::Historical,
    };
    worker_repo.set_worker(&worker).await?;

    // Final update to shared progress
    if let Some((ref progress, idx)) = shared_progress {
        progress.update(idx, chunk.to_block, total_events);
    }

    debug!(
        chain_id,
        worker_id = chunk.worker_id,
        total_events,
        "Historic worker completed"
    );

    Ok(())
}
/// Divide a block range into parallel worker chunks
pub fn divide_into_chunks(
    start_block: u64,
    target_block: u64,
    max_workers: u32,
    blocks_per_worker: u64,
) -> Vec<WorkerChunk> {
    let total_blocks = target_block - start_block;

    if total_blocks == 0 {
        return Vec::new();
    }

    let needed_workers = (total_blocks + blocks_per_worker - 1) / blocks_per_worker;
    let num_workers = std::cmp::min(needed_workers as u32, max_workers) as u64;

    if num_workers == 0 {
        return Vec::new();
    }

    let blocks_per_chunk = total_blocks / num_workers;
    let remainder = total_blocks % num_workers;

    let mut chunks = Vec::new();
    let mut from = start_block;

    for i in 0..num_workers {
        let extra = if i < remainder { 1 } else { 0 };
        let chunk_size = blocks_per_chunk + extra;
        let to = from + chunk_size - 1;

        chunks.push(WorkerChunk {
            worker_id: (i + 1) as i32,
            from_block: from,
            to_block: to.min(target_block),
        });

        from = to + 1;
    }

    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_divide_into_chunks_basic() {
        let chunks = divide_into_chunks(100, 1100, 4, 250_000);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].from_block, 100);
        assert_eq!(chunks[0].to_block, 1099);
        assert_eq!(chunks[0].worker_id, 1);
    }

    #[test]
    fn test_divide_into_chunks_multiple_workers() {
        let chunks = divide_into_chunks(0, 1_000_000, 4, 250_000);
        assert_eq!(chunks.len(), 4);
        assert_eq!(chunks[0].from_block, 0);
        assert_eq!(chunks.last().unwrap().to_block, 999_999);
        for i in 1..chunks.len() {
            assert_eq!(chunks[i].from_block, chunks[i - 1].to_block + 1);
        }
        for (i, chunk) in chunks.iter().enumerate() {
            assert_eq!(chunk.worker_id, (i + 1) as i32);
        }
    }

    #[test]
    fn test_divide_into_chunks_fewer_workers_than_max() {
        let chunks = divide_into_chunks(0, 500_000, 8, 250_000);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].from_block, 0);
        assert_eq!(chunks[1].to_block, 499_999);
    }

    #[test]
    fn test_divide_into_chunks_zero_range() {
        let chunks = divide_into_chunks(100, 100, 4, 250_000);
        assert_eq!(chunks.len(), 0);
    }

    #[test]
    fn test_divide_into_chunks_single_block() {
        let chunks = divide_into_chunks(100, 101, 4, 250_000);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].from_block, 100);
        assert_eq!(chunks[0].to_block, 100);
    }

    #[test]
    fn test_divide_into_chunks_even_distribution() {
        let chunks = divide_into_chunks(0, 1200, 3, 100);
        assert_eq!(chunks.len(), 3);
        for chunk in &chunks {
            let size = chunk.to_block - chunk.from_block + 1;
            assert!(size >= 399 && size <= 401, "Chunk size {} not balanced", size);
        }
    }

    #[test]
    fn test_divide_into_chunks_large_range_single_worker() {
        let chunks = divide_into_chunks(0, 10_000_000, 1, 250_000);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].from_block, 0);
        assert_eq!(chunks[0].to_block, 9_999_999);
        assert_eq!(chunks[0].worker_id, 1);
    }

    #[test]
    fn test_divide_into_chunks_two_blocks() {
        let chunks = divide_into_chunks(100, 102, 4, 250_000);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].from_block, 100);
        assert_eq!(chunks[0].to_block, 101);
    }

    #[test]
    fn test_divide_into_chunks_exact_division() {
        let chunks = divide_into_chunks(0, 1000, 4, 250);
        assert_eq!(chunks.len(), 4);
        for chunk in &chunks {
            let size = chunk.to_block - chunk.from_block + 1;
            assert_eq!(size, 250);
        }
    }

    #[test]
    fn test_divide_into_chunks_no_gaps_no_overlaps() {
        let start = 5000;
        let target = 5_000_000;
        let chunks = divide_into_chunks(start, target, 8, 250_000);
        assert!(!chunks.is_empty());
        assert_eq!(chunks[0].from_block, start);
        for i in 1..chunks.len() {
            assert_eq!(chunks[i].from_block, chunks[i - 1].to_block + 1);
        }
        assert!(chunks.last().unwrap().to_block < target);
    }

    #[test]
    fn test_divide_into_chunks_worker_ids_sequential() {
        let chunks = divide_into_chunks(0, 2_000_000, 8, 250_000);
        for (i, chunk) in chunks.iter().enumerate() {
            assert_eq!(chunk.worker_id, (i + 1) as i32);
        }
    }

    #[test]
    fn test_divide_into_chunks_remainder_distribution() {
        let chunks = divide_into_chunks(0, 7, 3, 1);
        assert_eq!(chunks.len(), 3);
        let size0 = chunks[0].to_block - chunks[0].from_block + 1;
        let size1 = chunks[1].to_block - chunks[1].from_block + 1;
        let size2 = chunks[2].to_block - chunks[2].from_block + 1;
        assert_eq!(size0 + size1 + size2, 7);
        assert!(size0 >= size2);
    }

    #[test]
    fn test_build_log_filter_static_addresses() {
        let filter = LogFilter {
            addresses: vec!["0xabcdef".to_string(), "0x123456".to_string()],
            topics: vec![],
        };
        assert_eq!(filter.addresses.len(), 2);
        assert!(filter.topics.is_empty());
    }

    #[test]
    fn test_build_log_filter_empty() {
        let filter = LogFilter {
            addresses: vec![],
            topics: vec![],
        };
        assert!(filter.addresses.is_empty());
    }

    // ========================================================================
    // Deep chunk division edge case tests
    // ========================================================================

    #[test]
    fn test_divide_into_chunks_start_equals_target_minus_1() {
        // Exactly 1 block range
        let chunks = divide_into_chunks(999, 1000, 4, 250_000);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].from_block, 999);
        assert_eq!(chunks[0].to_block, 999);
    }

    #[test]
    fn test_divide_into_chunks_more_workers_than_blocks() {
        // 3 blocks but 100 workers requested
        let chunks = divide_into_chunks(0, 3, 100, 1);
        assert_eq!(chunks.len(), 3);
        for chunk in &chunks {
            let size = chunk.to_block - chunk.from_block + 1;
            assert_eq!(size, 1);
        }
    }

    #[test]
    fn test_divide_into_chunks_very_large_range() {
        let start = 0;
        let target = 100_000_000;
        let chunks = divide_into_chunks(start, target, 16, 1_000_000);
        assert_eq!(chunks.len(), 16);
        // No gaps, no overlaps
        assert_eq!(chunks[0].from_block, start);
        for i in 1..chunks.len() {
            assert_eq!(chunks[i].from_block, chunks[i - 1].to_block + 1);
        }
        // Total coverage
        let total: u64 = chunks
            .iter()
            .map(|c| c.to_block - c.from_block + 1)
            .sum();
        assert_eq!(total, target - start);
    }

    #[test]
    fn test_divide_into_chunks_blocks_per_worker_larger_than_range() {
        // blocks_per_worker is huge, should produce 1 chunk
        let chunks = divide_into_chunks(0, 100, 4, 1_000_000);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].from_block, 0);
        assert_eq!(chunks[0].to_block, 99);
    }

    #[test]
    fn test_divide_into_chunks_remainder_goes_to_early_workers() {
        // 10 blocks, 3 workers -> 3+3+4? or 4+3+3?
        let chunks = divide_into_chunks(0, 10, 3, 1);
        assert_eq!(chunks.len(), 3);
        let sizes: Vec<u64> = chunks
            .iter()
            .map(|c| c.to_block - c.from_block + 1)
            .collect();
        // Remainder blocks (10 % 3 = 1) go to first worker(s)
        assert!(sizes[0] >= sizes[2], "First chunk should be >= last chunk");
        assert_eq!(sizes.iter().sum::<u64>(), 10);
    }

    #[test]
    fn test_divide_into_chunks_worker_ids_start_at_1() {
        let chunks = divide_into_chunks(0, 100, 4, 25);
        assert!(!chunks.is_empty());
        assert_eq!(chunks[0].worker_id, 1);
        // Worker ID 0 is reserved for live sync
        assert!(chunks.iter().all(|c| c.worker_id > 0));
    }

    #[test]
    fn test_divide_into_chunks_prime_number_blocks() {
        // 97 blocks (prime), 4 workers
        let chunks = divide_into_chunks(0, 97, 4, 25);
        let total: u64 = chunks
            .iter()
            .map(|c| c.to_block - c.from_block + 1)
            .sum();
        assert_eq!(total, 97);
        // No gaps
        for i in 1..chunks.len() {
            assert_eq!(chunks[i].from_block, chunks[i - 1].to_block + 1);
        }
    }

    #[test]
    fn test_divide_into_chunks_high_start_block() {
        // Start from a high block (e.g., Ethereum mainnet)
        let chunks = divide_into_chunks(18_000_000, 19_000_000, 4, 250_000);
        assert_eq!(chunks.len(), 4);
        assert_eq!(chunks[0].from_block, 18_000_000);
        assert!(chunks.last().unwrap().to_block < 19_000_000);
    }

    #[test]
    fn test_divide_into_chunks_all_blocks_covered_no_leftover() {
        // Verify total coverage for various inputs
        for (start, target, workers, bpw) in [
            (0, 1000, 3, 100),
            (100, 200, 7, 10),
            (0, 1, 10, 1),
            (50, 5050, 5, 1000),
        ] {
            let chunks = divide_into_chunks(start, target, workers, bpw);
            if chunks.is_empty() {
                assert_eq!(start, target);
                continue;
            }
            let total: u64 = chunks
                .iter()
                .map(|c| c.to_block - c.from_block + 1)
                .sum();
            assert_eq!(
                total,
                target - start,
                "Coverage mismatch for start={}, target={}, workers={}, bpw={}",
                start,
                target,
                workers,
                bpw
            );
        }
    }

    #[test]
    fn test_divide_into_chunks_max_workers_1() {
        let chunks = divide_into_chunks(0, 10_000, 1, 100);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].from_block, 0);
        assert_eq!(chunks[0].to_block, 9999);
    }

    #[test]
    fn test_divide_into_chunks_blocks_per_worker_1() {
        // Each worker gets ~1 block, but capped by max_workers
        let chunks = divide_into_chunks(0, 10, 5, 1);
        assert_eq!(chunks.len(), 5);
        let total: u64 = chunks
            .iter()
            .map(|c| c.to_block - c.from_block + 1)
            .sum();
        assert_eq!(total, 10);
    }
}
