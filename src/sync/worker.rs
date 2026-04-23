use crate::db::events::EventRepository;
use crate::db::factory::FactoryRepository;
use crate::db::workers::WorkerRepository;
use crate::factory::watcher::FactoryWatcher;
use crate::sources::BlockSource;
use crate::sync::progress::ProgressTracker;
use crate::types::{
    BlockRange, LogFilter, RawEventRecord, SyncWorker, SyncWorkerStatus, WorkerChunk,
};
use anyhow::{Context, Result};
use tracing::{debug, info, warn};

/// Configuration for a sync worker
pub struct WorkerConfig {
    pub chain_id: i32,
    pub blocks_per_request: u64,
    pub checkpoint_interval: u64,
    /// Maximum consecutive errors before the worker gives up
    pub max_consecutive_errors: u32,
}

impl WorkerConfig {
    /// Create a default config with standard error thresholds
    pub fn new(chain_id: i32, blocks_per_request: u64, checkpoint_interval: u64) -> Self {
        Self {
            chain_id,
            blocks_per_request,
            checkpoint_interval,
            max_consecutive_errors: 10,
        }
    }
}

/// Run a single historic sync worker for a given block range chunk.
/// Fetches blocks, inserts events, and checkpoints progress.
///
/// Includes exponential backoff retry on fetch failures. The worker will
/// abort after `max_consecutive_errors` consecutive failures (default: 10).
pub async fn run_historic_worker(
    config: &WorkerConfig,
    chunk: &WorkerChunk,
    source: &dyn BlockSource,
    filter: &LogFilter,
    event_repo: &EventRepository,
    worker_repo: &WorkerRepository,
    factory_watcher: Option<&FactoryWatcher>,
    factory_repo: Option<&FactoryRepository>,
    progress: &ProgressTracker,
) -> Result<WorkerStats> {
    let mut stats = WorkerStats::default();
    let mut current_block = chunk.from_block;
    let mut blocks_since_checkpoint = 0u64;
    let mut consecutive_errors = 0u32;
    let total_blocks = chunk.to_block.saturating_sub(chunk.from_block) + 1;

    debug!(
        worker_id = chunk.worker_id,
        chain_id = config.chain_id,
        from = chunk.from_block,
        to = chunk.to_block,
        total_blocks,
        source_type = source.source_type(),
        "Starting historic worker"
    );

    // Create the worker record
    worker_repo
        .set_worker(&SyncWorker {
            chain_id: config.chain_id,
            worker_id: chunk.worker_id,
            range_start: chunk.from_block as i64,
            range_end: Some(chunk.to_block as i64),
            current_block: chunk.from_block as i64,
            status: SyncWorkerStatus::Historical,
        })
        .await
        .context("Failed to create worker record")?;

    while current_block <= chunk.to_block {
        let to = std::cmp::min(
            current_block + config.blocks_per_request - 1,
            chunk.to_block,
        );

        let range = BlockRange {
            from: current_block,
            to,
        };

        match source.get_blocks(range, filter).await {
            Ok(blocks) => {
                if consecutive_errors > 0 {
                    info!(
                        worker_id = chunk.worker_id,
                        chain_id = config.chain_id,
                        from_block = current_block,
                        to_block = to,
                        previous_consecutive_errors = consecutive_errors,
                        "Worker recovered after errors"
                    );
                }
                consecutive_errors = 0;

                let mut batch: Vec<RawEventRecord> = Vec::new();

                for block in &blocks {
                    for log in &block.logs {
                        batch.push(RawEventRecord {
                            chain_id: config.chain_id,
                            block_number: log.block_number as i64,
                            tx_index: log.transaction_index as i32,
                            log_index: log.log_index as i32,
                            block_hash: log.block_hash.clone(),
                            block_timestamp: log.block_timestamp as i64,
                            tx_hash: log.transaction_hash.clone(),
                            address: log.address.clone(),
                            topic0: log.topic0.clone(),
                            topic1: log.topic1.clone(),
                            topic2: log.topic2.clone(),
                            topic3: log.topic3.clone(),
                            data: log.data.clone(),
                        });
                    }

                    // Check for factory events in this block's logs
                    if let Some(watcher) = factory_watcher {
                        let children = watcher.process_logs(&block.logs);
                        if !children.is_empty() {
                            debug!(
                                worker_id = chunk.worker_id,
                                chain_id = config.chain_id,
                                block_number = block.block.number,
                                factory_children = children.len(),
                                "Discovered factory children"
                            );

                            // Persist discovered children to the database
                            if let Some(repo) = factory_repo {
                                repo.insert_batch(&children)
                                    .await
                                    .context("Failed to insert factory children")?;
                            }
                        }
                        stats.factory_children_found += children.len() as u64;
                    }
                }

                if !batch.is_empty() {
                    let inserted = event_repo
                        .insert_batch(&batch)
                        .await
                        .context("Failed to insert event batch")?;
                    stats.events_inserted += inserted;
                    progress.add_events(inserted);
                }

                stats.blocks_processed += to - current_block + 1;
            }
            Err(e) => {
                consecutive_errors += 1;
                let backoff_secs = std::cmp::min(2u64.saturating_pow(consecutive_errors), 60);

                warn!(
                    worker_id = chunk.worker_id,
                    chain_id = config.chain_id,
                    from_block = current_block,
                    to_block = to,
                    consecutive_errors,
                    max_consecutive_errors = config.max_consecutive_errors,
                    backoff_secs,
                    error = %e,
                    "Failed to fetch blocks, retrying with exponential backoff"
                );

                if consecutive_errors >= config.max_consecutive_errors {
                    warn!(
                        worker_id = chunk.worker_id,
                        chain_id = config.chain_id,
                        consecutive_errors,
                        blocks_processed = stats.blocks_processed,
                        events_inserted = stats.events_inserted,
                        "Worker aborting due to too many consecutive errors"
                    );
                    return Err(e).context(format!(
                        "Worker {} aborted after {} consecutive errors at block {}",
                        chunk.worker_id, consecutive_errors, current_block
                    ));
                }

                tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
                continue;
            }
        }

        current_block = to + 1;
        blocks_since_checkpoint += to - range.from + 1;

        // Checkpoint progress
        if blocks_since_checkpoint >= config.checkpoint_interval {
            checkpoint(
                worker_repo,
                config.chain_id,
                chunk.worker_id,
                current_block.saturating_sub(1) as i64,
            )
            .await?;

            progress.set_current_block(current_block);
            blocks_since_checkpoint = 0;

            let progress_pct = if total_blocks > 0 {
                ((current_block - chunk.from_block) as f64 / total_blocks as f64 * 100.0)
                    .min(100.0)
            } else {
                100.0
            };

            debug!(
                worker_id = chunk.worker_id,
                chain_id = config.chain_id,
                block = current_block - 1,
                events = stats.events_inserted,
                progress = format!("{:.1}%", progress_pct),
                "Checkpoint"
            );
        }
    }

    // Final checkpoint
    checkpoint(
        worker_repo,
        config.chain_id,
        chunk.worker_id,
        chunk.to_block as i64,
    )
    .await?;

    debug!(
        worker_id = chunk.worker_id,
        chain_id = config.chain_id,
        blocks = stats.blocks_processed,
        events = stats.events_inserted,
        factory_children = stats.factory_children_found,
        "Historic worker completed"
    );

    Ok(stats)
}

/// Checkpoint the worker's current position
async fn checkpoint(
    worker_repo: &WorkerRepository,
    chain_id: i32,
    worker_id: i32,
    current_block: i64,
) -> Result<()> {
    worker_repo
        .set_worker(&SyncWorker {
            chain_id,
            worker_id,
            range_start: 0, // Not updated during checkpoint
            range_end: None,
            current_block,
            status: SyncWorkerStatus::Historical,
        })
        .await
        .context("Failed to checkpoint worker")
}

/// Stats from a worker run
#[derive(Debug, Default)]
pub struct WorkerStats {
    pub blocks_processed: u64,
    pub events_inserted: u64,
    pub factory_children_found: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_stats_default() {
        let stats = WorkerStats::default();
        assert_eq!(stats.blocks_processed, 0);
        assert_eq!(stats.events_inserted, 0);
        assert_eq!(stats.factory_children_found, 0);
    }

    #[test]
    fn test_worker_config() {
        let config = WorkerConfig {
            chain_id: 1,
            blocks_per_request: 10000,
            checkpoint_interval: 500,
            max_consecutive_errors: 10,
        };
        assert_eq!(config.chain_id, 1);
        assert_eq!(config.blocks_per_request, 10000);
        assert_eq!(config.checkpoint_interval, 500);
        assert_eq!(config.max_consecutive_errors, 10);
    }

    #[test]
    fn test_worker_stats_accumulation() {
        let mut stats = WorkerStats::default();
        stats.blocks_processed += 1000;
        stats.events_inserted += 500;
        stats.factory_children_found += 3;
        assert_eq!(stats.blocks_processed, 1000);
        assert_eq!(stats.events_inserted, 500);
        assert_eq!(stats.factory_children_found, 3);
    }

    #[test]
    fn test_worker_config_polygon() {
        let config = WorkerConfig {
            chain_id: 137,
            blocks_per_request: 2000,
            checkpoint_interval: 1000,
            max_consecutive_errors: 5,
        };
        assert_eq!(config.chain_id, 137);
        assert_eq!(config.blocks_per_request, 2000);
        assert_eq!(config.max_consecutive_errors, 5);
    }

    #[test]
    fn test_worker_config_new() {
        let config = WorkerConfig::new(1, 10000, 500);
        assert_eq!(config.chain_id, 1);
        assert_eq!(config.blocks_per_request, 10000);
        assert_eq!(config.checkpoint_interval, 500);
        assert_eq!(config.max_consecutive_errors, 10); // default
    }

    #[test]
    fn test_worker_config_custom_max_errors() {
        let config = WorkerConfig {
            chain_id: 42161,
            blocks_per_request: 5000,
            checkpoint_interval: 200,
            max_consecutive_errors: 3,
        };
        assert_eq!(config.chain_id, 42161);
        assert_eq!(config.max_consecutive_errors, 3);
    }
}
