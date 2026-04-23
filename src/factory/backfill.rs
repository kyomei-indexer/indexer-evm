use crate::abi::decoder::AbiDecoder;
use crate::db::decoded::DecodedEventRepository;
use crate::db::events::EventRepository;
use crate::db::factory::FactoryRepository;
use crate::sources::BlockSource;
use crate::types::{BlockRange, FactoryChild, LogFilter, RawEventRecord};
use anyhow::{Context, Result};
use futures::stream::StreamExt;
use tracing::{debug, warn};

/// Backfills events for newly discovered factory child contracts.
/// When a factory event creates a new child contract, we need to go back
/// and fetch all events for that child from its creation block to the current block.
pub struct FactoryBackfiller {
    chain_id: i32,
    blocks_per_request: u64,
}

impl FactoryBackfiller {
    pub fn new(chain_id: i32, blocks_per_request: u64) -> Self {
        Self {
            chain_id,
            blocks_per_request,
        }
    }

    /// Backfill events for a batch of newly discovered factory children.
    /// Fetches events from each child's creation block up to `current_block`.
    pub async fn backfill(
        &self,
        children: &[FactoryChild],
        current_block: u64,
        source: &dyn BlockSource,
        event_repo: &EventRepository,
        decoded_repo: &DecodedEventRepository,
        factory_repo: &FactoryRepository,
        topic_filters: &[String],
        decoder: &AbiDecoder,
    ) -> Result<u64> {
        if children.is_empty() {
            return Ok(0);
        }

        // Store new factory children in the database
        factory_repo
            .insert_batch(children)
            .await
            .context("Failed to insert factory children")?;

        // Filter children synchronously, then backfill concurrently
        let eligible: Vec<&FactoryChild> = children
            .iter()
            .filter(|child| {
                let from_block = child.created_at_block as u64;
                if from_block >= current_block {
                    debug!(
                        child_address = %child.child_address,
                        created_at = from_block,
                        current = current_block,
                        "Skipping backfill: child created at or after current block"
                    );
                    false
                } else {
                    true
                }
            })
            .collect();

        let mut total_events = 0u64;

        // Process children concurrently using FuturesUnordered (stays on same task, no Send needed).
        // Bounded to max 4 concurrent backfills to avoid overwhelming the RPC.
        let max_concurrent = 4;
        for chunk in eligible.chunks(max_concurrent) {
            let mut futs = futures::stream::FuturesUnordered::new();
            for child in chunk {
                futs.push(self.backfill_child(
                    child,
                    child.created_at_block as u64,
                    current_block,
                    source,
                    event_repo,
                    decoded_repo,
                    topic_filters,
                    decoder,
                ));
            }
            while let Some(result) = futs.next().await {
                total_events += result?;
            }
        }

        debug!(
            children_count = children.len(),
            total_events,
            "Factory backfill completed"
        );

        Ok(total_events)
    }

    /// Backfill events for a single child contract
    async fn backfill_child(
        &self,
        child: &FactoryChild,
        from_block: u64,
        to_block: u64,
        source: &dyn BlockSource,
        event_repo: &EventRepository,
        decoded_repo: &DecodedEventRepository,
        topic_filters: &[String],
        decoder: &AbiDecoder,
    ) -> Result<u64> {
        debug!(
            child_address = %child.child_address,
            from_block,
            to_block,
            "Starting backfill for factory child"
        );

        let mut total_events = 0u64;
        let mut from = from_block;

        while from <= to_block {
            let to = std::cmp::min(from + self.blocks_per_request - 1, to_block);

            // Create a filter specifically for this child's address
            let filter = LogFilter {
                addresses: vec![child.child_address.clone()],
                topics: topic_filters
                    .iter()
                    .map(|t| Some(t.clone()))
                    .collect(),
            };

            let range = BlockRange { from, to };

            match source.get_blocks(range, &filter).await {
                Ok(blocks) => {
                    let mut batch: Vec<RawEventRecord> = Vec::new();

                    for block in &blocks {
                        for log in &block.logs {
                            batch.push(RawEventRecord {
                                chain_id: self.chain_id,
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
                    }

                    if !batch.is_empty() {
                        let inserted = event_repo.insert_batch(&batch).await?;
                        decoded_repo.insert_batch(&batch, decoder).await?;
                        total_events += inserted;
                    }
                }
                Err(e) => {
                    warn!(
                        child = %child.child_address,
                        from, to,
                        error = %e,
                        "Failed to fetch blocks for backfill, skipping range"
                    );
                }
            }

            from = to + 1;
        }

        debug!(
            child_address = %child.child_address,
            total_events,
            "Backfill completed for factory child"
        );

        Ok(total_events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backfiller_creation() {
        let backfiller = FactoryBackfiller::new(1, 10000);
        assert_eq!(backfiller.chain_id, 1);
        assert_eq!(backfiller.blocks_per_request, 10000);
    }
}
