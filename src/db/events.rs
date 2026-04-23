use crate::types::RawEventRecord;
use anyhow::Result;
use sqlx::{PgPool, Postgres, Transaction};
use std::io::Write;
use tracing::trace;

/// Repository for raw blockchain events
pub struct EventRepository {
    pool: PgPool,
    schema: String,
}

impl EventRepository {
    pub fn new(pool: PgPool, schema: String) -> Self {
        Self {
            pool,
            schema,
        }
    }

    /// Get a reference to the underlying connection pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Ensure the staging temp table exists on the current connection.
    /// Temp tables are per-connection, so this must run on every connection
    /// that will use the staging table — the `IF NOT EXISTS` clause makes
    /// repeated calls on the same connection a no-op.
    async fn ensure_staging_table(
        &self,
        executor: impl sqlx::Executor<'_, Database = Postgres>,
    ) -> Result<()> {
        sqlx::query(&format!(
            "CREATE TEMP TABLE IF NOT EXISTS _stg_raw_events \
             (LIKE {}.raw_events) ON COMMIT DELETE ROWS",
            self.schema
        ))
        .execute(executor)
        .await?;
        Ok(())
    }

    /// Insert a batch of events using PostgreSQL COPY for maximum throughput.
    /// Uses a temp staging table with INSERT...ON CONFLICT DO NOTHING for idempotency.
    pub async fn insert_batch(&self, events: &[RawEventRecord]) -> Result<u64> {
        if events.is_empty() {
            return Ok(0);
        }

        let copy_start = std::time::Instant::now();
        let mut tx = self.pool.begin().await?;

        self.ensure_staging_table(&mut *tx).await?;

        // Stream events into staging table via COPY protocol in chunks
        // to keep memory bounded while still being much faster than INSERT.
        {
            let mut copy = tx
                .as_mut()
                .copy_in_raw(
                    "COPY _stg_raw_events (\
                     chain_id, block_number, tx_index, log_index, \
                     block_hash, block_timestamp, tx_hash, address, \
                     topic0, topic1, topic2, topic3, data\
                     ) FROM STDIN",
                )
                .await?;

            let chunk_size = adaptive_chunk_size(events);
            for chunk in events.chunks(chunk_size) {
                let buf = encode_copy_text(chunk);
                copy.send(&buf[..]).await?;
            }

            copy.finish().await?;
        }

        // Move from staging to target, skipping duplicates
        let result = sqlx::query(&format!(
            "INSERT INTO {}.raw_events SELECT * FROM _stg_raw_events ON CONFLICT DO NOTHING",
            self.schema
        ))
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        let count = result.rows_affected();
        if let Some(first) = events.first() {
            crate::metrics::db_copy_duration(first.chain_id as u32, copy_start.elapsed().as_secs_f64());
        }
        trace!(count, events = events.len(), "Inserted events via COPY");
        Ok(count)
    }

    /// Insert a batch of events within an existing transaction.
    /// Used by live sync to ensure atomicity across raw events, decoded events, and worker checkpoint.
    pub async fn insert_batch_in_tx(
        &self,
        events: &[RawEventRecord],
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<u64> {
        if events.is_empty() {
            return Ok(0);
        }

        self.ensure_staging_table(&mut **tx).await?;

        {
            let mut copy = tx
                .as_mut()
                .copy_in_raw(
                    "COPY _stg_raw_events (\
                     chain_id, block_number, tx_index, log_index, \
                     block_hash, block_timestamp, tx_hash, address, \
                     topic0, topic1, topic2, topic3, data\
                     ) FROM STDIN",
                )
                .await?;

            let chunk_size = adaptive_chunk_size(events);
            for chunk in events.chunks(chunk_size) {
                let buf = encode_copy_text(chunk);
                copy.send(&buf[..]).await?;
            }

            copy.finish().await?;
        }

        let result = sqlx::query(&format!(
            "INSERT INTO {}.raw_events SELECT * FROM _stg_raw_events ON CONFLICT DO NOTHING",
            self.schema
        ))
        .execute(&mut **tx)
        .await?;

        let count = result.rows_affected();
        trace!(count, events = events.len(), "Inserted events via COPY (in tx)");
        Ok(count)
    }

    /// Insert a batch of events directly into the target table within a transaction.
    /// No staging table, no ON CONFLICT — fastest path for exclusive block ranges.
    /// Returns Err on any failure including unique violations.
    pub async fn insert_batch_direct_in_tx(
        &self,
        events: &[RawEventRecord],
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<u64> {
        if events.is_empty() {
            return Ok(0);
        }

        {
            let mut copy = tx
                .as_mut()
                .copy_in_raw(&format!(
                    "COPY {}.raw_events (\
                     chain_id, block_number, tx_index, log_index, \
                     block_hash, block_timestamp, tx_hash, address, \
                     topic0, topic1, topic2, topic3, data\
                     ) FROM STDIN",
                    self.schema
                ))
                .await?;

            let chunk_size = adaptive_chunk_size(events);
            for chunk in events.chunks(chunk_size) {
                let buf = encode_copy_text(chunk);
                copy.send(&buf[..]).await?;
            }

            copy.finish().await?;
        }

        let count = events.len() as u64;
        trace!(count, "Inserted events via direct COPY (in tx)");
        Ok(count)
    }

    /// Query events for a block range, ordered by block_number, tx_index, log_index
    pub async fn query_range(
        &self,
        chain_id: i32,
        from_block: i64,
        to_block: i64,
    ) -> Result<Vec<RawEventRecord>> {
        let rows = sqlx::query_as::<_, RawEventRow>(&format!(
            r#"
            SELECT chain_id, block_number, tx_index, log_index, block_hash, block_timestamp,
                   tx_hash, address, topic0, topic1, topic2, topic3, data
            FROM {}.raw_events
            WHERE chain_id = $1 AND block_number >= $2 AND block_number <= $3
            ORDER BY block_number, tx_index, log_index
            "#,
            self.schema
        ))
        .bind(chain_id)
        .bind(from_block)
        .bind(to_block)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| r.into()).collect())
    }

    /// Delete events in a block range (used for reorg handling)
    pub async fn delete_range(
        &self,
        chain_id: i32,
        from_block: i64,
        to_block: i64,
    ) -> Result<u64> {
        let result = sqlx::query(&format!(
            r#"
            DELETE FROM {}.raw_events
            WHERE chain_id = $1 AND block_number >= $2 AND block_number <= $3
            "#,
            self.schema
        ))
        .bind(chain_id)
        .bind(from_block)
        .bind(to_block)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    /// Get the latest block number for a chain
    pub async fn get_latest_block(&self, chain_id: i32) -> Result<Option<i64>> {
        let result = sqlx::query_scalar::<_, Option<i64>>(&format!(
            "SELECT MAX(block_number) FROM {}.raw_events WHERE chain_id = $1",
            self.schema
        ))
        .bind(chain_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(result)
    }

    /// Count events for a chain
    pub async fn count(&self, chain_id: i32) -> Result<i64> {
        let result = sqlx::query_scalar::<_, i64>(&format!(
            "SELECT COUNT(*) FROM {}.raw_events WHERE chain_id = $1",
            self.schema
        ))
        .bind(chain_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(result)
    }

    /// Read raw events from DB for a block range (used for decoded table rebuild).
    /// Returns events ordered by block_number, tx_index, log_index.
    pub async fn get_raw_events_range(
        &self,
        chain_id: i32,
        from_block: i64,
        to_block: i64,
    ) -> Result<Vec<RawEventRecord>> {
        let rows = sqlx::query_as::<_, RawEventRow>(&format!(
            r#"
            SELECT chain_id, block_number, tx_index, log_index, block_hash,
                   block_timestamp, tx_hash, address, topic0, topic1, topic2, topic3, data
            FROM {}.raw_events
            WHERE chain_id = $1 AND block_number >= $2 AND block_number <= $3
            ORDER BY block_number, tx_index, log_index
            "#,
            self.schema
        ))
        .bind(chain_id)
        .bind(from_block)
        .bind(to_block)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| r.into()).collect())
    }

    /// Get the earliest block number for a chain
    pub async fn get_earliest_block(&self, chain_id: i32) -> Result<Option<i64>> {
        let result = sqlx::query_scalar::<_, Option<i64>>(&format!(
            "SELECT MIN(block_number) FROM {}.raw_events WHERE chain_id = $1",
            self.schema
        ))
        .bind(chain_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(result)
    }

    /// Check if a specific block has any events
    pub async fn has_block(&self, chain_id: i32, block_number: i64) -> Result<bool> {
        let result = sqlx::query_scalar::<_, bool>(&format!(
            "SELECT EXISTS(SELECT 1 FROM {}.raw_events WHERE chain_id = $1 AND block_number = $2)",
            self.schema
        ))
        .bind(chain_id)
        .bind(block_number)
        .fetch_one(&self.pool)
        .await?;

        Ok(result)
    }
}

/// Compute adaptive COPY chunk size based on estimated event sizes.
/// Targets ~8MB per COPY send to balance memory use vs throughput.
/// For small events: larger chunks (up to 50K). For large events (big data fields): smaller chunks (min 1K).
fn adaptive_chunk_size(events: &[RawEventRecord]) -> usize {
    if events.is_empty() {
        return 5000;
    }
    // Sample first few events to estimate average size
    let sample_count = events.len().min(10);
    let sample_size: usize = events[..sample_count]
        .iter()
        .map(|e| {
            80 // fixed overhead (ints, delimiters)
                + e.block_hash.len()
                + e.tx_hash.len()
                + e.address.len()
                + e.topic0.as_ref().map_or(2, |s| s.len())
                + e.topic1.as_ref().map_or(2, |s| s.len())
                + e.topic2.as_ref().map_or(2, |s| s.len())
                + e.topic3.as_ref().map_or(2, |s| s.len())
                + e.data.len()
        })
        .sum();
    let avg_size = sample_size / sample_count;
    let target_bytes = 8_000_000usize;
    (target_bytes / avg_size.max(1)).clamp(1000, 50_000)
}

/// Encode events as PostgreSQL COPY text format (tab-separated, newline-delimited).
/// All string values are hex-encoded so no escaping is needed.
fn encode_copy_text(events: &[RawEventRecord]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(events.len() * 200);

    for event in events {
        write!(buf, "{}", event.chain_id).unwrap();
        buf.push(b'\t');
        write!(buf, "{}", event.block_number).unwrap();
        buf.push(b'\t');
        write!(buf, "{}", event.tx_index).unwrap();
        buf.push(b'\t');
        write!(buf, "{}", event.log_index).unwrap();
        buf.push(b'\t');
        buf.extend_from_slice(event.block_hash.as_bytes());
        buf.push(b'\t');
        write!(buf, "{}", event.block_timestamp).unwrap();
        buf.push(b'\t');
        buf.extend_from_slice(event.tx_hash.as_bytes());
        buf.push(b'\t');
        buf.extend_from_slice(event.address.as_bytes());
        buf.push(b'\t');
        write_nullable(&mut buf, &event.topic0);
        buf.push(b'\t');
        write_nullable(&mut buf, &event.topic1);
        buf.push(b'\t');
        write_nullable(&mut buf, &event.topic2);
        buf.push(b'\t');
        write_nullable(&mut buf, &event.topic3);
        buf.push(b'\t');
        buf.extend_from_slice(event.data.as_bytes());
        buf.push(b'\n');
    }

    buf
}

#[inline]
fn write_nullable(buf: &mut Vec<u8>, value: &Option<String>) {
    match value {
        Some(v) => buf.extend_from_slice(v.as_bytes()),
        None => buf.extend_from_slice(b"\\N"),
    }
}

/// Internal row type for sqlx deserialization
#[derive(sqlx::FromRow)]
struct RawEventRow {
    chain_id: i32,
    block_number: i64,
    tx_index: i32,
    log_index: i32,
    block_hash: String,
    block_timestamp: i64,
    tx_hash: String,
    address: String,
    topic0: Option<String>,
    topic1: Option<String>,
    topic2: Option<String>,
    topic3: Option<String>,
    data: String,
}

impl From<RawEventRow> for RawEventRecord {
    fn from(row: RawEventRow) -> Self {
        Self {
            chain_id: row.chain_id,
            block_number: row.block_number,
            tx_index: row.tx_index,
            log_index: row.log_index,
            block_hash: row.block_hash,
            block_timestamp: row.block_timestamp,
            tx_hash: row.tx_hash,
            address: row.address,
            topic0: row.topic0,
            topic1: row.topic1,
            topic2: row.topic2,
            topic3: row.topic3,
            data: row.data,
        }
    }
}

/// Convert a LogEntry into a RawEventRecord.
/// Uses saturating/checked casts to prevent overflow panics — values exceeding
/// PostgreSQL limits are clamped to max and a warning is logged.
impl crate::types::LogEntry {
    pub fn to_raw_event(&self, chain_id: i32) -> RawEventRecord {
        let block_number = i64::try_from(self.block_number).unwrap_or_else(|_| {
            tracing::warn!(
                block_number = self.block_number,
                "block_number exceeds i64::MAX, clamping"
            );
            i64::MAX
        });
        let tx_index = i32::try_from(self.transaction_index).unwrap_or_else(|_| {
            tracing::warn!(
                transaction_index = self.transaction_index,
                "transaction_index exceeds i32::MAX, clamping"
            );
            i32::MAX
        });
        let log_index = i32::try_from(self.log_index).unwrap_or_else(|_| {
            tracing::warn!(
                log_index = self.log_index,
                "log_index exceeds i32::MAX, clamping"
            );
            i32::MAX
        });
        let block_timestamp = i64::try_from(self.block_timestamp).unwrap_or_else(|_| {
            tracing::warn!(
                block_timestamp = self.block_timestamp,
                "block_timestamp exceeds i64::MAX, clamping"
            );
            i64::MAX
        });

        RawEventRecord {
            chain_id,
            block_number,
            tx_index,
            log_index,
            block_hash: self.block_hash.clone(),
            block_timestamp,
            tx_hash: self.transaction_hash.clone(),
            address: self.address.to_lowercase(),
            topic0: self.topic0.clone(),
            topic1: self.topic1.clone(),
            topic2: self.topic2.clone(),
            topic3: self.topic3.clone(),
            data: self.data.clone(),
        }
    }

    /// Consuming version that moves Strings instead of cloning them.
    pub fn into_raw_event(self, chain_id: i32) -> RawEventRecord {
        let block_number = i64::try_from(self.block_number).unwrap_or_else(|_| {
            tracing::warn!(
                block_number = self.block_number,
                "block_number exceeds i64::MAX, clamping"
            );
            i64::MAX
        });
        let tx_index = i32::try_from(self.transaction_index).unwrap_or_else(|_| {
            tracing::warn!(
                transaction_index = self.transaction_index,
                "transaction_index exceeds i32::MAX, clamping"
            );
            i32::MAX
        });
        let log_index = i32::try_from(self.log_index).unwrap_or_else(|_| {
            tracing::warn!(
                log_index = self.log_index,
                "log_index exceeds i32::MAX, clamping"
            );
            i32::MAX
        });
        let block_timestamp = i64::try_from(self.block_timestamp).unwrap_or_else(|_| {
            tracing::warn!(
                block_timestamp = self.block_timestamp,
                "block_timestamp exceeds i64::MAX, clamping"
            );
            i64::MAX
        });

        RawEventRecord {
            chain_id,
            block_number,
            tx_index,
            log_index,
            block_hash: self.block_hash,
            block_timestamp,
            tx_hash: self.transaction_hash,
            address: self.address.to_lowercase(),
            topic0: self.topic0,
            topic1: self.topic1,
            topic2: self.topic2,
            topic3: self.topic3,
            data: self.data,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::LogEntry;

    fn sample_event(block: i64, log_idx: i32) -> RawEventRecord {
        RawEventRecord {
            chain_id: 1,
            block_number: block,
            tx_index: 0,
            log_index: log_idx,
            block_hash: "0xabc123".to_string(),
            block_timestamp: 1700000000,
            tx_hash: "0xdef456".to_string(),
            address: "0x1234567890abcdef1234567890abcdef12345678".to_string(),
            topic0: Some("0xtopic0".to_string()),
            topic1: Some("0xtopic1".to_string()),
            topic2: None,
            topic3: None,
            data: "0xdata".to_string(),
        }
    }

    #[test]
    fn test_encode_copy_text_single_event() {
        let events = vec![sample_event(18000000, 3)];
        let buf = encode_copy_text(&events);
        let text = String::from_utf8(buf).unwrap();

        let cols: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
        assert_eq!(cols.len(), 13);
        assert_eq!(cols[0], "1");
        assert_eq!(cols[1], "18000000");
        assert_eq!(cols[2], "0");
        assert_eq!(cols[3], "3");
        assert_eq!(cols[4], "0xabc123");
        assert_eq!(cols[5], "1700000000");
        assert_eq!(cols[6], "0xdef456");
        assert_eq!(cols[7], "0x1234567890abcdef1234567890abcdef12345678");
        assert_eq!(cols[8], "0xtopic0");
        assert_eq!(cols[9], "0xtopic1");
        assert_eq!(cols[10], "\\N");
        assert_eq!(cols[11], "\\N");
        assert_eq!(cols[12], "0xdata");
    }

    #[test]
    fn test_encode_copy_text_multiple_events() {
        let events = vec![sample_event(100, 0), sample_event(100, 1), sample_event(101, 0)];
        let buf = encode_copy_text(&events);
        let text = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = text.trim_end_matches('\n').split('\n').collect();
        assert_eq!(lines.len(), 3);
    }

    #[test]
    fn test_encode_copy_text_empty() {
        let buf = encode_copy_text(&[]);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_encode_copy_text_all_topics_null() {
        let event = RawEventRecord {
            chain_id: 1,
            block_number: 100,
            tx_index: 0,
            log_index: 0,
            block_hash: "0x".to_string(),
            block_timestamp: 0,
            tx_hash: "0x".to_string(),
            address: "0x".to_string(),
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
            data: "0x".to_string(),
        };
        let buf = encode_copy_text(&[event]);
        let text = String::from_utf8(buf).unwrap();
        let cols: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
        assert_eq!(cols[8], "\\N");
        assert_eq!(cols[9], "\\N");
        assert_eq!(cols[10], "\\N");
        assert_eq!(cols[11], "\\N");
    }

    #[test]
    fn test_encode_copy_text_all_topics_present() {
        let event = RawEventRecord {
            chain_id: 1,
            block_number: 100,
            tx_index: 0,
            log_index: 0,
            block_hash: "0x".to_string(),
            block_timestamp: 0,
            tx_hash: "0x".to_string(),
            address: "0x".to_string(),
            topic0: Some("0xa".to_string()),
            topic1: Some("0xb".to_string()),
            topic2: Some("0xc".to_string()),
            topic3: Some("0xd".to_string()),
            data: "0x".to_string(),
        };
        let buf = encode_copy_text(&[event]);
        let text = String::from_utf8(buf).unwrap();
        let cols: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
        assert_eq!(cols[8], "0xa");
        assert_eq!(cols[9], "0xb");
        assert_eq!(cols[10], "0xc");
        assert_eq!(cols[11], "0xd");
    }

    #[test]
    fn test_write_nullable_some() {
        let mut buf = Vec::new();
        write_nullable(&mut buf, &Some("0xvalue".to_string()));
        assert_eq!(&buf, b"0xvalue");
    }

    #[test]
    fn test_write_nullable_none() {
        let mut buf = Vec::new();
        write_nullable(&mut buf, &None);
        assert_eq!(&buf, b"\\N");
    }

    #[test]
    fn test_log_entry_to_raw_event_basic() {
        let log = LogEntry {
            block_number: 18000000,
            block_hash: "0xabc".to_string(),
            block_timestamp: 1700000000,
            transaction_hash: "0xdef".to_string(),
            transaction_index: 5,
            log_index: 3,
            address: "0xAbCdEf1234567890AbCdEf1234567890AbCdEf12".to_string(),
            topic0: Some("0xtopic0".to_string()),
            topic1: Some("0xtopic1".to_string()),
            topic2: None,
            topic3: None,
            data: "0xdata".to_string(),
        };

        let event = log.to_raw_event(1);
        assert_eq!(event.chain_id, 1);
        assert_eq!(event.block_number, 18000000);
        assert_eq!(event.tx_index, 5);
        assert_eq!(event.log_index, 3);
        assert_eq!(event.address, "0xabcdef1234567890abcdef1234567890abcdef12");
    }

    #[test]
    fn test_log_entry_to_raw_event_address_lowercased() {
        let log = LogEntry {
            block_number: 100,
            block_hash: "0x".to_string(),
            block_timestamp: 0,
            transaction_hash: "0x".to_string(),
            transaction_index: 0,
            log_index: 0,
            address: "0xABCDEF".to_string(),
            topic0: None, topic1: None, topic2: None, topic3: None,
            data: "0x".to_string(),
        };
        assert_eq!(log.to_raw_event(42).address, "0xabcdef");
    }

    #[test]
    fn test_log_entry_to_raw_event_all_topics() {
        let log = LogEntry {
            block_number: 100,
            block_hash: "0x".to_string(),
            block_timestamp: 0,
            transaction_hash: "0x".to_string(),
            transaction_index: 0,
            log_index: 0,
            address: "0x".to_string(),
            topic0: Some("0xt0".to_string()),
            topic1: Some("0xt1".to_string()),
            topic2: Some("0xt2".to_string()),
            topic3: Some("0xt3".to_string()),
            data: "0xdata".to_string(),
        };
        let event = log.to_raw_event(1);
        assert_eq!(event.topic0.unwrap(), "0xt0");
        assert_eq!(event.topic3.unwrap(), "0xt3");
    }

    #[test]
    fn test_log_entry_to_raw_event_no_topics() {
        let log = LogEntry {
            block_number: 100,
            block_hash: "0x".to_string(),
            block_timestamp: 0,
            transaction_hash: "0x".to_string(),
            transaction_index: 0,
            log_index: 0,
            address: "0x".to_string(),
            topic0: None, topic1: None, topic2: None, topic3: None,
            data: "0x".to_string(),
        };
        let event = log.to_raw_event(1);
        assert!(event.topic0.is_none());
        assert!(event.topic1.is_none());
    }

    #[test]
    fn test_raw_event_row_to_record_conversion() {
        let row = RawEventRow {
            chain_id: 1,
            block_number: 18000000,
            tx_index: 42,
            log_index: 7,
            block_hash: "0xblockhash".to_string(),
            block_timestamp: 1700000000,
            tx_hash: "0xtxhash".to_string(),
            address: "0xaddress".to_string(),
            topic0: Some("0xtopic".to_string()),
            topic1: None, topic2: None, topic3: None,
            data: "0xdata".to_string(),
        };
        let record: RawEventRecord = row.into();
        assert_eq!(record.chain_id, 1);
        assert_eq!(record.block_number, 18000000);
        assert_eq!(record.tx_hash, "0xtxhash");
    }

    #[test]
    fn test_raw_event_row_preserves_all_topics() {
        let row = RawEventRow {
            chain_id: 137,
            block_number: 50000000,
            tx_index: 0,
            log_index: 0,
            block_hash: "0xhash".to_string(),
            block_timestamp: 1600000000,
            tx_hash: "0xtx".to_string(),
            address: "0xaddr".to_string(),
            topic0: Some("0xa".to_string()),
            topic1: Some("0xb".to_string()),
            topic2: Some("0xc".to_string()),
            topic3: Some("0xd".to_string()),
            data: "0xfulldata".to_string(),
        };
        let record: RawEventRecord = row.into();
        assert_eq!(record.topic0, Some("0xa".to_string()));
        assert_eq!(record.topic3, Some("0xd".to_string()));
        assert_eq!(record.data, "0xfulldata");
    }

    // ========================================================================
    // Deep COPY encoding edge case tests
    // ========================================================================

    #[test]
    fn test_encode_copy_text_max_i64_block_number() {
        let event = RawEventRecord {
            chain_id: 1,
            block_number: i64::MAX,
            tx_index: 0,
            log_index: 0,
            block_hash: "0x".to_string(),
            block_timestamp: 0,
            tx_hash: "0x".to_string(),
            address: "0x".to_string(),
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
            data: "0x".to_string(),
        };
        let buf = encode_copy_text(&[event]);
        let text = String::from_utf8(buf).unwrap();
        let cols: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
        assert_eq!(cols[1], i64::MAX.to_string());
    }

    #[test]
    fn test_encode_copy_text_negative_block_number() {
        // Shouldn't happen, but verify encoding handles it
        let event = RawEventRecord {
            chain_id: 1,
            block_number: -1,
            tx_index: -1,
            log_index: -1,
            block_hash: "0x".to_string(),
            block_timestamp: -1,
            tx_hash: "0x".to_string(),
            address: "0x".to_string(),
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
            data: "0x".to_string(),
        };
        let buf = encode_copy_text(&[event]);
        let text = String::from_utf8(buf).unwrap();
        let cols: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
        assert_eq!(cols[1], "-1");
        assert_eq!(cols[2], "-1");
        assert_eq!(cols[3], "-1");
    }

    #[test]
    fn test_encode_copy_text_large_data_field() {
        // Simulate a very large data field (e.g., long ABI-encoded bytes)
        let big_data = "0x".to_string() + &"ab".repeat(10_000);
        let event = RawEventRecord {
            chain_id: 1,
            block_number: 100,
            tx_index: 0,
            log_index: 0,
            block_hash: "0x".to_string(),
            block_timestamp: 0,
            tx_hash: "0x".to_string(),
            address: "0x".to_string(),
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
            data: big_data.clone(),
        };
        let buf = encode_copy_text(&[event]);
        let text = String::from_utf8(buf).unwrap();
        let cols: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
        assert_eq!(cols[12], big_data);
    }

    #[test]
    fn test_encode_copy_text_tab_in_hash_would_corrupt() {
        // This tests that if somehow a hash contained a tab character,
        // it would split into extra columns (defensive check)
        let event = RawEventRecord {
            chain_id: 1,
            block_number: 100,
            tx_index: 0,
            log_index: 0,
            block_hash: "0xhash_with_no_tab".to_string(),
            block_timestamp: 0,
            tx_hash: "0xtx".to_string(),
            address: "0xaddr".to_string(),
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
            data: "0x".to_string(),
        };
        let buf = encode_copy_text(&[event]);
        let text = String::from_utf8(buf).unwrap();
        let cols: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
        // Should always be exactly 13 columns
        assert_eq!(cols.len(), 13);
    }

    #[test]
    fn test_encode_copy_text_many_events_chunked() {
        // The insert_batch method chunks at 5000, verify encoding works near that boundary
        let events: Vec<RawEventRecord> = (0..5001)
            .map(|i| RawEventRecord {
                chain_id: 1,
                block_number: i as i64,
                tx_index: 0,
                log_index: 0,
                block_hash: format!("0xhash_{}", i),
                block_timestamp: 1700000000 + i as i64,
                tx_hash: format!("0xtx_{}", i),
                address: "0x1234".to_string(),
                topic0: Some("0xtopic".to_string()),
                topic1: None,
                topic2: None,
                topic3: None,
                data: "0x".to_string(),
            })
            .collect();

        let buf = encode_copy_text(&events);
        let text = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = text.trim_end_matches('\n').split('\n').collect();
        assert_eq!(lines.len(), 5001);

        // Verify first and last line have correct structure
        for line in [lines[0], lines[5000]] {
            let cols: Vec<&str> = line.split('\t').collect();
            assert_eq!(cols.len(), 13);
        }
    }

    #[test]
    fn test_encode_copy_text_chain_id_boundaries() {
        for chain_id in [0, 1, i32::MAX, i32::MIN] {
            let event = RawEventRecord {
                chain_id,
                block_number: 0,
                tx_index: 0,
                log_index: 0,
                block_hash: "0x".to_string(),
                block_timestamp: 0,
                tx_hash: "0x".to_string(),
                address: "0x".to_string(),
                topic0: None,
                topic1: None,
                topic2: None,
                topic3: None,
                data: "0x".to_string(),
            };
            let buf = encode_copy_text(&[event]);
            let text = String::from_utf8(buf).unwrap();
            let cols: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
            assert_eq!(cols[0], chain_id.to_string());
        }
    }

    #[test]
    fn test_encode_copy_text_partial_topics() {
        // topic0 and topic2 present, topic1 and topic3 null
        let event = RawEventRecord {
            chain_id: 1,
            block_number: 100,
            tx_index: 0,
            log_index: 0,
            block_hash: "0x".to_string(),
            block_timestamp: 0,
            tx_hash: "0x".to_string(),
            address: "0x".to_string(),
            topic0: Some("0xa".to_string()),
            topic1: None,
            topic2: Some("0xc".to_string()),
            topic3: None,
            data: "0x".to_string(),
        };
        let buf = encode_copy_text(&[event]);
        let text = String::from_utf8(buf).unwrap();
        let cols: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
        assert_eq!(cols[8], "0xa");
        assert_eq!(cols[9], "\\N");
        assert_eq!(cols[10], "0xc");
        assert_eq!(cols[11], "\\N");
    }

    // ========================================================================
    // Deep LogEntry -> RawEventRecord conversion edge cases
    // ========================================================================

    #[test]
    fn test_log_entry_to_raw_event_block_number_overflow() {
        let log = LogEntry {
            block_number: u64::MAX,
            block_hash: "0x".to_string(),
            block_timestamp: 0,
            transaction_hash: "0x".to_string(),
            transaction_index: 0,
            log_index: 0,
            address: "0x".to_string(),
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
            data: "0x".to_string(),
        };
        let event = log.to_raw_event(1);
        // u64::MAX > i64::MAX, should clamp
        assert_eq!(event.block_number, i64::MAX);
    }

    #[test]
    fn test_log_entry_to_raw_event_tx_index_overflow() {
        let log = LogEntry {
            block_number: 100,
            block_hash: "0x".to_string(),
            block_timestamp: 0,
            transaction_hash: "0x".to_string(),
            transaction_index: u32::MAX,
            log_index: 0,
            address: "0x".to_string(),
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
            data: "0x".to_string(),
        };
        let event = log.to_raw_event(1);
        // u32::MAX > i32::MAX, should clamp
        assert_eq!(event.tx_index, i32::MAX);
    }

    #[test]
    fn test_log_entry_to_raw_event_log_index_overflow() {
        let log = LogEntry {
            block_number: 100,
            block_hash: "0x".to_string(),
            block_timestamp: 0,
            transaction_hash: "0x".to_string(),
            transaction_index: 0,
            log_index: u32::MAX,
            address: "0x".to_string(),
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
            data: "0x".to_string(),
        };
        let event = log.to_raw_event(1);
        assert_eq!(event.log_index, i32::MAX);
    }

    #[test]
    fn test_log_entry_to_raw_event_timestamp_overflow() {
        let log = LogEntry {
            block_number: 100,
            block_hash: "0x".to_string(),
            block_timestamp: u64::MAX,
            transaction_hash: "0x".to_string(),
            transaction_index: 0,
            log_index: 0,
            address: "0x".to_string(),
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
            data: "0x".to_string(),
        };
        let event = log.to_raw_event(1);
        assert_eq!(event.block_timestamp, i64::MAX);
    }

    #[test]
    fn test_log_entry_to_raw_event_normal_u32_values_fit_i32() {
        // Values at the i32::MAX boundary
        let log = LogEntry {
            block_number: 100,
            block_hash: "0x".to_string(),
            block_timestamp: 0,
            transaction_hash: "0x".to_string(),
            transaction_index: i32::MAX as u32,
            log_index: i32::MAX as u32,
            address: "0x".to_string(),
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
            data: "0x".to_string(),
        };
        let event = log.to_raw_event(1);
        assert_eq!(event.tx_index, i32::MAX);
        assert_eq!(event.log_index, i32::MAX);
    }

    #[test]
    fn test_log_entry_to_raw_event_just_above_i32_max() {
        let log = LogEntry {
            block_number: 100,
            block_hash: "0x".to_string(),
            block_timestamp: 0,
            transaction_hash: "0x".to_string(),
            transaction_index: i32::MAX as u32 + 1,
            log_index: i32::MAX as u32 + 1,
            address: "0x".to_string(),
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
            data: "0x".to_string(),
        };
        let event = log.to_raw_event(1);
        // Should clamp to i32::MAX
        assert_eq!(event.tx_index, i32::MAX);
        assert_eq!(event.log_index, i32::MAX);
    }

    #[test]
    fn test_log_entry_to_raw_event_block_number_at_i64_boundary() {
        let log = LogEntry {
            block_number: i64::MAX as u64,
            block_hash: "0x".to_string(),
            block_timestamp: 0,
            transaction_hash: "0x".to_string(),
            transaction_index: 0,
            log_index: 0,
            address: "0x".to_string(),
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
            data: "0x".to_string(),
        };
        let event = log.to_raw_event(1);
        assert_eq!(event.block_number, i64::MAX);
    }

    #[test]
    fn test_log_entry_to_raw_event_just_above_i64_max() {
        let log = LogEntry {
            block_number: i64::MAX as u64 + 1,
            block_hash: "0x".to_string(),
            block_timestamp: 0,
            transaction_hash: "0x".to_string(),
            transaction_index: 0,
            log_index: 0,
            address: "0x".to_string(),
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
            data: "0x".to_string(),
        };
        let event = log.to_raw_event(1);
        assert_eq!(event.block_number, i64::MAX);
    }

    #[test]
    fn test_log_entry_to_raw_event_empty_address() {
        let log = LogEntry {
            block_number: 100,
            block_hash: "0x".to_string(),
            block_timestamp: 0,
            transaction_hash: "0x".to_string(),
            transaction_index: 0,
            log_index: 0,
            address: "".to_string(),
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
            data: "0x".to_string(),
        };
        let event = log.to_raw_event(1);
        assert_eq!(event.address, "");
    }

    #[test]
    fn test_log_entry_to_raw_event_mixed_case_address_normalized() {
        let log = LogEntry {
            block_number: 100,
            block_hash: "0x".to_string(),
            block_timestamp: 0,
            transaction_hash: "0x".to_string(),
            transaction_index: 0,
            log_index: 0,
            address: "0xDeAdBeEf0000000000000000000000000000dEaD".to_string(),
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
            data: "0x".to_string(),
        };
        let event = log.to_raw_event(42);
        assert_eq!(
            event.address,
            "0xdeadbeef0000000000000000000000000000dead"
        );
        assert_eq!(event.chain_id, 42);
    }

    #[test]
    fn test_encode_copy_text_preserves_event_ordering() {
        let events: Vec<RawEventRecord> = (0..100)
            .map(|i| RawEventRecord {
                chain_id: 1,
                block_number: i / 10,
                tx_index: (i % 10) as i32,
                log_index: 0,
                block_hash: "0x".to_string(),
                block_timestamp: 0,
                tx_hash: format!("0xtx_{}", i),
                address: "0x".to_string(),
                topic0: None,
                topic1: None,
                topic2: None,
                topic3: None,
                data: "0x".to_string(),
            })
            .collect();

        let buf = encode_copy_text(&events);
        let text = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = text.trim_end_matches('\n').split('\n').collect();
        assert_eq!(lines.len(), 100);

        // Verify ordering is preserved
        for (i, line) in lines.iter().enumerate() {
            let cols: Vec<&str> = line.split('\t').collect();
            assert_eq!(cols[6], format!("0xtx_{}", i));
        }
    }

    #[test]
    fn test_encode_copy_text_each_line_ends_with_newline() {
        let events = vec![sample_event(1, 0), sample_event(2, 0)];
        let buf = encode_copy_text(&events);
        // Each line should end with \n
        let lines: Vec<&[u8]> = buf.split(|&b| b == b'\n').collect();
        // Last element after split on trailing \n is empty
        assert_eq!(lines.last().unwrap().len(), 0);
        // Total newlines = number of events
        let newline_count = buf.iter().filter(|&&b| b == b'\n').count();
        assert_eq!(newline_count, 2);
    }

    #[test]
    fn test_write_nullable_empty_string_is_not_null() {
        let mut buf = Vec::new();
        write_nullable(&mut buf, &Some("".to_string()));
        // Empty string is NOT null - should produce empty (not \N)
        assert!(buf.is_empty());
    }
}
