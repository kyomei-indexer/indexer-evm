use crate::types::RawAccountEventRecord;
use anyhow::Result;
use sqlx::{PgPool, Postgres, Transaction};
use std::io::Write;
use tracing::trace;

/// Repository for raw account event records (transaction and transfer tracking)
pub struct AccountEventRepository {
    pool: PgPool,
    schema: String,
}

impl AccountEventRepository {
    pub fn new(pool: PgPool, schema: String) -> Self {
        Self { pool, schema }
    }

    /// Insert a batch of account events using PostgreSQL COPY for maximum throughput.
    pub async fn insert_batch(&self, events: &[RawAccountEventRecord]) -> Result<u64> {
        if events.is_empty() {
            return Ok(0);
        }

        let copy_start = std::time::Instant::now();
        let mut tx = self.pool.begin().await?;

        sqlx::query(&format!(
            "CREATE TEMP TABLE IF NOT EXISTS _stg_raw_account_events \
             (LIKE {}.raw_account_events) ON COMMIT DELETE ROWS",
            self.schema
        ))
        .execute(&mut *tx)
        .await?;

        {
            let mut copy = tx
                .as_mut()
                .copy_in_raw(
                    "COPY _stg_raw_account_events (\
                     chain_id, block_number, tx_index, event_type, \
                     address, tx_hash, block_hash, block_timestamp, \
                     counterparty, value, input, trace_address\
                     ) FROM STDIN",
                )
                .await?;

            for chunk in events.chunks(5000) {
                let buf = encode_account_event_copy_text(chunk);
                copy.send(&buf[..]).await?;
            }

            copy.finish().await?;
        }

        let result = sqlx::query(&format!(
            "INSERT INTO {}.raw_account_events SELECT * FROM _stg_raw_account_events ON CONFLICT DO NOTHING",
            self.schema
        ))
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        let count = result.rows_affected();
        if let Some(first) = events.first() {
            crate::metrics::db_copy_duration(first.chain_id as u32, copy_start.elapsed().as_secs_f64());
        }
        trace!(count, events = events.len(), "Inserted account events via COPY");
        Ok(count)
    }

    /// Insert a batch of account events within an existing transaction.
    pub async fn insert_batch_in_tx(
        &self,
        events: &[RawAccountEventRecord],
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<u64> {
        if events.is_empty() {
            return Ok(0);
        }

        sqlx::query(&format!(
            "CREATE TEMP TABLE IF NOT EXISTS _stg_raw_account_events \
             (LIKE {}.raw_account_events) ON COMMIT DELETE ROWS",
            self.schema
        ))
        .execute(&mut **tx)
        .await?;

        {
            let mut copy = tx
                .as_mut()
                .copy_in_raw(
                    "COPY _stg_raw_account_events (\
                     chain_id, block_number, tx_index, event_type, \
                     address, tx_hash, block_hash, block_timestamp, \
                     counterparty, value, input, trace_address\
                     ) FROM STDIN",
                )
                .await?;

            for chunk in events.chunks(5000) {
                let buf = encode_account_event_copy_text(chunk);
                copy.send(&buf[..]).await?;
            }

            copy.finish().await?;
        }

        let result = sqlx::query(&format!(
            "INSERT INTO {}.raw_account_events SELECT * FROM _stg_raw_account_events ON CONFLICT DO NOTHING",
            self.schema
        ))
        .execute(&mut **tx)
        .await?;

        let count = result.rows_affected();
        trace!(count, events = events.len(), "Inserted account events via COPY (in tx)");
        Ok(count)
    }

    /// Delete account events in a block range (used for reorg handling)
    pub async fn delete_range(
        &self,
        chain_id: i32,
        from_block: i64,
        to_block: i64,
    ) -> Result<u64> {
        let result = sqlx::query(&format!(
            "DELETE FROM {}.raw_account_events \
             WHERE chain_id = $1 AND block_number >= $2 AND block_number <= $3",
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
            "SELECT MAX(block_number) FROM {}.raw_account_events WHERE chain_id = $1",
            self.schema
        ))
        .bind(chain_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(result)
    }

    /// Count account events for a chain
    pub async fn count(&self, chain_id: i32) -> Result<i64> {
        let result = sqlx::query_scalar::<_, i64>(&format!(
            "SELECT COUNT(*) FROM {}.raw_account_events WHERE chain_id = $1",
            self.schema
        ))
        .bind(chain_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(result)
    }
}

/// Encode account events as PostgreSQL COPY text format.
fn encode_account_event_copy_text(events: &[RawAccountEventRecord]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(events.len() * 200);

    for e in events {
        write!(buf, "{}", e.chain_id).unwrap();
        buf.push(b'\t');
        write!(buf, "{}", e.block_number).unwrap();
        buf.push(b'\t');
        write!(buf, "{}", e.tx_index).unwrap();
        buf.push(b'\t');
        buf.extend_from_slice(e.event_type.as_bytes());
        buf.push(b'\t');
        buf.extend_from_slice(e.address.as_bytes());
        buf.push(b'\t');
        buf.extend_from_slice(e.tx_hash.as_bytes());
        buf.push(b'\t');
        buf.extend_from_slice(e.block_hash.as_bytes());
        buf.push(b'\t');
        write!(buf, "{}", e.block_timestamp).unwrap();
        buf.push(b'\t');
        buf.extend_from_slice(e.counterparty.as_bytes());
        buf.push(b'\t');
        buf.extend_from_slice(e.value.as_bytes());
        buf.push(b'\t');
        write_nullable(&mut buf, &e.input);
        buf.push(b'\t');
        write_nullable(&mut buf, &e.trace_address);
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

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_tx_event(block: i64, event_type: &str) -> RawAccountEventRecord {
        RawAccountEventRecord {
            chain_id: 1,
            block_number: block,
            tx_index: 0,
            tx_hash: "0xdef".to_string(),
            block_hash: "0xabc".to_string(),
            block_timestamp: 1700000000,
            event_type: event_type.to_string(),
            address: "0xaaaa".to_string(),
            counterparty: "0xbbbb".to_string(),
            value: "1000000000000000000".to_string(),
            input: Some("0x38ed1739".to_string()),
            trace_address: None,
        }
    }

    #[test]
    fn test_encode_account_event_copy_text_single() {
        let events = vec![sample_tx_event(18000000, "transaction:from")];
        let buf = encode_account_event_copy_text(&events);
        let text = String::from_utf8(buf).unwrap();
        let cols: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();

        assert_eq!(cols.len(), 12);
        assert_eq!(cols[0], "1");                          // chain_id
        assert_eq!(cols[1], "18000000");                   // block_number
        assert_eq!(cols[2], "0");                          // tx_index
        assert_eq!(cols[3], "transaction:from");           // event_type
        assert_eq!(cols[4], "0xaaaa");                     // address
        assert_eq!(cols[5], "0xdef");                      // tx_hash
        assert_eq!(cols[6], "0xabc");                      // block_hash
        assert_eq!(cols[7], "1700000000");                 // block_timestamp
        assert_eq!(cols[8], "0xbbbb");                     // counterparty
        assert_eq!(cols[9], "1000000000000000000");        // value
        assert_eq!(cols[10], "0x38ed1739");                // input
        assert_eq!(cols[11], "\\N");                       // trace_address (None)
    }

    #[test]
    fn test_encode_account_event_copy_text_empty() {
        let buf = encode_account_event_copy_text(&[]);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_encode_account_event_copy_text_multiple() {
        let events = vec![
            sample_tx_event(100, "transaction:from"),
            sample_tx_event(100, "transaction:to"),
            sample_tx_event(101, "transfer:from"),
        ];
        let buf = encode_account_event_copy_text(&events);
        let text = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = text.trim_end_matches('\n').split('\n').collect();
        assert_eq!(lines.len(), 3);
    }

    #[test]
    fn test_encode_account_event_transfer_with_trace_address() {
        let event = RawAccountEventRecord {
            chain_id: 1,
            block_number: 100,
            tx_index: 0,
            tx_hash: "0x1".to_string(),
            block_hash: "0x2".to_string(),
            block_timestamp: 1000,
            event_type: "transfer:from".to_string(),
            address: "0xaaaa".to_string(),
            counterparty: "0xbbbb".to_string(),
            value: "500".to_string(),
            input: None,
            trace_address: Some("0.1".to_string()),
        };

        let buf = encode_account_event_copy_text(&[event]);
        let text = String::from_utf8(buf).unwrap();
        let cols: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();

        assert_eq!(cols[3], "transfer:from");
        assert_eq!(cols[10], "\\N");       // input (None)
        assert_eq!(cols[11], "0.1");       // trace_address (Some)
    }

    #[test]
    fn test_encode_account_event_all_event_types() {
        for event_type in &[
            "transaction:from",
            "transaction:to",
            "transfer:from",
            "transfer:to",
        ] {
            let event = sample_tx_event(100, event_type);
            let buf = encode_account_event_copy_text(&[event]);
            let text = String::from_utf8(buf).unwrap();
            let cols: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
            assert_eq!(cols[3], *event_type);
        }
    }

    #[test]
    fn test_encode_account_event_both_nullable_none() {
        let event = RawAccountEventRecord {
            chain_id: 1,
            block_number: 100,
            tx_index: 0,
            tx_hash: "0x".to_string(),
            block_hash: "0x".to_string(),
            block_timestamp: 0,
            event_type: "transaction:from".to_string(),
            address: "0x".to_string(),
            counterparty: "0x".to_string(),
            value: "0".to_string(),
            input: None,
            trace_address: None,
        };

        let buf = encode_account_event_copy_text(&[event]);
        let text = String::from_utf8(buf).unwrap();
        let cols: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();

        assert_eq!(cols[10], "\\N");
        assert_eq!(cols[11], "\\N");
    }

    #[test]
    fn test_encode_account_event_both_nullable_present() {
        let event = RawAccountEventRecord {
            chain_id: 1,
            block_number: 100,
            tx_index: 0,
            tx_hash: "0x".to_string(),
            block_hash: "0x".to_string(),
            block_timestamp: 0,
            event_type: "transfer:from".to_string(),
            address: "0x".to_string(),
            counterparty: "0x".to_string(),
            value: "0".to_string(),
            input: Some("0xdeadbeef".to_string()),
            trace_address: Some("0.1.2".to_string()),
        };

        let buf = encode_account_event_copy_text(&[event]);
        let text = String::from_utf8(buf).unwrap();
        let cols: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();

        assert_eq!(cols[10], "0xdeadbeef");
        assert_eq!(cols[11], "0.1.2");
    }

    #[test]
    fn test_encode_account_event_large_batch() {
        let events: Vec<RawAccountEventRecord> = (0..5001)
            .map(|i| sample_tx_event(i as i64, "transaction:from"))
            .collect();
        let buf = encode_account_event_copy_text(&events);
        let text = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = text.trim_end_matches('\n').split('\n').collect();
        assert_eq!(lines.len(), 5001);
    }
}
