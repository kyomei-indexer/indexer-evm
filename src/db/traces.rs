use crate::types::RawTraceRecord;
use anyhow::Result;
use sqlx::{PgPool, Postgres, Transaction};
use std::io::Write;
use tracing::trace;

/// Repository for raw call trace records
pub struct TraceRepository {
    pool: PgPool,
    schema: String,
}

impl TraceRepository {
    pub fn new(pool: PgPool, schema: String) -> Self {
        Self { pool, schema }
    }

    /// Insert a batch of traces using PostgreSQL COPY for maximum throughput.
    /// Uses a temp staging table with INSERT...ON CONFLICT DO NOTHING for idempotency.
    pub async fn insert_batch(&self, traces: &[RawTraceRecord]) -> Result<u64> {
        if traces.is_empty() {
            return Ok(0);
        }

        let copy_start = std::time::Instant::now();
        let mut tx = self.pool.begin().await?;

        sqlx::query(&format!(
            "CREATE TEMP TABLE IF NOT EXISTS _stg_raw_traces \
             (LIKE {}.raw_traces) ON COMMIT DELETE ROWS",
            self.schema
        ))
        .execute(&mut *tx)
        .await?;

        {
            let mut copy = tx
                .as_mut()
                .copy_in_raw(
                    "COPY _stg_raw_traces (\
                     chain_id, block_number, tx_index, trace_address, \
                     tx_hash, block_hash, block_timestamp, call_type, \
                     from_address, to_address, input, output, \
                     value, gas, gas_used, error, \
                     function_name, function_sig\
                     ) FROM STDIN",
                )
                .await?;

            for chunk in traces.chunks(5000) {
                let buf = encode_trace_copy_text(chunk);
                copy.send(&buf[..]).await?;
            }

            copy.finish().await?;
        }

        let result = sqlx::query(&format!(
            "INSERT INTO {}.raw_traces SELECT * FROM _stg_raw_traces ON CONFLICT DO NOTHING",
            self.schema
        ))
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        let count = result.rows_affected();
        if let Some(first) = traces.first() {
            crate::metrics::db_copy_duration(first.chain_id as u32, copy_start.elapsed().as_secs_f64());
        }
        trace!(count, traces = traces.len(), "Inserted traces via COPY");
        Ok(count)
    }

    /// Insert a batch of traces within an existing transaction.
    pub async fn insert_batch_in_tx(
        &self,
        traces: &[RawTraceRecord],
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<u64> {
        if traces.is_empty() {
            return Ok(0);
        }

        sqlx::query(&format!(
            "CREATE TEMP TABLE IF NOT EXISTS _stg_raw_traces \
             (LIKE {}.raw_traces) ON COMMIT DELETE ROWS",
            self.schema
        ))
        .execute(&mut **tx)
        .await?;

        {
            let mut copy = tx
                .as_mut()
                .copy_in_raw(
                    "COPY _stg_raw_traces (\
                     chain_id, block_number, tx_index, trace_address, \
                     tx_hash, block_hash, block_timestamp, call_type, \
                     from_address, to_address, input, output, \
                     value, gas, gas_used, error, \
                     function_name, function_sig\
                     ) FROM STDIN",
                )
                .await?;

            for chunk in traces.chunks(5000) {
                let buf = encode_trace_copy_text(chunk);
                copy.send(&buf[..]).await?;
            }

            copy.finish().await?;
        }

        let result = sqlx::query(&format!(
            "INSERT INTO {}.raw_traces SELECT * FROM _stg_raw_traces ON CONFLICT DO NOTHING",
            self.schema
        ))
        .execute(&mut **tx)
        .await?;

        let count = result.rows_affected();
        trace!(count, traces = traces.len(), "Inserted traces via COPY (in tx)");
        Ok(count)
    }

    /// Delete traces in a block range (used for reorg handling)
    pub async fn delete_range(
        &self,
        chain_id: i32,
        from_block: i64,
        to_block: i64,
    ) -> Result<u64> {
        let result = sqlx::query(&format!(
            "DELETE FROM {}.raw_traces \
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
            "SELECT MAX(block_number) FROM {}.raw_traces WHERE chain_id = $1",
            self.schema
        ))
        .bind(chain_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(result)
    }

    /// Count traces for a chain
    pub async fn count(&self, chain_id: i32) -> Result<i64> {
        let result = sqlx::query_scalar::<_, i64>(&format!(
            "SELECT COUNT(*) FROM {}.raw_traces WHERE chain_id = $1",
            self.schema
        ))
        .bind(chain_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(result)
    }
}

/// Encode traces as PostgreSQL COPY text format (tab-separated, newline-delimited).
fn encode_trace_copy_text(traces: &[RawTraceRecord]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(traces.len() * 300);

    for t in traces {
        write!(buf, "{}", t.chain_id).unwrap();
        buf.push(b'\t');
        write!(buf, "{}", t.block_number).unwrap();
        buf.push(b'\t');
        write!(buf, "{}", t.tx_index).unwrap();
        buf.push(b'\t');
        buf.extend_from_slice(t.trace_address.as_bytes());
        buf.push(b'\t');
        buf.extend_from_slice(t.tx_hash.as_bytes());
        buf.push(b'\t');
        buf.extend_from_slice(t.block_hash.as_bytes());
        buf.push(b'\t');
        write!(buf, "{}", t.block_timestamp).unwrap();
        buf.push(b'\t');
        buf.extend_from_slice(t.call_type.as_bytes());
        buf.push(b'\t');
        buf.extend_from_slice(t.from_address.as_bytes());
        buf.push(b'\t');
        buf.extend_from_slice(t.to_address.as_bytes());
        buf.push(b'\t');
        buf.extend_from_slice(t.input.as_bytes());
        buf.push(b'\t');
        buf.extend_from_slice(t.output.as_bytes());
        buf.push(b'\t');
        buf.extend_from_slice(t.value.as_bytes());
        buf.push(b'\t');
        write!(buf, "{}", t.gas).unwrap();
        buf.push(b'\t');
        write!(buf, "{}", t.gas_used).unwrap();
        buf.push(b'\t');
        write_nullable(&mut buf, &t.error);
        buf.push(b'\t');
        write_nullable(&mut buf, &t.function_name);
        buf.push(b'\t');
        write_nullable(&mut buf, &t.function_sig);
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

    fn sample_trace(block: i64, tx_idx: i32, trace_addr: &str) -> RawTraceRecord {
        RawTraceRecord {
            chain_id: 1,
            block_number: block,
            tx_index: tx_idx,
            trace_address: trace_addr.to_string(),
            tx_hash: "0xdef".to_string(),
            block_hash: "0xabc".to_string(),
            block_timestamp: 1700000000,
            call_type: "call".to_string(),
            from_address: "0xaaaa".to_string(),
            to_address: "0xbbbb".to_string(),
            input: "0x38ed1739".to_string(),
            output: "0x".to_string(),
            value: "1000000000000000000".to_string(),
            gas: 100000,
            gas_used: 50000,
            error: None,
            function_name: Some("swap".to_string()),
            function_sig: Some("0x38ed1739".to_string()),
        }
    }

    #[test]
    fn test_encode_trace_copy_text_single() {
        let traces = vec![sample_trace(18000000, 5, "0.1")];
        let buf = encode_trace_copy_text(&traces);
        let text = String::from_utf8(buf).unwrap();
        let cols: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();

        assert_eq!(cols.len(), 18);
        assert_eq!(cols[0], "1");              // chain_id
        assert_eq!(cols[1], "18000000");       // block_number
        assert_eq!(cols[2], "5");              // tx_index
        assert_eq!(cols[3], "0.1");            // trace_address
        assert_eq!(cols[4], "0xdef");          // tx_hash
        assert_eq!(cols[5], "0xabc");          // block_hash
        assert_eq!(cols[6], "1700000000");     // block_timestamp
        assert_eq!(cols[7], "call");           // call_type
        assert_eq!(cols[8], "0xaaaa");         // from_address
        assert_eq!(cols[9], "0xbbbb");         // to_address
        assert_eq!(cols[10], "0x38ed1739");    // input
        assert_eq!(cols[11], "0x");            // output
        assert_eq!(cols[12], "1000000000000000000"); // value
        assert_eq!(cols[13], "100000");        // gas
        assert_eq!(cols[14], "50000");         // gas_used
        assert_eq!(cols[15], "\\N");           // error (None)
        assert_eq!(cols[16], "swap");          // function_name
        assert_eq!(cols[17], "0x38ed1739");    // function_sig
    }

    #[test]
    fn test_encode_trace_copy_text_empty() {
        let buf = encode_trace_copy_text(&[]);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_encode_trace_copy_text_multiple() {
        let traces = vec![
            sample_trace(100, 0, ""),
            sample_trace(100, 0, "0"),
            sample_trace(100, 0, "0.1"),
        ];
        let buf = encode_trace_copy_text(&traces);
        let text = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = text.trim_end_matches('\n').split('\n').collect();
        assert_eq!(lines.len(), 3);
    }

    #[test]
    fn test_encode_trace_copy_text_with_error() {
        let mut trace = sample_trace(100, 0, "");
        trace.error = Some("execution reverted".to_string());
        trace.function_name = None;
        trace.function_sig = None;

        let buf = encode_trace_copy_text(&[trace]);
        let text = String::from_utf8(buf).unwrap();
        let cols: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();

        assert_eq!(cols[15], "execution reverted"); // error
        assert_eq!(cols[16], "\\N");                // function_name
        assert_eq!(cols[17], "\\N");                // function_sig
    }

    #[test]
    fn test_encode_trace_copy_text_all_nullable_fields_none() {
        let mut trace = sample_trace(100, 0, "");
        trace.error = None;
        trace.function_name = None;
        trace.function_sig = None;

        let buf = encode_trace_copy_text(&[trace]);
        let text = String::from_utf8(buf).unwrap();
        let cols: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();

        assert_eq!(cols[15], "\\N");
        assert_eq!(cols[16], "\\N");
        assert_eq!(cols[17], "\\N");
    }

    #[test]
    fn test_encode_trace_copy_text_empty_trace_address() {
        let trace = sample_trace(100, 0, "");
        let buf = encode_trace_copy_text(&[trace]);
        let text = String::from_utf8(buf).unwrap();
        let cols: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
        assert_eq!(cols[3], ""); // empty trace_address for root call
    }

    #[test]
    fn test_encode_trace_copy_text_large_batch() {
        let traces: Vec<RawTraceRecord> = (0..5001)
            .map(|i| sample_trace(i as i64, 0, &format!("0.{}", i)))
            .collect();
        let buf = encode_trace_copy_text(&traces);
        let text = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = text.trim_end_matches('\n').split('\n').collect();
        assert_eq!(lines.len(), 5001);
    }
}
