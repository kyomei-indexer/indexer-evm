use crate::abi::decoder::{to_snake_case, AbiDecoder, DecodedEventRow};
use crate::filter::EventFilterEngine;
use crate::types::RawEventRecord;
use anyhow::Result;
use sqlx::{PgPool, Postgres, Transaction};
use std::collections::HashMap;
use std::io::Write;
use tracing::trace;

/// Repository for decoded per-event tables
pub struct DecodedEventRepository {
    pool: PgPool,
    schema: String,
}

impl DecodedEventRepository {
    pub fn new(pool: PgPool, schema: String) -> Self {
        Self { pool, schema }
    }

    /// Decode raw events and insert into per-event decoded tables using COPY
    pub async fn insert_batch(
        &self,
        events: &[RawEventRecord],
        decoder: &AbiDecoder,
    ) -> Result<u64> {
        self.insert_batch_filtered(events, decoder, None).await
    }

    /// Decode raw events with optional filtering and insert into per-event decoded tables
    pub async fn insert_batch_filtered(
        &self,
        events: &[RawEventRecord],
        decoder: &AbiDecoder,
        filter: Option<&EventFilterEngine>,
    ) -> Result<u64> {
        if events.is_empty() {
            return Ok(0);
        }

        let by_table = decode_and_group(events, decoder, filter);

        if by_table.is_empty() {
            return Ok(0);
        }

        let mut total = 0u64;
        for (table_name, (param_cols, rows)) in &by_table {
            let inserted = self.copy_to_table(table_name, param_cols, rows).await?;
            total += inserted;
        }
        trace!(total, tables = by_table.len(), "Inserted decoded events");
        Ok(total)
    }

    /// COPY decoded rows into a single decoded event table via staging table
    async fn copy_to_table(
        &self,
        table_name: &str,
        param_cols: &[String],
        rows: &[Vec<u8>],
    ) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }

        let stg_table = format!("_stg_{}", table_name);
        let base_cols = "chain_id, block_number, tx_index, log_index, \
                         block_hash, block_timestamp, tx_hash, address";
        let all_cols = if param_cols.is_empty() {
            base_cols.to_string()
        } else {
            format!("{}, {}", base_cols, param_cols.join(", "))
        };

        let mut tx = self.pool.begin().await?;

        sqlx::query(&format!(
            "CREATE TEMP TABLE IF NOT EXISTS {} (LIKE {}.{}) ON COMMIT DELETE ROWS",
            stg_table, self.schema, table_name
        ))
        .execute(&mut *tx)
        .await?;

        {
            let copy_sql = format!("COPY {} ({}) FROM STDIN", stg_table, all_cols);
            let mut copy = tx.as_mut().copy_in_raw(&copy_sql).await?;

            for chunk in rows.chunks(5000) {
                let mut buf = Vec::with_capacity(chunk.len() * 256);
                for row in chunk {
                    buf.extend_from_slice(row);
                }
                copy.send(&buf[..]).await?;
            }

            copy.finish().await?;
        }

        let result = sqlx::query(&format!(
            "INSERT INTO {}.{} SELECT * FROM {} ON CONFLICT DO NOTHING",
            self.schema, table_name, stg_table
        ))
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(result.rows_affected())
    }

    /// Decode raw events and insert into per-event decoded tables within an existing transaction.
    /// Used by live sync to ensure atomicity across raw events, decoded events, and worker checkpoint.
    pub async fn insert_batch_in_tx(
        &self,
        events: &[RawEventRecord],
        decoder: &AbiDecoder,
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<u64> {
        self.insert_batch_in_tx_filtered(events, decoder, tx, None).await
    }

    /// Decode raw events with optional filtering within an existing transaction
    pub async fn insert_batch_in_tx_filtered(
        &self,
        events: &[RawEventRecord],
        decoder: &AbiDecoder,
        tx: &mut Transaction<'_, Postgres>,
        filter: Option<&EventFilterEngine>,
    ) -> Result<u64> {
        if events.is_empty() {
            return Ok(0);
        }

        let by_table = decode_and_group(events, decoder, filter);

        if by_table.is_empty() {
            return Ok(0);
        }

        let mut total = 0u64;
        for (table_name, (param_cols, rows)) in &by_table {
            let inserted = self.copy_to_table_in_tx(table_name, param_cols, rows, tx).await?;
            total += inserted;
        }
        trace!(total, tables = by_table.len(), "Inserted decoded events (in tx)");
        Ok(total)
    }

    /// COPY decoded rows into a single decoded event table within an existing transaction
    async fn copy_to_table_in_tx(
        &self,
        table_name: &str,
        param_cols: &[String],
        rows: &[Vec<u8>],
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }

        let stg_table = format!("_stg_{}", table_name);
        let base_cols = "chain_id, block_number, tx_index, log_index, \
                         block_hash, block_timestamp, tx_hash, address";
        let all_cols = if param_cols.is_empty() {
            base_cols.to_string()
        } else {
            format!("{}, {}", base_cols, param_cols.join(", "))
        };

        sqlx::query(&format!(
            "CREATE TEMP TABLE IF NOT EXISTS {} (LIKE {}.{}) ON COMMIT DELETE ROWS",
            stg_table, self.schema, table_name
        ))
        .execute(&mut **tx)
        .await?;

        {
            let copy_sql = format!("COPY {} ({}) FROM STDIN", stg_table, all_cols);
            let mut copy = tx.as_mut().copy_in_raw(&copy_sql).await?;

            for chunk in rows.chunks(5000) {
                let mut buf = Vec::with_capacity(chunk.len() * 256);
                for row in chunk {
                    buf.extend_from_slice(row);
                }
                copy.send(&buf[..]).await?;
            }

            copy.finish().await?;
        }

        let result = sqlx::query(&format!(
            "INSERT INTO {}.{} SELECT * FROM {} ON CONFLICT DO NOTHING",
            self.schema, table_name, stg_table
        ))
        .execute(&mut **tx)
        .await?;

        Ok(result.rows_affected())
    }

    /// Decode raw events with optional filtering and COPY directly into target tables.
    /// No staging table, no ON CONFLICT — fastest path for exclusive block ranges.
    /// Returns Err on any failure including unique violations.
    pub async fn insert_batch_direct_in_tx_filtered(
        &self,
        events: &[RawEventRecord],
        decoder: &AbiDecoder,
        tx: &mut Transaction<'_, Postgres>,
        filter: Option<&EventFilterEngine>,
    ) -> Result<u64> {
        if events.is_empty() {
            return Ok(0);
        }

        let by_table = decode_and_group(events, decoder, filter);

        if by_table.is_empty() {
            return Ok(0);
        }

        let mut total = 0u64;
        for (table_name, (param_cols, rows)) in &by_table {
            let inserted = self
                .copy_to_table_direct_in_tx(table_name, param_cols, rows, tx)
                .await?;
            total += inserted;
        }
        trace!(total, tables = by_table.len(), "Inserted decoded events via direct COPY (in tx)");
        Ok(total)
    }

    /// COPY decoded rows directly into a target table within an existing transaction.
    /// No staging table — propagates all errors including unique violations.
    async fn copy_to_table_direct_in_tx(
        &self,
        table_name: &str,
        param_cols: &[String],
        rows: &[Vec<u8>],
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }

        let base_cols = "chain_id, block_number, tx_index, log_index, \
                         block_hash, block_timestamp, tx_hash, address";
        let all_cols = if param_cols.is_empty() {
            base_cols.to_string()
        } else {
            format!("{}, {}", base_cols, param_cols.join(", "))
        };

        let copy_sql = format!(
            "COPY {}.{} ({}) FROM STDIN",
            self.schema, table_name, all_cols
        );
        let mut copy = tx.as_mut().copy_in_raw(&copy_sql).await?;

        for chunk in rows.chunks(5000) {
            let mut buf = Vec::with_capacity(chunk.len() * 256);
            for row in chunk {
                buf.extend_from_slice(row);
            }
            copy.send(&buf[..]).await?;
        }

        copy.finish().await?;

        Ok(rows.len() as u64)
    }

    /// Delete decoded events in a block range (for reorg handling)
    pub async fn delete_range(
        &self,
        chain_id: i32,
        from_block: i64,
        to_block: i64,
        decoder: &AbiDecoder,
    ) -> Result<u64> {
        let mut total = 0u64;
        for (contract_name, events) in decoder.all_contract_events() {
            for event in events {
                let table_name = format!(
                    "event_{}_{}",
                    to_snake_case(contract_name),
                    to_snake_case(&event.name)
                );
                let result = sqlx::query(&format!(
                    "DELETE FROM {}.{} WHERE chain_id = $1 AND block_number >= $2 AND block_number <= $3",
                    self.schema, table_name
                ))
                .bind(chain_id)
                .bind(from_block)
                .bind(to_block)
                .execute(&self.pool)
                .await?;
                total += result.rows_affected();
            }
        }
        Ok(total)
    }
}

/// Decode raw events and group by table name, applying optional filters.
/// Returns a map of table_name → (param_columns, encoded_rows).
fn decode_and_group(
    events: &[RawEventRecord],
    decoder: &AbiDecoder,
    filter: Option<&EventFilterEngine>,
) -> HashMap<String, (Vec<String>, Vec<Vec<u8>>)> {
    use rayon::prelude::*;

    if let Some(filter_engine) = filter {
        // Filter path: need DecodedEventRow to check filter predicates
        let decoded_rows: Vec<DecodedEventRow> = events
            .par_iter()
            .filter_map(|event| {
                let decoded = decoder.decode_raw_event(event)?;

                let params: Vec<(&str, &str)> = decoded
                    .param_columns
                    .iter()
                    .zip(decoded.param_values.iter())
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect();

                if !filter_engine.should_include(
                    &decoded.contract_name,
                    &decoded.event_name,
                    &params,
                ) {
                    return None;
                }

                Some(decoded)
            })
            .collect();

        let mut by_table: HashMap<String, (Vec<String>, Vec<Vec<u8>>)> = HashMap::new();
        for decoded in &decoded_rows {
            encode_decoded_row(decoded, &mut by_table);
        }
        by_table
    } else {
        // Fast path: decode directly to COPY bytes, skipping DecodedEventRow entirely.
        // Uses decode_raw_event_to_copy_rows to emit into all matching contract tables
        // when multiple contracts share the same event signature (e.g., ERC20 Transfer).
        let all_rows: Vec<Vec<(&str, &[String], Vec<u8>)>> = events
            .par_iter()
            .map(|event| decoder.decode_raw_event_to_copy_rows(event))
            .collect();

        let mut by_table: HashMap<String, (Vec<String>, Vec<Vec<u8>>)> = HashMap::new();
        for rows in all_rows {
            for (table_name, param_cols, row_bytes) in rows {
                by_table
                    .entry(table_name.to_string())
                    .or_insert_with(|| (param_cols.to_vec(), Vec::new()))
                    .1
                    .push(row_bytes);
            }
        }
        by_table
    }
}

/// Encode a decoded row into the COPY format and add to the by_table map
fn encode_decoded_row(
    decoded: &DecodedEventRow,
    by_table: &mut HashMap<String, (Vec<String>, Vec<Vec<u8>>)>,
) {
    let entry = by_table
        .entry(decoded.table_name.clone())
        .or_insert_with(|| (decoded.param_columns.clone(), Vec::new()));

    let mut row = Vec::with_capacity(256);
    write!(row, "{}", decoded.chain_id).unwrap();
    row.push(b'\t');
    write!(row, "{}", decoded.block_number).unwrap();
    row.push(b'\t');
    write!(row, "{}", decoded.tx_index).unwrap();
    row.push(b'\t');
    write!(row, "{}", decoded.log_index).unwrap();
    row.push(b'\t');
    row.extend_from_slice(decoded.block_hash.as_bytes());
    row.push(b'\t');
    write!(row, "{}", decoded.block_timestamp).unwrap();
    row.push(b'\t');
    row.extend_from_slice(decoded.tx_hash.as_bytes());
    row.push(b'\t');
    row.extend_from_slice(decoded.address.as_bytes());
    for val in &decoded.param_values {
        row.push(b'\t');
        row.extend_from_slice(val.as_bytes());
    }
    row.push(b'\n');

    entry.1.push(row);
}
