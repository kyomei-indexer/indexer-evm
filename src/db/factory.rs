use crate::types::FactoryChild;
use anyhow::Result;
use sqlx::PgPool;
use std::io::Write;
use tracing::trace;

/// Repository for factory-discovered child contracts
pub struct FactoryRepository {
    pool: PgPool,
    schema: String,
}

impl FactoryRepository {
    pub fn new(pool: PgPool, schema: String) -> Self {
        Self { pool, schema }
    }

    /// Insert a batch of factory children using COPY for throughput.
    /// Uses a staging table with INSERT...ON CONFLICT DO NOTHING for idempotency.
    pub async fn insert_batch(&self, children: &[FactoryChild]) -> Result<u64> {
        if children.is_empty() {
            return Ok(0);
        }

        let mut tx = self.pool.begin().await?;

        sqlx::query(&format!(
            "CREATE TEMP TABLE IF NOT EXISTS _stg_factory_children \
             (LIKE {}.factory_children INCLUDING DEFAULTS) ON COMMIT DELETE ROWS",
            self.schema
        ))
        .execute(&mut *tx)
        .await?;

        {
            let mut copy = tx
                .as_mut()
                .copy_in_raw(
                    "COPY _stg_factory_children (\
                     chain_id, factory_address, child_address, contract_name, \
                     created_at_block, created_at_tx_hash, created_at_log_index, \
                     metadata, child_abi\
                     ) FROM STDIN",
                )
                .await?;

            const CHUNK_SIZE: usize = 5000;
            for chunk in children.chunks(CHUNK_SIZE) {
                let buf = encode_factory_copy_text(chunk);
                copy.send(&buf[..]).await?;
            }
            copy.finish().await?;
        }

        let result = sqlx::query(&format!(
            "INSERT INTO {}.factory_children SELECT * FROM _stg_factory_children ON CONFLICT DO NOTHING",
            self.schema
        ))
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        let count = result.rows_affected();
        trace!(count, children = children.len(), "Inserted factory children via COPY");
        Ok(count)
    }

    /// Get all child addresses for a chain, grouped by contract name
    pub async fn get_all_children(
        &self,
        chain_id: i32,
    ) -> Result<std::collections::HashMap<String, Vec<String>>> {
        let rows = sqlx::query_as::<_, ChildRow>(&format!(
            r#"
            SELECT contract_name, child_address
            FROM {}.factory_children
            WHERE chain_id = $1
            ORDER BY created_at_block
            "#,
            self.schema
        ))
        .bind(chain_id)
        .fetch_all(&self.pool)
        .await?;

        let mut map: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        for row in rows {
            map.entry(row.contract_name)
                .or_default()
                .push(row.child_address);
        }

        Ok(map)
    }

    /// Get all child addresses for a specific factory contract
    pub async fn get_children_by_factory(
        &self,
        chain_id: i32,
        factory_address: &str,
    ) -> Result<Vec<String>> {
        let rows = sqlx::query_scalar::<_, String>(&format!(
            r#"
            SELECT child_address
            FROM {}.factory_children
            WHERE chain_id = $1 AND factory_address = $2
            ORDER BY created_at_block
            "#,
            self.schema
        ))
        .bind(chain_id)
        .bind(factory_address.to_lowercase())
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    /// Delete factory children from a specific block onwards (for reorg handling)
    pub async fn delete_from_block(&self, chain_id: i32, from_block: i64) -> Result<u64> {
        let result = sqlx::query(&format!(
            r#"
            DELETE FROM {}.factory_children
            WHERE chain_id = $1 AND created_at_block >= $2
            "#,
            self.schema
        ))
        .bind(chain_id)
        .bind(from_block)
        .execute(&self.pool)
        .await?;

        trace!(
            chain_id,
            from_block,
            deleted = result.rows_affected(),
            "Deleted factory children for reorg"
        );
        Ok(result.rows_affected())
    }

    /// Count all children for a chain
    pub async fn count(&self, chain_id: i32) -> Result<i64> {
        let result = sqlx::query_scalar::<_, i64>(&format!(
            "SELECT COUNT(*) FROM {}.factory_children WHERE chain_id = $1",
            self.schema
        ))
        .bind(chain_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(result)
    }

    /// Get all children with their creation block (for reconciliation after parallel historic sync)
    pub async fn get_all_children_with_blocks(
        &self,
        chain_id: i32,
    ) -> Result<Vec<ChildWithBlock>> {
        let rows = sqlx::query_as::<_, ChildWithBlock>(&format!(
            r#"
            SELECT child_address, created_at_block
            FROM {}.factory_children
            WHERE chain_id = $1
            ORDER BY created_at_block
            "#,
            self.schema
        ))
        .bind(chain_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }
}

/// Encode factory children as PostgreSQL COPY text format (tab-separated, newline-delimited).
fn encode_factory_copy_text(children: &[FactoryChild]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(children.len() * 300);

    for child in children {
        write!(buf, "{}", child.chain_id).unwrap();
        buf.push(b'\t');
        write_copy_str(&mut buf, &child.factory_address);
        buf.push(b'\t');
        write_copy_str(&mut buf, &child.child_address);
        buf.push(b'\t');
        write_copy_str(&mut buf, &child.contract_name);
        buf.push(b'\t');
        write!(buf, "{}", child.created_at_block).unwrap();
        buf.push(b'\t');
        write_copy_str(&mut buf, &child.created_at_tx_hash);
        buf.push(b'\t');
        write!(buf, "{}", child.created_at_log_index).unwrap();
        buf.push(b'\t');
        write_copy_nullable_str(&mut buf, &child.metadata);
        buf.push(b'\t');
        write_copy_nullable_str(&mut buf, &child.child_abi);
        buf.push(b'\n');
    }

    buf
}

/// Write a string value, escaping COPY special characters (tab, newline, backslash).
#[inline]
fn write_copy_str(buf: &mut Vec<u8>, value: &str) {
    for byte in value.bytes() {
        match byte {
            b'\\' => buf.extend_from_slice(b"\\\\"),
            b'\t' => buf.extend_from_slice(b"\\t"),
            b'\n' => buf.extend_from_slice(b"\\n"),
            b'\r' => buf.extend_from_slice(b"\\r"),
            _ => buf.push(byte),
        }
    }
}

/// Write a nullable string, using \N for None.
#[inline]
fn write_copy_nullable_str(buf: &mut Vec<u8>, value: &Option<String>) {
    match value {
        Some(v) => write_copy_str(buf, v),
        None => buf.extend_from_slice(b"\\N"),
    }
}

#[derive(sqlx::FromRow)]
struct ChildRow {
    contract_name: String,
    child_address: String,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ChildWithBlock {
    pub child_address: String,
    pub created_at_block: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_child(chain_id: i32, addr: &str) -> FactoryChild {
        FactoryChild {
            chain_id,
            factory_address: "0xfactory".to_string(),
            child_address: addr.to_string(),
            contract_name: "Pair".to_string(),
            created_at_block: 100,
            created_at_tx_hash: "0xtx".to_string(),
            created_at_log_index: 0,
            metadata: None,
            child_abi: None,
        }
    }

    #[test]
    fn test_encode_factory_copy_text_basic() {
        let children = vec![sample_child(1, "0xchild1")];
        let buf = encode_factory_copy_text(&children);
        let text = String::from_utf8(buf).unwrap();
        let cols: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
        assert_eq!(cols.len(), 9);
        assert_eq!(cols[0], "1");
        assert_eq!(cols[1], "0xfactory");
        assert_eq!(cols[2], "0xchild1");
        assert_eq!(cols[3], "Pair");
        assert_eq!(cols[4], "100");
        assert_eq!(cols[7], "\\N"); // metadata
        assert_eq!(cols[8], "\\N"); // child_abi
    }

    #[test]
    fn test_encode_factory_copy_text_with_metadata() {
        let mut child = sample_child(1, "0xchild1");
        child.metadata = Some("{\"token0\": \"WETH\"}".to_string());
        let buf = encode_factory_copy_text(&[child]);
        let text = String::from_utf8(buf).unwrap();
        let cols: Vec<&str> = text.trim_end_matches('\n').split('\t').collect();
        assert_eq!(cols[7], "{\"token0\": \"WETH\"}");
        assert_eq!(cols[8], "\\N");
    }

    #[test]
    fn test_encode_factory_copy_text_escapes_special_chars() {
        let mut child = sample_child(1, "0xchild1");
        child.metadata = Some("has\ttab\nand\\backslash".to_string());
        let buf = encode_factory_copy_text(&[child]);
        let text = String::from_utf8(buf).unwrap();
        assert!(text.contains("has\\ttab\\nand\\\\backslash"));
    }

    #[test]
    fn test_encode_factory_copy_text_multiple() {
        let children = vec![
            sample_child(1, "0xchild1"),
            sample_child(1, "0xchild2"),
            sample_child(1, "0xchild3"),
        ];
        let buf = encode_factory_copy_text(&children);
        let text = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = text.trim_end_matches('\n').split('\n').collect();
        assert_eq!(lines.len(), 3);
    }

    #[test]
    fn test_encode_factory_copy_text_empty() {
        let buf = encode_factory_copy_text(&[]);
        assert!(buf.is_empty());
    }
}
