use crate::types::{SyncWorker, SyncWorkerStatus};
use anyhow::Result;
use sqlx::{PgPool, Postgres, Transaction};
use tracing::trace;

/// Repository for sync worker state
pub struct WorkerRepository {
    pool: PgPool,
    schema: String,
}

impl WorkerRepository {
    pub fn new(pool: PgPool, schema: String) -> Self {
        Self { pool, schema }
    }

    /// Get all workers for a chain
    pub async fn get_workers(&self, chain_id: i32) -> Result<Vec<SyncWorker>> {
        let rows = sqlx::query_as::<_, WorkerRow>(&format!(
            r#"
            SELECT chain_id, worker_id, range_start, range_end, current_block, status
            FROM {}.sync_workers
            WHERE chain_id = $1
            ORDER BY worker_id
            "#,
            self.schema
        ))
        .bind(chain_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|r| r.try_into())
            .collect::<Result<Vec<_>>>()
    }

    /// Get the live worker (worker_id = 0) for a chain
    pub async fn get_live_worker(&self, chain_id: i32) -> Result<Option<SyncWorker>> {
        let row = sqlx::query_as::<_, WorkerRow>(&format!(
            r#"
            SELECT chain_id, worker_id, range_start, range_end, current_block, status
            FROM {}.sync_workers
            WHERE chain_id = $1 AND worker_id = 0
            "#,
            self.schema
        ))
        .bind(chain_id)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(r) => Ok(Some(r.try_into()?)),
            None => Ok(None),
        }
    }

    /// Upsert a worker's state
    pub async fn set_worker(&self, worker: &SyncWorker) -> Result<()> {
        sqlx::query(&format!(
            r#"
            INSERT INTO {}.sync_workers (chain_id, worker_id, range_start, range_end, current_block, status, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, NOW())
            ON CONFLICT (chain_id, worker_id) DO UPDATE SET
                range_start = EXCLUDED.range_start,
                range_end = EXCLUDED.range_end,
                current_block = EXCLUDED.current_block,
                status = EXCLUDED.status,
                updated_at = NOW()
            "#,
            self.schema
        ))
        .bind(worker.chain_id)
        .bind(worker.worker_id)
        .bind(worker.range_start)
        .bind(worker.range_end)
        .bind(worker.current_block)
        .bind(worker.status.to_string())
        .execute(&self.pool)
        .await?;

        trace!(
            chain_id = worker.chain_id,
            worker_id = worker.worker_id,
            current_block = worker.current_block,
            "Updated worker state"
        );
        Ok(())
    }

    /// Upsert a worker's state within an existing transaction.
    /// Used by live sync to ensure atomicity across raw events, decoded events, and worker checkpoint.
    pub async fn set_worker_in_tx(
        &self,
        worker: &SyncWorker,
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<()> {
        sqlx::query(&format!(
            r#"
            INSERT INTO {}.sync_workers (chain_id, worker_id, range_start, range_end, current_block, status, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, NOW())
            ON CONFLICT (chain_id, worker_id) DO UPDATE SET
                range_start = EXCLUDED.range_start,
                range_end = EXCLUDED.range_end,
                current_block = EXCLUDED.current_block,
                status = EXCLUDED.status,
                updated_at = NOW()
            "#,
            self.schema
        ))
        .bind(worker.chain_id)
        .bind(worker.worker_id)
        .bind(worker.range_start)
        .bind(worker.range_end)
        .bind(worker.current_block)
        .bind(worker.status.to_string())
        .execute(&mut **tx)
        .await?;

        trace!(
            chain_id = worker.chain_id,
            worker_id = worker.worker_id,
            current_block = worker.current_block,
            "Updated worker state (in tx)"
        );
        Ok(())
    }

    /// Delete a specific worker
    pub async fn delete_worker(&self, chain_id: i32, worker_id: i32) -> Result<()> {
        sqlx::query(&format!(
            "DELETE FROM {}.sync_workers WHERE chain_id = $1 AND worker_id = $2",
            self.schema
        ))
        .bind(chain_id)
        .bind(worker_id)
        .execute(&self.pool)
        .await?;

        trace!(chain_id, worker_id, "Deleted worker");
        Ok(())
    }

    /// Delete all workers for a chain
    pub async fn delete_all_workers(&self, chain_id: i32) -> Result<()> {
        sqlx::query(&format!(
            "DELETE FROM {}.sync_workers WHERE chain_id = $1",
            self.schema
        ))
        .bind(chain_id)
        .execute(&self.pool)
        .await?;

        trace!(chain_id, "Deleted all workers");
        Ok(())
    }
}

#[derive(sqlx::FromRow)]
struct WorkerRow {
    chain_id: i32,
    worker_id: i32,
    range_start: i64,
    range_end: Option<i64>,
    current_block: i64,
    status: String,
}

impl TryFrom<WorkerRow> for SyncWorker {
    type Error = anyhow::Error;

    fn try_from(row: WorkerRow) -> Result<Self> {
        Ok(Self {
            chain_id: row.chain_id,
            worker_id: row.worker_id,
            range_start: row.range_start,
            range_end: row.range_end,
            current_block: row.current_block,
            status: SyncWorkerStatus::try_from(row.status.as_str())?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_row_to_sync_worker_historical() {
        let row = WorkerRow {
            chain_id: 1,
            worker_id: 3,
            range_start: 10000000,
            range_end: Some(15000000),
            current_block: 12500000,
            status: "historical".to_string(),
        };
        let worker: SyncWorker = row.try_into().unwrap();
        assert_eq!(worker.chain_id, 1);
        assert_eq!(worker.worker_id, 3);
        assert_eq!(worker.range_start, 10000000);
        assert_eq!(worker.range_end, Some(15000000));
        assert_eq!(worker.current_block, 12500000);
        assert_eq!(worker.status, SyncWorkerStatus::Historical);
    }

    #[test]
    fn test_worker_row_to_sync_worker_live() {
        let row = WorkerRow {
            chain_id: 137,
            worker_id: 0,
            range_start: 50000000,
            range_end: None,
            current_block: 55000000,
            status: "live".to_string(),
        };
        let worker: SyncWorker = row.try_into().unwrap();
        assert_eq!(worker.status, SyncWorkerStatus::Live);
        assert!(worker.range_end.is_none());
    }

    #[test]
    fn test_worker_row_to_sync_worker_invalid_status() {
        let row = WorkerRow {
            chain_id: 1,
            worker_id: 0,
            range_start: 0,
            range_end: None,
            current_block: 0,
            status: "invalid_status".to_string(),
        };
        let result: Result<SyncWorker> = row.try_into();
        assert!(result.is_err());
    }

    #[test]
    fn test_worker_row_to_sync_worker_empty_status() {
        let row = WorkerRow {
            chain_id: 1,
            worker_id: 0,
            range_start: 0,
            range_end: None,
            current_block: 0,
            status: "".to_string(),
        };
        let result: Result<SyncWorker> = row.try_into();
        assert!(result.is_err());
    }

    // ========================================================================
    // Deep worker state edge case tests
    // ========================================================================

    #[test]
    fn test_worker_row_live_worker_id_0() {
        // Live worker should always have worker_id = 0
        let row = WorkerRow {
            chain_id: 1,
            worker_id: 0,
            range_start: 18_000_000,
            range_end: None,
            current_block: 18_500_000,
            status: "live".to_string(),
        };
        let worker: SyncWorker = row.try_into().unwrap();
        assert_eq!(worker.worker_id, 0);
        assert_eq!(worker.status, SyncWorkerStatus::Live);
        assert!(worker.range_end.is_none());
    }

    #[test]
    fn test_worker_row_historical_with_completed_range() {
        // Historical worker where current_block equals range_end (completed)
        let row = WorkerRow {
            chain_id: 1,
            worker_id: 3,
            range_start: 10_000_000,
            range_end: Some(15_000_000),
            current_block: 15_000_000,
            status: "historical".to_string(),
        };
        let worker: SyncWorker = row.try_into().unwrap();
        assert_eq!(worker.current_block, worker.range_end.unwrap());
    }

    #[test]
    fn test_worker_row_current_block_before_range_start() {
        // Edge case: current_block < range_start (shouldn't happen, but shouldn't panic)
        let row = WorkerRow {
            chain_id: 1,
            worker_id: 1,
            range_start: 100,
            range_end: Some(200),
            current_block: 50,
            status: "historical".to_string(),
        };
        let worker: SyncWorker = row.try_into().unwrap();
        assert!(worker.current_block < worker.range_start);
    }

    #[test]
    fn test_worker_row_current_block_beyond_range_end() {
        // Edge case: current_block > range_end (shouldn't happen normally)
        let row = WorkerRow {
            chain_id: 1,
            worker_id: 1,
            range_start: 100,
            range_end: Some(200),
            current_block: 300,
            status: "historical".to_string(),
        };
        let worker: SyncWorker = row.try_into().unwrap();
        assert!(worker.current_block > worker.range_end.unwrap());
    }

    #[test]
    fn test_worker_row_max_block_numbers() {
        let row = WorkerRow {
            chain_id: i32::MAX,
            worker_id: i32::MAX,
            range_start: i64::MAX,
            range_end: Some(i64::MAX),
            current_block: i64::MAX,
            status: "historical".to_string(),
        };
        let worker: SyncWorker = row.try_into().unwrap();
        assert_eq!(worker.chain_id, i32::MAX);
        assert_eq!(worker.range_start, i64::MAX);
    }

    #[test]
    fn test_worker_row_zero_block_range() {
        // range_start == range_end (single block range)
        let row = WorkerRow {
            chain_id: 1,
            worker_id: 1,
            range_start: 100,
            range_end: Some(100),
            current_block: 100,
            status: "historical".to_string(),
        };
        let worker: SyncWorker = row.try_into().unwrap();
        assert_eq!(worker.range_start, worker.range_end.unwrap());
    }

    #[test]
    fn test_worker_row_status_case_sensitivity() {
        // Various wrong casings
        for bad_status in [
            "Historical",
            "HISTORICAL",
            "Live",
            "LIVE",
            "hISTORICAL",
            "lIVE",
        ] {
            let row = WorkerRow {
                chain_id: 1,
                worker_id: 0,
                range_start: 0,
                range_end: None,
                current_block: 0,
                status: bad_status.to_string(),
            };
            let result: Result<SyncWorker> = row.try_into();
            assert!(
                result.is_err(),
                "Status '{}' should be rejected",
                bad_status
            );
        }
    }

    #[test]
    fn test_worker_row_status_with_whitespace() {
        for bad_status in [" historical", "historical ", " live ", "live\n"] {
            let row = WorkerRow {
                chain_id: 1,
                worker_id: 0,
                range_start: 0,
                range_end: None,
                current_block: 0,
                status: bad_status.to_string(),
            };
            let result: Result<SyncWorker> = row.try_into();
            assert!(
                result.is_err(),
                "Status '{}' with whitespace should be rejected",
                bad_status
            );
        }
    }

    #[test]
    fn test_worker_row_negative_block_numbers() {
        // PostgreSQL could return negative values in edge cases
        let row = WorkerRow {
            chain_id: 1,
            worker_id: 1,
            range_start: -1,
            range_end: Some(-1),
            current_block: -1,
            status: "historical".to_string(),
        };
        let worker: SyncWorker = row.try_into().unwrap();
        assert_eq!(worker.range_start, -1);
    }

    #[test]
    fn test_worker_row_chain_id_0() {
        let row = WorkerRow {
            chain_id: 0,
            worker_id: 0,
            range_start: 0,
            range_end: None,
            current_block: 0,
            status: "live".to_string(),
        };
        let worker: SyncWorker = row.try_into().unwrap();
        assert_eq!(worker.chain_id, 0);
    }

    #[test]
    fn test_worker_row_negative_chain_id() {
        let row = WorkerRow {
            chain_id: -1,
            worker_id: -1,
            range_start: 0,
            range_end: None,
            current_block: 0,
            status: "live".to_string(),
        };
        let worker: SyncWorker = row.try_into().unwrap();
        assert_eq!(worker.chain_id, -1);
    }
}
