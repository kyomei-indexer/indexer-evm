use axum::{extract::{Query, State}, routing::get, Json, Router};
use serde::{Deserialize, Serialize};

use super::{AppState, ChainState};

#[derive(Deserialize)]
pub struct SyncQuery {
    pub chain_id: Option<u32>,
}

#[derive(Serialize)]
pub struct SyncResponse {
    /// Whether the indexer has caught up to the chain tip
    pub synced: bool,
    /// Last indexed block number
    pub current_block: i64,
    /// Current chain tip from the RPC/source
    pub chain_tip: i64,
    /// Sync progress (0-100)
    pub percentage: f64,
    /// Current sync mode: "historical" or "live"
    pub mode: String,
    /// Data source type: "rpc", "erpc", or "hypersync"
    pub source: String,
    /// Chain ID being indexed
    pub chain_id: u32,
    /// Chain name
    pub chain_name: String,
    /// Number of active historic workers (0 when in live mode)
    pub historic_workers: usize,
}

/// GET /sync — returns sync status for all chains or a specific chain via ?chain_id=
async fn sync_status(
    State(state): State<AppState>,
    Query(query): Query<SyncQuery>,
) -> Json<Vec<SyncResponse>> {
    let chains: Vec<&ChainState> = match query.chain_id {
        Some(id) => state.chains.iter().filter(|c| c.chain_id == id).collect(),
        None => state.chains.iter().collect(),
    };

    let mut responses = Vec::with_capacity(chains.len());

    for chain in chains {
        let response = get_chain_sync_status(&state.pool, chain).await;
        responses.push(response);
    }

    Json(responses)
}

async fn get_chain_sync_status(pool: &sqlx::PgPool, chain: &ChainState) -> SyncResponse {
    let chain_id = chain.chain_id as i32;
    let schema = &chain.sync_schema;

    // Query workers from DB
    let workers: Vec<WorkerRow> = sqlx::query_as::<_, WorkerRow>(&format!(
        r#"
        SELECT worker_id, range_start, range_end, current_block, status
        FROM {}.sync_workers
        WHERE chain_id = $1
        ORDER BY worker_id
        "#,
        schema
    ))
    .bind(chain_id)
    .fetch_all(pool)
    .await
    .unwrap_or_default();

    // Determine mode and compute progress
    let live_worker = workers.iter().find(|w| w.status == "live");
    let historic_workers: Vec<&WorkerRow> = workers.iter().filter(|w| w.status == "historical").collect();

    let (mode, current_block) = if let Some(lw) = live_worker {
        ("live".to_string(), lw.current_block)
    } else if !historic_workers.is_empty() {
        let min_block = historic_workers.iter().map(|w| w.current_block).min().unwrap_or(0);
        ("historical".to_string(), min_block)
    } else {
        ("initializing".to_string(), 0)
    };

    let chain_tip = get_chain_tip(pool, schema, &chain.data_schema, chain_id).await;

    let start_block = chain.start_block as i64;
    let percentage = if chain_tip > start_block && chain_tip > 0 {
        let progress = (current_block - start_block) as f64 / (chain_tip - start_block) as f64 * 100.0;
        progress.clamp(0.0, 100.0)
    } else {
        0.0
    };

    let synced = mode == "live" && (chain_tip - current_block) <= 1;

    SyncResponse {
        synced,
        current_block,
        chain_tip,
        percentage,
        mode,
        source: chain.source_type.clone(),
        chain_id: chain.chain_id,
        chain_name: chain.chain_name.clone(),
        historic_workers: historic_workers.len(),
    }
}

/// Get the chain tip — use the latest block from raw_events as an approximation
async fn get_chain_tip(pool: &sqlx::PgPool, sync_schema: &str, data_schema: &str, chain_id: i32) -> i64 {
    let result = sqlx::query_scalar::<_, Option<i64>>(&format!(
        "SELECT MAX(current_block) FROM {}.sync_workers WHERE chain_id = $1",
        sync_schema
    ))
    .bind(chain_id)
    .fetch_one(pool)
    .await
    .ok()
    .flatten();

    if let Some(tip) = result {
        let raw_max = sqlx::query_scalar::<_, Option<i64>>(&format!(
            "SELECT MAX(block_number) FROM {}.raw_events WHERE chain_id = $1",
            data_schema
        ))
        .bind(chain_id)
        .fetch_one(pool)
        .await
        .ok()
        .flatten()
        .unwrap_or(0);

        tip.max(raw_max)
    } else {
        0
    }
}

pub fn routes(state: AppState) -> Router {
    Router::new()
        .route("/sync", get(sync_status))
        .with_state(state)
}

#[derive(sqlx::FromRow, Default)]
struct WorkerRow {
    #[allow(dead_code)]
    worker_id: i32,
    #[allow(dead_code)]
    range_start: i64,
    #[allow(dead_code)]
    range_end: Option<i64>,
    current_block: i64,
    status: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_response_serialization() {
        let response = SyncResponse {
            synced: false,
            current_block: 15000000,
            chain_tip: 18000000,
            percentage: 83.33,
            mode: "historical".to_string(),
            source: "rpc".to_string(),
            chain_id: 1,
            chain_name: "ethereum".to_string(),
            historic_workers: 4,
        };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"synced\":false"));
        assert!(json.contains("\"current_block\":15000000"));
        assert!(json.contains("\"chain_tip\":18000000"));
        assert!(json.contains("\"mode\":\"historical\""));
        assert!(json.contains("\"source\":\"rpc\""));
        assert!(json.contains("\"chain_id\":1"));
        assert!(json.contains("\"chain_name\":\"ethereum\""));
        assert!(json.contains("\"historic_workers\":4"));
    }

    #[test]
    fn test_sync_response_live_mode() {
        let response = SyncResponse {
            synced: true,
            current_block: 18000000,
            chain_tip: 18000000,
            percentage: 100.0,
            mode: "live".to_string(),
            source: "hypersync".to_string(),
            chain_id: 1,
            chain_name: "ethereum".to_string(),
            historic_workers: 0,
        };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"synced\":true"));
        assert!(json.contains("\"mode\":\"live\""));
        assert!(json.contains("\"source\":\"hypersync\""));
        assert!(json.contains("\"historic_workers\":0"));
    }

    #[test]
    fn test_sync_response_initializing() {
        let response = SyncResponse {
            synced: false,
            current_block: 0,
            chain_tip: 0,
            percentage: 0.0,
            mode: "initializing".to_string(),
            source: "erpc".to_string(),
            chain_id: 137,
            chain_name: "polygon".to_string(),
            historic_workers: 0,
        };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"mode\":\"initializing\""));
        assert!(json.contains("\"chain_id\":137"));
    }

    #[test]
    fn test_multi_chain_response_serialization() {
        let responses = vec![
            SyncResponse {
                synced: true,
                current_block: 18000000,
                chain_tip: 18000000,
                percentage: 100.0,
                mode: "live".to_string(),
                source: "rpc".to_string(),
                chain_id: 1,
                chain_name: "ethereum".to_string(),
                historic_workers: 0,
            },
            SyncResponse {
                synced: false,
                current_block: 50000000,
                chain_tip: 60000000,
                percentage: 83.33,
                mode: "historical".to_string(),
                source: "hypersync".to_string(),
                chain_id: 137,
                chain_name: "polygon".to_string(),
                historic_workers: 4,
            },
        ];
        let json = serde_json::to_string(&responses).unwrap();
        assert!(json.starts_with('['));
        assert!(json.contains("\"chain_id\":1"));
        assert!(json.contains("\"chain_id\":137"));
    }
}
