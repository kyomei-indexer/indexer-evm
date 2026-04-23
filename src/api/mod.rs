pub mod health;
pub mod sync;

use crate::config::IndexerConfig;
use anyhow::Result;
use axum::{response::IntoResponse, Router};
use metrics_exporter_prometheus::PrometheusHandle;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::info;

/// Per-chain state stored in the multi-chain app state
#[derive(Clone)]
pub struct ChainState {
    pub chain_id: u32,
    pub chain_name: String,
    pub source_type: String,
    pub data_schema: String,
    pub sync_schema: String,
    pub start_block: u64,
}

/// Shared application state for API handlers (supports multiple chains)
#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub redis: Option<redis::Client>,
    pub chains: Vec<ChainState>,
}

/// Start the HTTP API server on the configured host:port
pub async fn start_server(
    configs: &[IndexerConfig],
    pool: PgPool,
    redis: Option<redis::Client>,
    metrics_handle: PrometheusHandle,
) -> Result<JoinHandle<()>> {
    let metrics_handle = Arc::new(metrics_handle);

    let chains: Vec<ChainState> = configs
        .iter()
        .map(|config| ChainState {
            chain_id: config.chain.id,
            chain_name: config.chain.name.clone(),
            source_type: config.source.source_type().to_string(),
            data_schema: config.schema.data_schema.clone(),
            sync_schema: config.schema.sync_schema.clone(),
            start_block: config.sync.start_block,
        })
        .collect();

    let state = AppState {
        pool: pool.clone(),
        redis,
        chains,
    };

    let app = Router::new()
        .merge(health::routes(state.clone()))
        .merge(sync::routes(state))
        .route("/metrics", axum::routing::get({
            let metrics_handle = Arc::clone(&metrics_handle);
            move || {
                let handle = Arc::clone(&metrics_handle);
                async move { handle.render().into_response() }
            }
        }));

    let api_config = &configs[0];
    let addr = format!("{}:{}", api_config.api.host, api_config.api.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!(addr = %addr, "API server listening");

    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });

    Ok(handle)
}
