use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Json, Router};
use serde::Serialize;

use super::AppState;

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    database: &'static str,
    redis: &'static str, // "connected", "disconnected", or "not_configured"
}

#[derive(Serialize)]
struct ReadinessResponse {
    status: &'static str,
    mode: String,
}

/// Liveness probe — checks that the process is running and connections are healthy.
/// Returns 200 if DB (and Redis, when configured) are reachable, 503 otherwise.
async fn health(State(state): State<AppState>) -> impl IntoResponse {
    // Check database connectivity
    let db_ok = sqlx::query("SELECT 1")
        .execute(&state.pool)
        .await
        .is_ok();

    // Check Redis connectivity (skip if not configured)
    let redis_status = match &state.redis {
        Some(client) => {
            match client.get_multiplexed_async_connection().await {
                Ok(mut conn) => {
                    if redis::cmd("PING")
                        .query_async::<String>(&mut conn)
                        .await
                        .is_ok()
                    {
                        "connected"
                    } else {
                        "disconnected"
                    }
                }
                Err(_) => "disconnected",
            }
        }
        None => "not_configured",
    };

    let redis_ok = redis_status != "disconnected";
    let all_ok = db_ok && redis_ok;

    let response = HealthResponse {
        status: if all_ok { "ok" } else { "degraded" },
        database: if db_ok { "connected" } else { "disconnected" },
        redis: redis_status,
    };

    let status_code = if all_ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (status_code, Json(response))
}

/// Readiness probe — returns 200 only when all chains are in live sync mode.
/// Returns 503 during historic sync or initialization.
async fn readiness(State(state): State<AppState>) -> impl IntoResponse {
    let mut all_live = true;
    let mut any_live = false;

    for chain in &state.chains {
        let chain_id = chain.chain_id as i32;
        let schema = &chain.sync_schema;

        let is_live = sqlx::query_scalar::<_, bool>(&format!(
            "SELECT EXISTS(SELECT 1 FROM {}.sync_workers WHERE chain_id = $1 AND status = 'live')",
            schema
        ))
        .bind(chain_id)
        .fetch_one(&state.pool)
        .await
        .unwrap_or(false);

        if is_live {
            any_live = true;
        } else {
            all_live = false;
        }
    }

    if all_live && !state.chains.is_empty() {
        (
            StatusCode::OK,
            Json(ReadinessResponse {
                status: "ready",
                mode: "live".to_string(),
            }),
        )
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ReadinessResponse {
                status: "syncing",
                mode: if any_live { "partial".to_string() } else { "historical".to_string() },
            }),
        )
    }
}

pub fn routes(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/readiness", get(readiness))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_response_ok() {
        let response = HealthResponse {
            status: "ok",
            database: "connected",
            redis: "connected",
        };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"status\":\"ok\""));
        assert!(json.contains("\"database\":\"connected\""));
        assert!(json.contains("\"redis\":\"connected\""));
    }

    #[test]
    fn test_health_response_degraded() {
        let response = HealthResponse {
            status: "degraded",
            database: "connected",
            redis: "disconnected",
        };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"status\":\"degraded\""));
        assert!(json.contains("\"redis\":\"disconnected\""));
    }

    #[test]
    fn test_readiness_response_ready() {
        let response = ReadinessResponse {
            status: "ready",
            mode: "live".to_string(),
        };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"status\":\"ready\""));
        assert!(json.contains("\"mode\":\"live\""));
    }

    #[test]
    fn test_readiness_response_syncing() {
        let response = ReadinessResponse {
            status: "syncing",
            mode: "historical".to_string(),
        };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"status\":\"syncing\""));
        assert!(json.contains("\"mode\":\"historical\""));
    }
}
