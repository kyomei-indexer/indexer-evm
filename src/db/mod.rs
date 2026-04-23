pub mod accounts;
pub mod decoded;
pub mod events;
pub mod factory;
pub mod migrations;
pub mod traces;
pub mod views;
pub mod workers;

pub use views::ViewCreator;

use crate::config::DatabaseConfig;
use anyhow::{Context, Result};
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// Create a PostgreSQL connection pool with retry logic
pub async fn create_pool(config: &DatabaseConfig) -> Result<PgPool> {
    // Mask password in connection string for logging
    let safe_url = mask_connection_string(&config.connection_string);
    info!(
        connection = %safe_url,
        pool_size = config.pool_size,
        "Connecting to database"
    );

    let pool = PgPoolOptions::new()
        .max_connections(config.pool_size)
        .acquire_timeout(Duration::from_secs(10))
        .idle_timeout(Duration::from_secs(300))
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                // Suppress PostgreSQL NOTICE messages (e.g. "relation already exists, skipping")
                sqlx::query("SET client_min_messages TO WARNING")
                    .execute(&mut *conn)
                    .await?;
                Ok(())
            })
        })
        .connect_with_retry(&config.connection_string, &safe_url)
        .await?;

    debug!(pool_size = config.pool_size, "Database pool created");
    Ok(pool)
}

/// Mask the password in a PostgreSQL connection string for safe logging
fn mask_connection_string(url: &str) -> String {
    // postgresql://user:password@host:port/db -> postgresql://user:***@host:port/db
    if let Some(at_pos) = url.find('@') {
        if let Some(colon_pos) = url[..at_pos].rfind(':') {
            let prefix = &url[..colon_pos + 1];
            let suffix = &url[at_pos..];
            return format!("{}***{}", prefix, suffix);
        }
    }
    url.to_string()
}

trait PgPoolOptionsExt {
    async fn connect_with_retry(self, url: &str, safe_url: &str) -> Result<PgPool>;
}

impl PgPoolOptionsExt for PgPoolOptions {
    async fn connect_with_retry(self, url: &str, safe_url: &str) -> Result<PgPool> {
        let max_retries = 5u32;
        let mut attempt = 0;

        loop {
            attempt += 1;
            match self.clone().connect(url).await {
                Ok(pool) => {
                    // Verify the connection is actually usable
                    sqlx::query("SELECT 1")
                        .execute(&pool)
                        .await
                        .context("Database connection established but health check query failed")?;
                    return Ok(pool);
                }
                Err(e) => {
                    if attempt >= max_retries {
                        error!(
                            attempt,
                            max_retries,
                            connection = %safe_url,
                            error = %e,
                            "Failed to connect to database after all retries"
                        );
                        return Err(e).with_context(|| {
                            format!(
                                "Failed to connect to database at {} after {} attempts. \
                                 Ensure the database is running and the connection string is correct.",
                                safe_url, max_retries
                            )
                        });
                    }

                    let backoff = Duration::from_secs(2u64.saturating_pow(attempt - 1));
                    warn!(
                        attempt,
                        max_retries,
                        backoff_secs = backoff.as_secs(),
                        connection = %safe_url,
                        error = %e,
                        "Failed to connect to database, retrying"
                    );
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }
}
