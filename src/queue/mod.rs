pub mod publisher;
pub mod webhook;

use crate::config::RedisConfig;
use anyhow::{Context, Result};

/// Create a Redis client from config and verify connectivity
pub fn create_client(config: &RedisConfig) -> Result<redis::Client> {
    let client = redis::Client::open(config.url.as_str())
        .with_context(|| format!("Invalid Redis URL: {}", config.url))?;

    // Verify connectivity synchronously via a quick ping
    let mut conn = client
        .get_connection()
        .with_context(|| {
            format!(
                "Failed to connect to Redis at {}. Ensure Redis is running and accessible.",
                config.url
            )
        })?;
    redis::cmd("PING")
        .query::<String>(&mut conn)
        .with_context(|| format!("Redis PING failed at {}", config.url))?;

    tracing::info!(url = %config.url, "Redis client connected");
    Ok(client)
}
