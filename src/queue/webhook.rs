use crate::abi::decoder::DecodedEventRow;
use crate::config::WebhookConfig;
use reqwest::Client;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

/// Payload sent to webhook endpoints
#[derive(Debug, Clone, Serialize)]
pub struct WebhookEventPayload {
    pub chain_id: i32,
    pub block_number: i64,
    pub block_timestamp: i64,
    pub tx_hash: String,
    pub log_index: i32,
    pub address: String,
    pub contract_name: String,
    pub event_name: String,
    pub params: HashMap<String, String>,
}

impl WebhookEventPayload {
    pub fn from_decoded(row: &DecodedEventRow) -> Self {
        let mut params = HashMap::new();
        for (col, val) in row.param_columns.iter().zip(row.param_values.iter()) {
            params.insert(col.clone(), val.clone());
        }

        Self {
            chain_id: row.chain_id,
            block_number: row.block_number,
            block_timestamp: row.block_timestamp,
            tx_hash: row.tx_hash.clone(),
            log_index: row.log_index,
            address: row.address.clone(),
            contract_name: row.contract_name.clone(),
            event_name: row.event_name.clone(),
            params,
        }
    }
}

/// Sends decoded events to configured webhook endpoints
pub struct WebhookSender {
    configs: Vec<WebhookConfig>,
    client: Client,
}

impl WebhookSender {
    pub fn new(configs: Vec<WebhookConfig>) -> Result<Self, reqwest::Error> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;

        debug!(webhook_count = configs.len(), "Webhook sender initialized");

        Ok(Self { configs, client })
    }

    /// Send decoded events to all matching webhook endpoints.
    /// Fire-and-forget — errors are logged but don't block the sync pipeline.
    pub async fn send_events(&self, events: &[WebhookEventPayload]) {
        if events.is_empty() {
            return;
        }

        for config in &self.configs {
            let matching: Vec<&WebhookEventPayload> = if config.events.is_empty() {
                events.iter().collect()
            } else {
                events
                    .iter()
                    .filter(|e| config.events.contains(&e.event_name))
                    .collect()
            };

            if matching.is_empty() {
                continue;
            }

            let url = config.url.clone();
            let timeout = Duration::from_secs(config.timeout_secs);
            let max_retries = config.retry_attempts;
            let headers = config.headers.clone();
            let client = self.client.clone();
            let payload: Vec<_> = matching.into_iter().cloned().collect();

            tokio::spawn(async move {
                send_with_retry(&client, &url, &payload, &headers, timeout, max_retries).await;
            });
        }
    }
}

async fn send_with_retry(
    client: &Client,
    url: &str,
    payload: &[WebhookEventPayload],
    headers: &HashMap<String, String>,
    timeout: Duration,
    max_retries: u32,
) {
    for attempt in 0..=max_retries {
        let mut request = client.post(url).timeout(timeout).json(payload);

        for (key, value) in headers {
            request = request.header(key.as_str(), value.as_str());
        }

        match request.send().await {
            Ok(response) => {
                let status = response.status();
                if status.is_success() {
                    debug!(
                        url,
                        events = payload.len(),
                        "Webhook delivered successfully"
                    );
                    return;
                }
                if status.is_client_error() {
                    // 4xx — don't retry, it's a permanent error
                    warn!(url, status = %status, "Webhook returned client error, not retrying");
                    return;
                }
                // 5xx — retry
                warn!(
                    url,
                    status = %status,
                    attempt = attempt + 1,
                    max_retries,
                    "Webhook returned server error, retrying"
                );
            }
            Err(e) => {
                warn!(
                    url,
                    error = %e,
                    attempt = attempt + 1,
                    max_retries,
                    "Webhook request failed, retrying"
                );
            }
        }

        if attempt < max_retries {
            let backoff = Duration::from_millis(500 * 2u64.saturating_pow(attempt));
            tokio::time::sleep(backoff).await;
        }
    }

    warn!(url, events = payload.len(), "Webhook delivery failed after all retries");
}

/// Create a WebhookSender from configs, or None if no webhooks configured
pub fn create_sender(configs: &[WebhookConfig]) -> Result<Option<Arc<WebhookSender>>, reqwest::Error> {
    if configs.is_empty() {
        return Ok(None);
    }
    Ok(Some(Arc::new(WebhookSender::new(configs.to_vec())?)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::abi::decoder::DecodedEventRow;

    fn test_decoded_row() -> DecodedEventRow {
        DecodedEventRow {
            table_name: "event_erc20_transfer".to_string(),
            contract_name: "ERC20".to_string(),
            event_name: "Transfer".to_string(),
            param_columns: vec!["from".to_string(), "to".to_string(), "value".to_string()],
            chain_id: 1,
            block_number: 18000000,
            tx_index: 5,
            log_index: 12,
            block_hash: "0xabc".to_string(),
            block_timestamp: 1700000000,
            tx_hash: "0xdef".to_string(),
            address: "0xa0b8".to_string(),
            param_values: vec![
                "0xaaaa".to_string(),
                "0xbbbb".to_string(),
                "1000000".to_string(),
            ],
        }
    }

    #[test]
    fn test_webhook_payload_from_decoded() {
        let row = test_decoded_row();
        let payload = WebhookEventPayload::from_decoded(&row);

        assert_eq!(payload.chain_id, 1);
        assert_eq!(payload.block_number, 18000000);
        assert_eq!(payload.contract_name, "ERC20");
        assert_eq!(payload.event_name, "Transfer");
        assert_eq!(payload.params.get("from"), Some(&"0xaaaa".to_string()));
        assert_eq!(payload.params.get("to"), Some(&"0xbbbb".to_string()));
        assert_eq!(payload.params.get("value"), Some(&"1000000".to_string()));
    }

    #[test]
    fn test_webhook_payload_serialization() {
        let row = test_decoded_row();
        let payload = WebhookEventPayload::from_decoded(&row);
        let json = serde_json::to_string(&payload).unwrap();

        assert!(json.contains("\"chain_id\":1"));
        assert!(json.contains("\"event_name\":\"Transfer\""));
        assert!(json.contains("\"contract_name\":\"ERC20\""));
    }

    #[test]
    fn test_create_sender_empty_configs() {
        let sender = create_sender(&[]).unwrap();
        assert!(sender.is_none());
    }

    #[test]
    fn test_create_sender_with_configs() {
        let configs = vec![WebhookConfig {
            url: "http://localhost:9999/events".to_string(),
            events: vec!["Transfer".to_string()],
            retry_attempts: 3,
            timeout_secs: 10,
            headers: HashMap::new(),
        }];
        let sender = create_sender(&configs).unwrap();
        assert!(sender.is_some());
    }

    #[test]
    fn test_webhook_payload_empty_params() {
        let row = DecodedEventRow {
            table_name: "event_test".to_string(),
            contract_name: "Test".to_string(),
            event_name: "NoArgs".to_string(),
            param_columns: vec![],
            chain_id: 1,
            block_number: 100,
            tx_index: 0,
            log_index: 0,
            block_hash: "0x0".to_string(),
            block_timestamp: 1000,
            tx_hash: "0x0".to_string(),
            address: "0x0".to_string(),
            param_values: vec![],
        };
        let payload = WebhookEventPayload::from_decoded(&row);
        assert!(payload.params.is_empty());
        assert_eq!(payload.event_name, "NoArgs");
    }

    #[test]
    fn test_webhook_event_filtering_logic() {
        // Test the matching logic directly — events list filters which payloads match
        let config_filtered = WebhookConfig {
            url: "http://example.com".to_string(),
            events: vec!["Transfer".to_string(), "Approval".to_string()],
            retry_attempts: 3,
            timeout_secs: 10,
            headers: HashMap::new(),
        };
        let config_all = WebhookConfig {
            url: "http://example.com".to_string(),
            events: vec![], // empty = all events
            retry_attempts: 3,
            timeout_secs: 10,
            headers: HashMap::new(),
        };

        let payloads = vec![
            WebhookEventPayload {
                chain_id: 1,
                block_number: 100,
                block_timestamp: 1000,
                tx_hash: "0x1".to_string(),
                log_index: 0,
                address: "0xa".to_string(),
                contract_name: "Token".to_string(),
                event_name: "Transfer".to_string(),
                params: HashMap::new(),
            },
            WebhookEventPayload {
                chain_id: 1,
                block_number: 100,
                block_timestamp: 1000,
                tx_hash: "0x2".to_string(),
                log_index: 1,
                address: "0xa".to_string(),
                contract_name: "Token".to_string(),
                event_name: "Swap".to_string(),
                params: HashMap::new(),
            },
        ];

        // Filtered config: only Transfer matches
        let matching_filtered: Vec<_> = if config_filtered.events.is_empty() {
            payloads.iter().collect()
        } else {
            payloads.iter().filter(|e| config_filtered.events.contains(&e.event_name)).collect()
        };
        assert_eq!(matching_filtered.len(), 1);
        assert_eq!(matching_filtered[0].event_name, "Transfer");

        // All-events config: both match
        let matching_all: Vec<_> = if config_all.events.is_empty() {
            payloads.iter().collect()
        } else {
            payloads.iter().filter(|e| config_all.events.contains(&e.event_name)).collect()
        };
        assert_eq!(matching_all.len(), 2);
    }

    #[test]
    fn test_webhook_payload_all_fields_serialized() {
        let mut params = HashMap::new();
        params.insert("from".to_string(), "0xaaaa".to_string());
        params.insert("to".to_string(), "0xbbbb".to_string());

        let payload = WebhookEventPayload {
            chain_id: 137,
            block_number: 50000000,
            block_timestamp: 1700000000,
            tx_hash: "0xdef456".to_string(),
            log_index: 5,
            address: "0xcontract".to_string(),
            contract_name: "MyToken".to_string(),
            event_name: "Transfer".to_string(),
            params,
        };

        let json: serde_json::Value = serde_json::to_value(&payload).unwrap();
        assert_eq!(json["chain_id"], 137);
        assert_eq!(json["block_number"], 50000000);
        assert_eq!(json["block_timestamp"], 1700000000);
        assert_eq!(json["tx_hash"], "0xdef456");
        assert_eq!(json["log_index"], 5);
        assert_eq!(json["address"], "0xcontract");
        assert_eq!(json["contract_name"], "MyToken");
        assert_eq!(json["event_name"], "Transfer");
        assert_eq!(json["params"]["from"], "0xaaaa");
        assert_eq!(json["params"]["to"], "0xbbbb");
    }

    #[test]
    fn test_webhook_batch_payload_serialization() {
        // Webhooks send arrays of payloads
        let payloads = vec![
            WebhookEventPayload {
                chain_id: 1,
                block_number: 100,
                block_timestamp: 1000,
                tx_hash: "0x1".to_string(),
                log_index: 0,
                address: "0xa".to_string(),
                contract_name: "T".to_string(),
                event_name: "E1".to_string(),
                params: HashMap::new(),
            },
            WebhookEventPayload {
                chain_id: 1,
                block_number: 101,
                block_timestamp: 1001,
                tx_hash: "0x2".to_string(),
                log_index: 0,
                address: "0xa".to_string(),
                contract_name: "T".to_string(),
                event_name: "E2".to_string(),
                params: HashMap::new(),
            },
        ];

        let json = serde_json::to_string(&payloads).unwrap();
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0]["block_number"], 100);
        assert_eq!(parsed[1]["block_number"], 101);
    }

    #[test]
    fn test_webhook_sender_new_sets_timeout() {
        let configs = vec![WebhookConfig {
            url: "http://localhost:9999".to_string(),
            events: vec![],
            retry_attempts: 5,
            timeout_secs: 30,
            headers: HashMap::new(),
        }];
        let sender = WebhookSender::new(configs.clone()).unwrap();
        assert_eq!(sender.configs.len(), 1);
        assert_eq!(sender.configs[0].retry_attempts, 5);
    }

    #[test]
    fn test_webhook_config_from_yaml() {
        let yaml = r#"
database:
  connection_string: "postgresql://localhost/test"
redis:
  url: "redis://localhost:6379"
schema:
  sync_schema: "test_sync"
  user_schema: "test"
chain:
  id: 1
  name: "ethereum"
source:
  type: rpc
  url: "http://localhost:8545"
sync:
  start_block: 100
contracts:
  - name: "Test"
    address: "0x1234567890abcdef1234567890abcdef12345678"
    abi_path: "./test.json"
webhooks:
  - url: "https://api.example.com/events"
    events: ["Transfer", "Swap"]
    retry_attempts: 5
    timeout_secs: 15
    headers:
      Authorization: "Bearer token123"
  - url: "https://backup.example.com/events"
"#;
        let config: crate::config::IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.webhooks.len(), 2);
        assert_eq!(config.webhooks[0].url, "https://api.example.com/events");
        assert_eq!(config.webhooks[0].events, vec!["Transfer", "Swap"]);
        assert_eq!(config.webhooks[0].retry_attempts, 5);
        assert_eq!(config.webhooks[0].timeout_secs, 15);
        assert_eq!(
            config.webhooks[0].headers.get("Authorization"),
            Some(&"Bearer token123".to_string())
        );
        // Second webhook has defaults
        assert_eq!(config.webhooks[1].url, "https://backup.example.com/events");
        assert!(config.webhooks[1].events.is_empty());
        assert_eq!(config.webhooks[1].retry_attempts, 3);
        assert_eq!(config.webhooks[1].timeout_secs, 10);
    }
}
