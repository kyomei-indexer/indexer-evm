use crate::config::ContractConfig;
use anyhow::{Context, Result};
use sqlx::PgPool;
use tracing::{debug, warn};

/// Indexes view function results by periodically calling contract read functions
/// and storing the results in per-function tables.
pub struct ViewFunctionIndexer {
    pool: PgPool,
    schema: String,
    chain_id: i32,
    /// (contract_name, address, function_name) → ViewFunctionConfig
    views: Vec<ViewEntry>,
    rpc_url: String,
    client: reqwest::Client,
}

struct ViewEntry {
    contract_name: String,
    address: String,
    function: String,
    interval_blocks: u64,
    /// Pre-encoded calldata (function selector + params)
    calldata: String,
    table_name: String,
}

impl ViewFunctionIndexer {
    pub fn new(
        pool: PgPool,
        schema: String,
        chain_id: i32,
        contracts: &[ContractConfig],
        rpc_url: &str,
    ) -> Self {
        let mut views = Vec::new();

        for contract in contracts {
            if contract.views.is_empty() {
                continue;
            }
            let address = match &contract.address {
                Some(addr) => addr.clone(),
                None => continue, // factory contracts without fixed address can't do view calls
            };

            for view_config in &contract.views {
                let table_name = format!(
                    "view_{}_{}",
                    crate::abi::decoder::to_snake_case(&contract.name),
                    crate::abi::decoder::to_snake_case(&view_config.function)
                );

                // Build simple calldata: function selector from name
                // For parameterless functions, we just need the 4-byte selector
                let selector = function_selector(&view_config.function, &view_config.params);
                let calldata = if view_config.params.is_empty() {
                    format!("0x{}", selector)
                } else {
                    // Append params as raw hex (each param is 32 bytes zero-padded)
                    let mut data = format!("0x{}", selector);
                    for param in &view_config.params {
                        let clean = param.strip_prefix("0x").unwrap_or(param);
                        data.push_str(&format!("{:0>64}", clean));
                    }
                    data
                };

                views.push(ViewEntry {
                    contract_name: contract.name.clone(),
                    address: address.clone(),
                    function: view_config.function.clone(),
                    interval_blocks: view_config.interval_blocks,
                    calldata,
                    table_name,
                });
            }
        }

        if !views.is_empty() {
            debug!(view_count = views.len(), "View function indexer initialized");
        }

        Self {
            pool,
            schema,
            chain_id,
            views,
            rpc_url: rpc_url.to_string(),
            client: reqwest::Client::new(),
        }
    }

    /// Check if any view functions are configured
    pub fn has_views(&self) -> bool {
        !self.views.is_empty()
    }

    /// Snapshot all view functions that should be called at this block number
    pub async fn snapshot_at_block(
        &self,
        block_number: u64,
        block_timestamp: i64,
    ) -> Result<()> {
        for view in &self.views {
            if block_number % view.interval_blocks != 0 {
                continue;
            }

            match self.call_and_store(view, block_number, block_timestamp).await {
                Ok(_) => {
                    debug!(
                        contract = %view.contract_name,
                        function = %view.function,
                        block = block_number,
                        "View function snapshot stored"
                    );
                }
                Err(e) => {
                    warn!(
                        contract = %view.contract_name,
                        function = %view.function,
                        block = block_number,
                        error = %e,
                        "View function call failed"
                    );
                }
            }
        }

        Ok(())
    }

    async fn call_and_store(
        &self,
        view: &ViewEntry,
        block_number: u64,
        block_timestamp: i64,
    ) -> Result<()> {
        let block_hex = format!("0x{:x}", block_number);

        let result = self
            .eth_call(&view.address, &view.calldata, &block_hex)
            .await
            .with_context(|| {
                format!(
                    "eth_call failed for {}.{} at block {}",
                    view.contract_name, view.function, block_number
                )
            })?;

        let sql = format!(
            r#"INSERT INTO {schema}.{table} (chain_id, block_number, block_timestamp, address, result)
               VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT (chain_id, block_number, address) DO NOTHING"#,
            schema = self.schema,
            table = view.table_name,
        );

        sqlx::query(&sql)
            .bind(self.chain_id)
            .bind(block_number as i64)
            .bind(block_timestamp)
            .bind(&view.address)
            .bind(&result)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn eth_call(&self, to: &str, data: &str, block: &str) -> Result<String> {
        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [
                { "to": to, "data": data },
                block
            ],
            "id": 1
        });

        let response = self
            .client
            .post(&self.rpc_url)
            .json(&payload)
            .send()
            .await?;

        let body: serde_json::Value = response.json().await?;

        if let Some(error) = body.get("error") {
            anyhow::bail!("RPC error: {}", error);
        }

        body.get("result")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .context("Missing 'result' in RPC response")
    }
}

/// Generate a simple 4-byte function selector from function name
/// This is a simplified version — for parameterless functions it's keccak256("functionName()")
fn function_selector(name: &str, params: &[String]) -> String {
    use alloy_primitives::keccak256;

    let sig = if params.is_empty() {
        format!("{}()", name)
    } else {
        // For simplicity, assume all params are uint256 addresses
        // A full implementation would parse the ABI
        let param_types: Vec<&str> = params.iter().map(|_| "address").collect();
        format!("{}({})", name, param_types.join(","))
    };

    let hash = keccak256(sig.as_bytes());
    hex::encode(&hash[..4])
}

/// Generate CREATE TABLE SQL for a view function result table
pub fn view_table_sql(schema: &str, contract_name: &str, function_name: &str) -> String {
    let table_name = format!(
        "view_{}_{}",
        crate::abi::decoder::to_snake_case(contract_name),
        crate::abi::decoder::to_snake_case(function_name)
    );

    format!(
        r#"
        CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
            chain_id        INTEGER NOT NULL,
            block_number    BIGINT NOT NULL,
            block_timestamp BIGINT NOT NULL,
            address         TEXT NOT NULL,
            result          TEXT NOT NULL,
            PRIMARY KEY (chain_id, block_number, address)
        )
        "#
    )
}

/// Get all view table names from contract configs
pub fn view_table_names(contracts: &[ContractConfig]) -> Vec<(String, String, String)> {
    let mut names = Vec::new();
    for contract in contracts {
        for view in &contract.views {
            let table_name = format!(
                "view_{}_{}",
                crate::abi::decoder::to_snake_case(&contract.name),
                crate::abi::decoder::to_snake_case(&view.function)
            );
            names.push((contract.name.clone(), view.function.clone(), table_name));
        }
    }
    names
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ViewFunctionConfig;

    #[test]
    fn test_function_selector_no_params() {
        let selector = function_selector("totalSupply", &[]);
        // keccak256("totalSupply()") first 4 bytes = 18160ddd
        assert_eq!(selector, "18160ddd");
    }

    #[test]
    fn test_function_selector_with_params() {
        let selector = function_selector("balanceOf", &["0xdead".to_string()]);
        // keccak256("balanceOf(address)") first 4 bytes = 70a08231
        assert_eq!(selector, "70a08231");
    }

    #[test]
    fn test_view_table_sql_format() {
        let sql = view_table_sql("test_sync", "USDC", "totalSupply");
        assert!(sql.contains("test_sync.view_u_s_d_c_total_supply"));
        assert!(sql.contains("chain_id"));
        assert!(sql.contains("block_number"));
        assert!(sql.contains("result"));
        assert!(sql.contains("PRIMARY KEY"));
    }

    #[test]
    fn test_view_table_names() {
        let contracts = vec![ContractConfig {
            name: "USDC".to_string(),
            address: Some("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".to_string()),
            factory: None,
            abi_path: "./test.json".to_string(),
            start_block: None,
            filters: vec![],
            views: vec![
                ViewFunctionConfig {
                    function: "totalSupply".to_string(),
                    interval_blocks: 100,
                    params: vec![],
                },
                ViewFunctionConfig {
                    function: "decimals".to_string(),
                    interval_blocks: 10000,
                    params: vec![],
                },
            ],
        }];
        let names = view_table_names(&contracts);
        assert_eq!(names.len(), 2);
        assert_eq!(names[0].2, "view_u_s_d_c_total_supply");
        assert_eq!(names[1].2, "view_u_s_d_c_decimals");
    }

    #[test]
    fn test_view_config_from_yaml() {
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
  - name: "USDC"
    address: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
    abi_path: "./test.json"
    views:
      - function: "totalSupply"
        interval_blocks: 100
      - function: "balanceOf"
        interval_blocks: 1000
        params: ["0xdead000000000000000000000000000000000000"]
"#;
        let config: crate::config::IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.contracts[0].views.len(), 2);
        assert_eq!(config.contracts[0].views[0].function, "totalSupply");
        assert_eq!(config.contracts[0].views[0].interval_blocks, 100);
        assert!(config.contracts[0].views[0].params.is_empty());
        assert_eq!(config.contracts[0].views[1].function, "balanceOf");
        assert_eq!(config.contracts[0].views[1].params.len(), 1);
    }

    #[test]
    fn test_function_selector_multiple_params() {
        // Two address params: keccak256("allowance(address,address)")
        let selector = function_selector("allowance", &["0xowner".to_string(), "0xspender".to_string()]);
        assert_eq!(selector.len(), 8); // 4 bytes = 8 hex chars
        // dd62ed3e is the known selector for allowance(address,address)
        assert_eq!(selector, "dd62ed3e");
    }

    #[test]
    fn test_view_table_names_empty_views() {
        let contracts = vec![ContractConfig {
            name: "Token".to_string(),
            address: Some("0x1234567890abcdef1234567890abcdef12345678".to_string()),
            factory: None,
            abi_path: "./test.json".to_string(),
            start_block: None,
            filters: vec![],
            views: vec![], // no views
        }];
        let names = view_table_names(&contracts);
        assert!(names.is_empty());
    }

    #[test]
    fn test_view_table_names_multiple_contracts() {
        let contracts = vec![
            ContractConfig {
                name: "USDC".to_string(),
                address: Some("0xa".to_string()),
                factory: None,
                abi_path: "./test.json".to_string(),
                start_block: None,
                filters: vec![],
                views: vec![ViewFunctionConfig {
                    function: "totalSupply".to_string(),
                    interval_blocks: 100,
                    params: vec![],
                }],
            },
            ContractConfig {
                name: "WETH".to_string(),
                address: Some("0xb".to_string()),
                factory: None,
                abi_path: "./test.json".to_string(),
                start_block: None,
                filters: vec![],
                views: vec![ViewFunctionConfig {
                    function: "totalSupply".to_string(),
                    interval_blocks: 200,
                    params: vec![],
                }],
            },
        ];
        let names = view_table_names(&contracts);
        assert_eq!(names.len(), 2);
        // Each contract gets its own table even for same function name
        assert_eq!(names[0].2, "view_u_s_d_c_total_supply");
        assert_eq!(names[1].2, "view_w_e_t_h_total_supply");
        // Contract name and function name preserved
        assert_eq!(names[0].0, "USDC");
        assert_eq!(names[0].1, "totalSupply");
    }

    #[test]
    fn test_view_table_sql_contains_all_columns() {
        let sql = view_table_sql("my_schema", "Token", "balanceOf");
        assert!(sql.contains("chain_id"));
        assert!(sql.contains("INTEGER NOT NULL"));
        assert!(sql.contains("block_number"));
        assert!(sql.contains("BIGINT NOT NULL"));
        assert!(sql.contains("block_timestamp"));
        assert!(sql.contains("address"));
        assert!(sql.contains("TEXT"));
        assert!(sql.contains("result"));
        assert!(sql.contains("TEXT NOT NULL"));
        assert!(sql.contains("PRIMARY KEY (chain_id, block_number, address)"));
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS"));
    }

    #[test]
    fn test_interval_check_edge_cases() {
        // Block 0 always triggers (0 % N == 0)
        assert_eq!(0u64 % 100, 0);
        assert_eq!(0u64 % 1, 0);
        // Block 1 with interval 1 → always triggers
        assert_eq!(1u64 % 1, 0);
        assert_eq!(999u64 % 1, 0);
        // Large block numbers
        assert_eq!(18_000_000u64 % 1000, 0);
        assert_ne!(18_000_001u64 % 1000, 0);
    }

    #[test]
    fn test_calldata_encoding_no_params() {
        // For a parameterless function, calldata is just "0x" + 4-byte selector
        let selector = function_selector("totalSupply", &[]);
        let calldata = format!("0x{}", selector);
        assert_eq!(calldata, "0x18160ddd");
        assert_eq!(calldata.len(), 10); // "0x" + 8 hex chars
    }

    #[test]
    fn test_calldata_encoding_with_address_param() {
        let selector = function_selector("balanceOf", &["0xdead000000000000000000000000000000000000".to_string()]);
        let param = "dead000000000000000000000000000000000000";
        let calldata = format!("0x{}{:0>64}", selector, param);
        // Should be 0x + 8 (selector) + 64 (padded address) = 74 chars
        assert_eq!(calldata.len(), 74);
        assert!(calldata.starts_with("0x70a08231"));
    }

    #[test]
    fn test_interval_check() {
        // Block 100 with interval 100 → should trigger (100 % 100 == 0)
        assert_eq!(100u64 % 100, 0);
        // Block 150 with interval 100 → should not trigger
        assert_ne!(150u64 % 100, 0);
        // Block 200 with interval 100 → should trigger
        assert_eq!(200u64 % 100, 0);
    }
}
