use crate::abi::decoder::{AbiDecoder, ParsedEvent};
use crate::config::ContractConfig;
use crate::types::{FactoryChild, LogEntry};
use anyhow::{Context, Result};
use tracing::{debug, trace, warn};

/// Watches for factory events and extracts child contract addresses.
/// Ported from packages/syncer/src/services/FactoryWatcher.ts
pub struct FactoryWatcher {
    /// Factory configurations: (factory_address, factory_event_signature, parameter_indices, contract_name)
    factories: Vec<FactoryConfig>,
}

/// Internal configuration for a single factory contract
#[derive(Debug, Clone)]
struct FactoryConfig {
    /// The factory contract address (lowercased)
    factory_address: String,
    /// The event signature (topic0) to watch for
    event_signature: String,
    /// The name of the child contract being created
    contract_name: String,
    /// Indices of the parameters containing child addresses
    /// (indexed params map to topics, non-indexed to data)
    child_param_locations: Vec<ParamLocation>,
    /// Chain ID for created FactoryChild records
    chain_id: i32,
}

/// Where to find a child address parameter in the log
#[derive(Debug, Clone)]
enum ParamLocation {
    /// Indexed param at topic index (1-3, since topic0 is the event signature)
    Topic(usize),
    /// Non-indexed param at data offset (0-based, each param is 32 bytes)
    Data(usize),
}

impl FactoryWatcher {
    /// Create a new FactoryWatcher from contract configs and ABI decoder
    pub fn new(
        contracts: &[ContractConfig],
        decoder: &AbiDecoder,
        chain_id: i32,
    ) -> Result<Self> {
        let mut factories = Vec::new();

        for contract in contracts {
            let factory_config = match &contract.factory {
                Some(f) => f,
                None => continue,
            };

            // Find the contract whose address matches the factory address to get the correct ABI.
            // The factory event (e.g. PairCreated) lives in the factory's ABI, not the child's ABI.
            let factory_contract_name = contracts
                .iter()
                .find(|c| {
                    c.address
                        .as_ref()
                        .map(|a| a.to_lowercase() == factory_config.address.to_lowercase())
                        .unwrap_or(false)
                })
                .map(|c| c.name.as_str())
                .unwrap_or(&contract.name);

            let events = decoder
                .get_events(factory_contract_name)
                .with_context(|| {
                    format!(
                        "No ABI registered for factory contract '{}' (looked up from factory address '{}')",
                        factory_contract_name, factory_config.address
                    )
                })?;

            let factory_event = events
                .iter()
                .find(|e| e.name == factory_config.event)
                .with_context(|| {
                    format!(
                        "Event '{}' not found in ABI for contract '{}'",
                        factory_config.event, factory_contract_name
                    )
                })?;

            let param_names = factory_config.parameter.as_vec();
            let child_param_locations =
                resolve_param_locations(factory_event, &param_names)?;

            factories.push(FactoryConfig {
                factory_address: factory_config.address.to_lowercase(),
                event_signature: factory_event.signature.clone(),
                contract_name: factory_config
                    .child_contract_name
                    .clone()
                    .unwrap_or_else(|| contract.name.clone()),
                child_param_locations,
                chain_id,
            });

            debug!(
                factory_address = %factory_config.address,
                event = %factory_config.event,
                contract_name = %contract.name,
                "Registered factory watcher"
            );
        }

        Ok(Self { factories })
    }

    /// Process a batch of logs and extract any factory-created child contracts
    pub fn process_logs(&self, logs: &[LogEntry]) -> Vec<FactoryChild> {
        let mut children = Vec::new();

        for log in logs {
            let topic0 = match &log.topic0 {
                Some(t) => t,
                None => continue,
            };

            let address = log.address.to_lowercase();

            for factory in &self.factories {
                if address != factory.factory_address || *topic0 != factory.event_signature {
                    continue;
                }

                // This log matches a factory event — extract child addresses
                match extract_child_addresses(log, &factory.child_param_locations) {
                    Ok(addresses) => {
                        for child_address in addresses {
                            trace!(
                                factory = %factory.factory_address,
                                child = %child_address,
                                block = log.block_number,
                                "Discovered factory child"
                            );

                            children.push(FactoryChild {
                                chain_id: factory.chain_id,
                                factory_address: factory.factory_address.clone(),
                                child_address,
                                contract_name: factory.contract_name.clone(),
                                created_at_block: log.block_number as i64,
                                created_at_tx_hash: log.transaction_hash.clone(),
                                created_at_log_index: log.log_index as i32,
                                metadata: None,
                                child_abi: None,
                            });
                        }
                    }
                    Err(e) => {
                        warn!(
                            factory = %factory.factory_address,
                            block = log.block_number,
                            error = %e,
                            "Failed to extract child address from factory event"
                        );
                    }
                }
            }
        }

        children
    }

    /// Check if any factory watchers are configured
    pub fn has_factories(&self) -> bool {
        !self.factories.is_empty()
    }

    /// Get the set of factory addresses being watched
    pub fn factory_addresses(&self) -> Vec<&str> {
        self.factories
            .iter()
            .map(|f| f.factory_address.as_str())
            .collect()
    }
}

/// Resolve parameter names to their locations in the log (topic index or data offset)
fn resolve_param_locations(
    event: &ParsedEvent,
    param_names: &[&str],
) -> Result<Vec<ParamLocation>> {
    let mut locations = Vec::new();

    for param_name in param_names {
        // Check indexed params (these map to topics 1-3)
        if let Some(idx) = event
            .indexed_params
            .iter()
            .position(|(name, _)| name == param_name)
        {
            // topic0 is the event signature, so indexed params start at topic1
            locations.push(ParamLocation::Topic(idx + 1));
            continue;
        }

        // Check non-indexed params (these are ABI-encoded in the data field)
        if let Some(idx) = event
            .data_params
            .iter()
            .position(|(name, _)| name == param_name)
        {
            locations.push(ParamLocation::Data(idx));
            continue;
        }

        anyhow::bail!(
            "Parameter '{}' not found in event '{}'. Available params: {:?}, {:?}",
            param_name,
            event.name,
            event.indexed_params,
            event.data_params,
        );
    }

    Ok(locations)
}

/// Extract child contract addresses from a log entry based on parameter locations
fn extract_child_addresses(
    log: &LogEntry,
    locations: &[ParamLocation],
) -> Result<Vec<String>> {
    let mut addresses = Vec::new();

    for location in locations {
        let address = match location {
            ParamLocation::Topic(topic_index) => {
                let topic = match topic_index {
                    1 => log.topic1.as_deref(),
                    2 => log.topic2.as_deref(),
                    3 => log.topic3.as_deref(),
                    _ => None,
                };

                let topic_hex = topic.with_context(|| {
                    format!("Expected topic at index {} but it's missing", topic_index)
                })?;

                // Topics are 32 bytes, address is 20 bytes (last 40 hex chars)
                // Topic: 0x000000000000000000000000<address_20_bytes>
                extract_address_from_word(topic_hex)?
            }
            ParamLocation::Data(data_index) => {
                // Data is ABI-encoded: each param is a 32-byte word
                // Data format: "0x" + 64 hex chars per param
                let data = &log.data;
                if !data.starts_with("0x") {
                    anyhow::bail!("Invalid data format: missing 0x prefix");
                }

                let hex_data = &data[2..];
                let offset = data_index * 64; // 32 bytes = 64 hex chars

                if hex_data.len() < offset + 64 {
                    anyhow::bail!(
                        "Data too short for param at index {}: need {} hex chars, have {}",
                        data_index,
                        offset + 64,
                        hex_data.len()
                    );
                }

                let word = &hex_data[offset..offset + 64];
                extract_address_from_word(&format!("0x{}", word))?
            }
        };

        addresses.push(address);
    }

    Ok(addresses)
}

/// Extract a 20-byte address from a 32-byte hex word
/// Input: "0x000000000000000000000000abcdef1234567890abcdef1234567890abcdef12"
/// Output: "0xabcdef1234567890abcdef1234567890abcdef12"
fn extract_address_from_word(word: &str) -> Result<String> {
    let hex = word.strip_prefix("0x").unwrap_or(word);

    if hex.len() < 40 {
        anyhow::bail!("Word too short to contain an address: {}", word);
    }

    // Address is the last 40 hex characters (20 bytes)
    let address = &hex[hex.len() - 40..];
    Ok(format!("0x{}", address.to_lowercase()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::abi::decoder::AbiDecoder;
    use crate::config::{ContractConfig, FactoryContractConfig, FactoryParameter};

    fn create_test_decoder() -> AbiDecoder {
        let mut decoder = AbiDecoder::new();
        decoder
            .register_abi_json(
                "UniswapV2Factory",
                r#"[{
                    "type": "event",
                    "name": "PairCreated",
                    "anonymous": false,
                    "inputs": [
                        { "type": "address", "name": "token0", "indexed": true },
                        { "type": "address", "name": "token1", "indexed": true },
                        { "type": "address", "name": "pair", "indexed": false },
                        { "type": "uint256", "name": "pairIndex", "indexed": false }
                    ]
                }]"#,
            )
            .unwrap();
        decoder
    }

    fn create_test_contracts() -> Vec<ContractConfig> {
        vec![ContractConfig {
            name: "UniswapV2Factory".to_string(),
            address: None,
            factory: Some(FactoryContractConfig {
                address: "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f".to_string(),
                event: "PairCreated".to_string(),
                parameter: FactoryParameter::Single("pair".to_string()),
                child_abi_path: None,
                child_contract_name: Some("UniswapV2Pair".to_string()),
            }),
            abi_path: "./abis/UniswapV2Factory.json".to_string(),
            start_block: Some(10000835),
            filters: vec![],
            views: vec![],
        }]
    }

    #[test]
    fn test_factory_watcher_creation() {
        let decoder = create_test_decoder();
        let contracts = create_test_contracts();

        let watcher = FactoryWatcher::new(&contracts, &decoder, 1).unwrap();
        assert!(watcher.has_factories());
        assert_eq!(watcher.factory_addresses().len(), 1);
    }

    #[test]
    fn test_extract_address_from_word() {
        // Standard 32-byte padded address
        let word = "0x000000000000000000000000b4e16d0168e52d35cacd2c6185b44281ec28c9dc";
        let addr = extract_address_from_word(word).unwrap();
        assert_eq!(addr, "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc");
    }

    #[test]
    fn test_extract_child_from_data_param() {
        let decoder = create_test_decoder();
        let contracts = create_test_contracts();
        let watcher = FactoryWatcher::new(&contracts, &decoder, 1).unwrap();

        // Simulate a PairCreated event
        // token0 indexed (topic1), token1 indexed (topic2), pair non-indexed (data[0])
        let pair_created_sig = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9";
        let pair_address = "b4e16d0168e52d35cacd2c6185b44281ec28c9dc";

        let log = LogEntry {
            block_number: 10000835,
            block_hash: "0xabcd".to_string(),
            block_timestamp: 1600000000,
            transaction_hash: "0x1234".to_string(),
            transaction_index: 0,
            log_index: 0,
            address: "0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f".to_string(),
            topic0: Some(pair_created_sig.to_string()),
            topic1: Some("0x000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2".to_string()),
            topic2: Some("0x000000000000000000000000dac17f958d2ee523a2206206994597c13d831ec7".to_string()),
            topic3: None,
            // data: pair address (32 bytes) + pairIndex (32 bytes)
            data: format!(
                "0x000000000000000000000000{}0000000000000000000000000000000000000000000000000000000000000001",
                pair_address
            ),
        };

        let children = watcher.process_logs(&[log]);
        assert_eq!(children.len(), 1);
        assert_eq!(
            children[0].child_address,
            format!("0x{}", pair_address)
        );
        assert_eq!(children[0].contract_name, "UniswapV2Pair");
        assert_eq!(children[0].created_at_block, 10000835);
    }

    #[test]
    fn test_no_match_for_non_factory_logs() {
        let decoder = create_test_decoder();
        let contracts = create_test_contracts();
        let watcher = FactoryWatcher::new(&contracts, &decoder, 1).unwrap();

        // Random log that doesn't match any factory
        let log = LogEntry {
            block_number: 10000835,
            block_hash: "0xabcd".to_string(),
            block_timestamp: 1600000000,
            transaction_hash: "0x1234".to_string(),
            transaction_index: 0,
            log_index: 0,
            address: "0xdead000000000000000000000000000000000000".to_string(),
            topic0: Some("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef".to_string()),
            topic1: None,
            topic2: None,
            topic3: None,
            data: "0x".to_string(),
        };

        let children = watcher.process_logs(&[log]);
        assert!(children.is_empty());
    }

    #[test]
    fn test_indexed_param_extraction() {
        // Test with a factory where the child address is in an indexed param (topic)
        let mut decoder = AbiDecoder::new();
        decoder
            .register_abi_json(
                "TestFactory",
                r#"[{
                    "type": "event",
                    "name": "ChildCreated",
                    "anonymous": false,
                    "inputs": [
                        { "type": "address", "name": "child", "indexed": true },
                        { "type": "uint256", "name": "id", "indexed": false }
                    ]
                }]"#,
            )
            .unwrap();

        let contracts = vec![ContractConfig {
            name: "TestFactory".to_string(),
            address: None,
            factory: Some(FactoryContractConfig {
                address: "0xfactory0000000000000000000000000000000000".to_string(),
                event: "ChildCreated".to_string(),
                parameter: FactoryParameter::Single("child".to_string()),
                child_abi_path: None,
                child_contract_name: Some("TestChild".to_string()),
            }),
            abi_path: "./test.json".to_string(),
            start_block: None,
            filters: vec![],
            views: vec![],
        }];

        let watcher = FactoryWatcher::new(&contracts, &decoder, 1).unwrap();

        let child_addr = "aabbccdd00000000000000000000000000000001";
        let events = decoder.get_events("TestFactory").unwrap();
        let sig = &events[0].signature;

        let log = LogEntry {
            block_number: 100,
            block_hash: "0xhash".to_string(),
            block_timestamp: 1700000000,
            transaction_hash: "0xtx".to_string(),
            transaction_index: 0,
            log_index: 0,
            address: "0xfactory0000000000000000000000000000000000".to_string(),
            topic0: Some(sig.clone()),
            topic1: Some(format!("0x000000000000000000000000{}", child_addr)),
            topic2: None,
            topic3: None,
            data: "0x0000000000000000000000000000000000000000000000000000000000000001".to_string(),
        };

        let children = watcher.process_logs(&[log]);
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].child_address, format!("0x{}", child_addr));
        assert_eq!(children[0].contract_name, "TestChild");
    }

    #[test]
    fn test_resolve_param_locations() {
        let decoder = create_test_decoder();
        let events = decoder.get_events("UniswapV2Factory").unwrap();
        let pair_created = &events[0];

        // "pair" is a non-indexed param at data index 0
        let locations = resolve_param_locations(pair_created, &["pair"]).unwrap();
        assert_eq!(locations.len(), 1);
        assert!(matches!(locations[0], ParamLocation::Data(0)));

        // "token0" is an indexed param at topic index 1
        let locations = resolve_param_locations(pair_created, &["token0"]).unwrap();
        assert_eq!(locations.len(), 1);
        assert!(matches!(locations[0], ParamLocation::Topic(1)));

        // "token1" is an indexed param at topic index 2
        let locations = resolve_param_locations(pair_created, &["token1"]).unwrap();
        assert_eq!(locations.len(), 1);
        assert!(matches!(locations[0], ParamLocation::Topic(2)));

        // Non-existent param should error
        let result = resolve_param_locations(pair_created, &["nonexistent"]);
        assert!(result.is_err());
    }

    // === Additional factory watcher tests ===

    #[test]
    fn test_process_empty_logs() {
        let decoder = create_test_decoder();
        let contracts = create_test_contracts();
        let watcher = FactoryWatcher::new(&contracts, &decoder, 1).unwrap();

        let children = watcher.process_logs(&[]);
        assert!(children.is_empty());
    }

    #[test]
    fn test_process_logs_no_topic0() {
        let decoder = create_test_decoder();
        let contracts = create_test_contracts();
        let watcher = FactoryWatcher::new(&contracts, &decoder, 1).unwrap();

        let log = LogEntry {
            block_number: 100,
            block_hash: "0x".to_string(),
            block_timestamp: 0,
            transaction_hash: "0x".to_string(),
            transaction_index: 0,
            log_index: 0,
            address: "0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f".to_string(),
            topic0: None, // No topic0
            topic1: None,
            topic2: None,
            topic3: None,
            data: "0x".to_string(),
        };

        let children = watcher.process_logs(&[log]);
        assert!(children.is_empty());
    }

    #[test]
    fn test_no_factory_contracts() {
        let decoder = create_test_decoder();
        // A contract without factory config
        let contracts = vec![ContractConfig {
            name: "UniswapV2Factory".to_string(),
            address: Some("0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f".to_string()),
            factory: None,
            abi_path: "./test.json".to_string(),
            start_block: None,
            filters: vec![],
            views: vec![],
        }];

        let watcher = FactoryWatcher::new(&contracts, &decoder, 1).unwrap();
        assert!(!watcher.has_factories());
        assert!(watcher.factory_addresses().is_empty());
    }

    #[test]
    fn test_extract_address_from_word_no_prefix() {
        let word = "000000000000000000000000b4e16d0168e52d35cacd2c6185b44281ec28c9dc";
        let addr = extract_address_from_word(word).unwrap();
        assert_eq!(addr, "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc");
    }

    #[test]
    fn test_extract_address_from_word_too_short() {
        let result = extract_address_from_word("0x1234");
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_address_uppercase_lowercased() {
        let word = "0x000000000000000000000000ABCDEF1234567890ABCDEF1234567890ABCDEF12";
        let addr = extract_address_from_word(word).unwrap();
        assert_eq!(addr, "0xabcdef1234567890abcdef1234567890abcdef12");
    }

    #[test]
    fn test_factory_child_chain_id_propagation() {
        let decoder = create_test_decoder();
        let contracts = create_test_contracts();
        let watcher = FactoryWatcher::new(&contracts, &decoder, 42).unwrap(); // chain_id=42

        let pair_created_sig = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9";
        let log = LogEntry {
            block_number: 100,
            block_hash: "0x".to_string(),
            block_timestamp: 0,
            transaction_hash: "0xtx".to_string(),
            transaction_index: 0,
            log_index: 0,
            address: "0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f".to_string(),
            topic0: Some(pair_created_sig.to_string()),
            topic1: Some("0x000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2".to_string()),
            topic2: Some("0x000000000000000000000000dac17f958d2ee523a2206206994597c13d831ec7".to_string()),
            topic3: None,
            data: format!(
                "0x000000000000000000000000b4e16d0168e52d35cacd2c6185b44281ec28c9dc0000000000000000000000000000000000000000000000000000000000000001"
            ),
        };

        let children = watcher.process_logs(&[log]);
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].chain_id, 42);
    }

    #[test]
    fn test_factory_address_case_insensitive_match() {
        let decoder = create_test_decoder();
        let contracts = create_test_contracts();
        let watcher = FactoryWatcher::new(&contracts, &decoder, 1).unwrap();

        // Factory addresses are lowercased during creation
        let addrs = watcher.factory_addresses();
        for addr in &addrs {
            assert_eq!(*addr, addr.to_lowercase());
        }
    }
}
