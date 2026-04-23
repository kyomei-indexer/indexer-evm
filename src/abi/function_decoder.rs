use alloy_json_abi::JsonAbi;
use alloy_primitives::keccak256;
use anyhow::{Context, Result};
use std::collections::HashMap;
use tracing::{debug, trace};

use super::decoder::{decode_value, quote_if_reserved, to_snake_case, AbiDecoder};
use crate::types::RawTraceRecord;

/// A parsed function from an ABI
#[derive(Debug, Clone)]
pub struct ParsedFunction {
    /// The function name (e.g., "swapExactTokensForTokens")
    pub name: String,
    /// The 4-byte selector as hex (e.g., "0x38ed1739")
    pub selector: String,
    /// The full signature string (e.g., "swapExactTokensForTokens(uint256,uint256,address[],address,uint256)")
    pub signature_string: String,
    /// Input parameter names and Solidity types
    pub inputs: Vec<(String, String)>,
    /// Output parameter names and Solidity types
    pub outputs: Vec<(String, String)>,
}

/// Decoded trace row ready for insertion into a per-function table
#[derive(Debug, Clone)]
pub struct DecodedTraceRow {
    /// Target table name (e.g., "trace_uniswap_v2_router_swap_exact_tokens_for_tokens")
    pub table_name: String,
    /// Original contract name
    pub contract_name: String,
    /// Original function name
    pub function_name: String,
    /// Decoded input parameter column names
    pub input_columns: Vec<String>,
    /// Decoded input parameter values as strings
    pub input_values: Vec<String>,
    // Base trace fields
    pub chain_id: i32,
    pub block_number: i64,
    pub tx_index: i32,
    pub trace_address: String,
    pub block_hash: String,
    pub block_timestamp: i64,
    pub tx_hash: String,
    pub from_address: String,
    pub to_address: String,
    pub value: String,
    pub gas: i64,
    pub gas_used: i64,
    pub output: String,
    pub error: Option<String>,
}

/// Function decoder: loads ABI files, parses functions, and maps 4-byte selectors
pub struct FunctionDecoder {
    /// contract_name → list of parsed functions
    contracts: HashMap<String, Vec<ParsedFunction>>,
    /// 4-byte selector (e.g. "0x38ed1739") → list of (contract_name, function)
    selector_index: HashMap<String, Vec<(String, ParsedFunction)>>,
}

impl FunctionDecoder {
    pub fn new() -> Self {
        Self {
            contracts: HashMap::new(),
            selector_index: HashMap::new(),
        }
    }

    /// Load and register an ABI from a JSON file
    pub fn register_abi_file(&mut self, contract_name: &str, path: &str) -> Result<()> {
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read ABI file: {}", path))?;
        self.register_abi_json(contract_name, &contents)
            .with_context(|| format!("Failed to parse ABI file: {}", path))
    }

    /// Register an ABI from a JSON string
    pub fn register_abi_json(&mut self, contract_name: &str, json: &str) -> Result<()> {
        let abi: JsonAbi = serde_json::from_str(json)
            .with_context(|| format!("Invalid ABI JSON for contract '{}'", contract_name))?;
        self.register_abi(contract_name, &abi)
    }

    /// Register a parsed ABI, extracting all functions
    pub fn register_abi(&mut self, contract_name: &str, abi: &JsonAbi) -> Result<()> {
        let mut functions = Vec::new();

        for (fn_name, fn_list) in abi.functions.iter() {
            for func in fn_list {
                let parsed = parse_function(fn_name, func)?;

                self.selector_index
                    .entry(parsed.selector.clone())
                    .or_default()
                    .push((contract_name.to_string(), parsed.clone()));

                functions.push(parsed);
            }
        }

        debug!(
            contract = contract_name,
            function_count = functions.len(),
            "Registered ABI functions"
        );

        self.contracts
            .insert(contract_name.to_string(), functions);

        Ok(())
    }

    /// Get all registered functions for a contract
    pub fn get_functions(&self, contract_name: &str) -> Option<&[ParsedFunction]> {
        self.contracts.get(contract_name).map(|v| v.as_slice())
    }

    /// Look up a function by its 4-byte selector
    pub fn get_function_by_selector(&self, selector: &str) -> Option<&[(String, ParsedFunction)]> {
        self.selector_index.get(selector).map(|v| v.as_slice())
    }

    /// Get all unique selectors across all registered contracts
    pub fn all_selectors(&self) -> Vec<&str> {
        self.selector_index.keys().map(|s| s.as_str()).collect()
    }

    /// Check if any functions are registered
    pub fn is_empty(&self) -> bool {
        self.contracts.is_empty()
    }

    /// Decode a raw trace record into a DecodedTraceRow for insertion into a per-function table.
    /// Returns None if the selector is not recognized or input is too short.
    pub fn decode_trace(&self, trace: &RawTraceRecord) -> Option<DecodedTraceRow> {
        let selector = trace.function_sig.as_deref()?;
        let matches = self.get_function_by_selector(selector)?;
        if matches.is_empty() {
            return None;
        }

        let (contract_name, parsed) = &matches[0];

        let table_name = format!(
            "trace_{}_{}",
            to_snake_case(contract_name),
            to_snake_case(&parsed.name)
        );

        let input_columns: Vec<String> = parsed
            .inputs
            .iter()
            .map(|(name, _)| quote_if_reserved(&to_snake_case(name)))
            .collect();

        // Decode input params from calldata (skip 4-byte selector = 10 hex chars with "0x")
        let data_hex = trace.input.strip_prefix("0x").unwrap_or(&trace.input);
        let params_hex = if data_hex.len() > 8 {
            &data_hex[8..]
        } else {
            ""
        };

        let mut input_values = Vec::with_capacity(parsed.inputs.len());
        for (i, (_name, sol_type)) in parsed.inputs.iter().enumerate() {
            let offset = i * 64;
            let value = if params_hex.len() >= offset + 64 {
                let word = &params_hex[offset..offset + 64];
                decode_value(&format!("0x{}", word), sol_type)
            } else {
                "\\N".to_string()
            };
            input_values.push(value);
        }

        trace!(
            contract = %contract_name,
            function = %parsed.name,
            selector = selector,
            "Decoded trace function call"
        );

        Some(DecodedTraceRow {
            table_name,
            contract_name: contract_name.to_string(),
            function_name: parsed.name.clone(),
            input_columns,
            input_values,
            chain_id: trace.chain_id,
            block_number: trace.block_number,
            tx_index: trace.tx_index,
            trace_address: trace.trace_address.clone(),
            block_hash: trace.block_hash.clone(),
            block_timestamp: trace.block_timestamp,
            tx_hash: trace.tx_hash.clone(),
            from_address: trace.from_address.clone(),
            to_address: trace.to_address.clone(),
            value: trace.value.clone(),
            gas: trace.gas,
            gas_used: trace.gas_used,
            output: trace.output.clone(),
            error: trace.error.clone(),
        })
    }

    /// Generate CREATE TABLE SQL for a per-function decoded trace table
    pub fn generate_trace_table_sql(
        &self,
        schema: &str,
        contract_name: &str,
        function: &ParsedFunction,
    ) -> String {
        let table_name = format!(
            "trace_{}_{}",
            to_snake_case(contract_name),
            to_snake_case(&function.name)
        );

        let mut columns = vec![
            "chain_id INTEGER NOT NULL".to_string(),
            "block_number BIGINT NOT NULL".to_string(),
            "tx_index INTEGER NOT NULL".to_string(),
            "trace_address TEXT NOT NULL".to_string(),
            "block_hash TEXT NOT NULL".to_string(),
            "block_timestamp BIGINT NOT NULL".to_string(),
            "tx_hash TEXT NOT NULL".to_string(),
            "from_address TEXT NOT NULL".to_string(),
            "to_address TEXT NOT NULL".to_string(),
            "\"value\" NUMERIC NOT NULL".to_string(),
            "gas BIGINT NOT NULL".to_string(),
            "gas_used BIGINT NOT NULL".to_string(),
            "output TEXT NOT NULL".to_string(),
            "error TEXT".to_string(),
        ];

        // Add decoded input params
        for (name, sol_type) in &function.inputs {
            let pg_type = AbiDecoder::solidity_to_pg_type(sol_type);
            let col_name = to_snake_case(name);
            let quoted = quote_if_reserved(&col_name);
            columns.push(format!("{} {}", quoted, pg_type));
        }

        let columns_sql = columns.join(",\n            ");

        format!(
            r#"CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
            {columns_sql},
            PRIMARY KEY (chain_id, block_number, tx_index, trace_address)
        )"#
        )
    }
}

/// Parse a single function from the ABI into a ParsedFunction
fn parse_function(
    name: &str,
    func: &alloy_json_abi::Function,
) -> Result<ParsedFunction> {
    let mut inputs = Vec::new();
    for input in &func.inputs {
        inputs.push((input.name.clone(), input.ty.clone()));
    }

    let mut outputs = Vec::new();
    for output in &func.outputs {
        let out_name = if output.name.is_empty() {
            format!("output_{}", outputs.len())
        } else {
            output.name.clone()
        };
        outputs.push((out_name, output.ty.clone()));
    }

    // Build signature string for selector computation
    let input_types: Vec<&str> = func.inputs.iter().map(|p| p.ty.as_str()).collect();
    let sig_string = format!("{}({})", name, input_types.join(","));
    let hash = keccak256(sig_string.as_bytes());
    let selector = format!("0x{}", hex::encode(&hash[..4]));

    trace!(
        function = name,
        selector = %selector,
        sig = %sig_string,
        "Parsed function"
    );

    Ok(ParsedFunction {
        name: name.to_string(),
        selector,
        signature_string: sig_string,
        inputs,
        outputs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn erc20_abi_json() -> &'static str {
        r#"[
            {
                "type": "function",
                "name": "transfer",
                "inputs": [
                    { "type": "address", "name": "to" },
                    { "type": "uint256", "name": "amount" }
                ],
                "outputs": [{ "type": "bool", "name": "" }],
                "stateMutability": "nonpayable"
            },
            {
                "type": "function",
                "name": "approve",
                "inputs": [
                    { "type": "address", "name": "spender" },
                    { "type": "uint256", "name": "amount" }
                ],
                "outputs": [{ "type": "bool", "name": "" }],
                "stateMutability": "nonpayable"
            },
            {
                "type": "function",
                "name": "balanceOf",
                "inputs": [{ "type": "address", "name": "account" }],
                "outputs": [{ "type": "uint256", "name": "" }],
                "stateMutability": "view"
            },
            {
                "type": "event",
                "name": "Transfer",
                "anonymous": false,
                "inputs": [
                    { "type": "address", "name": "from", "indexed": true },
                    { "type": "address", "name": "to", "indexed": true },
                    { "type": "uint256", "name": "value", "indexed": false }
                ]
            }
        ]"#
    }

    fn router_abi_json() -> &'static str {
        r#"[
            {
                "type": "function",
                "name": "swapExactTokensForTokens",
                "inputs": [
                    { "type": "uint256", "name": "amountIn" },
                    { "type": "uint256", "name": "amountOutMin" },
                    { "type": "address[]", "name": "path" },
                    { "type": "address", "name": "to" },
                    { "type": "uint256", "name": "deadline" }
                ],
                "outputs": [{ "type": "uint256[]", "name": "amounts" }],
                "stateMutability": "nonpayable"
            }
        ]"#
    }

    // === Registration ===

    #[test]
    fn test_register_abi_json() {
        let mut decoder = FunctionDecoder::new();
        decoder.register_abi_json("ERC20", erc20_abi_json()).unwrap();

        let functions = decoder.get_functions("ERC20").unwrap();
        assert_eq!(functions.len(), 3); // transfer, approve, balanceOf (event skipped)
    }

    #[test]
    fn test_register_empty_abi() {
        let mut decoder = FunctionDecoder::new();
        decoder.register_abi_json("Empty", "[]").unwrap();
        let functions = decoder.get_functions("Empty").unwrap();
        assert_eq!(functions.len(), 0);
    }

    #[test]
    fn test_register_invalid_json() {
        let mut decoder = FunctionDecoder::new();
        assert!(decoder.register_abi_json("Bad", "not json").is_err());
    }

    #[test]
    fn test_is_empty() {
        let decoder = FunctionDecoder::new();
        assert!(decoder.is_empty());

        let mut decoder = FunctionDecoder::new();
        decoder.register_abi_json("ERC20", erc20_abi_json()).unwrap();
        assert!(!decoder.is_empty());
    }

    // === Selector computation ===

    #[test]
    fn test_transfer_selector() {
        let mut decoder = FunctionDecoder::new();
        decoder.register_abi_json("ERC20", erc20_abi_json()).unwrap();

        let functions = decoder.get_functions("ERC20").unwrap();
        let transfer = functions.iter().find(|f| f.name == "transfer").unwrap();

        // transfer(address,uint256) → 0xa9059cbb
        assert_eq!(transfer.selector, "0xa9059cbb");
        assert_eq!(transfer.signature_string, "transfer(address,uint256)");
    }

    #[test]
    fn test_approve_selector() {
        let mut decoder = FunctionDecoder::new();
        decoder.register_abi_json("ERC20", erc20_abi_json()).unwrap();

        let functions = decoder.get_functions("ERC20").unwrap();
        let approve = functions.iter().find(|f| f.name == "approve").unwrap();

        // approve(address,uint256) → 0x095ea7b3
        assert_eq!(approve.selector, "0x095ea7b3");
    }

    #[test]
    fn test_balance_of_selector() {
        let mut decoder = FunctionDecoder::new();
        decoder.register_abi_json("ERC20", erc20_abi_json()).unwrap();

        let functions = decoder.get_functions("ERC20").unwrap();
        let balance_of = functions.iter().find(|f| f.name == "balanceOf").unwrap();

        // balanceOf(address) → 0x70a08231
        assert_eq!(balance_of.selector, "0x70a08231");
    }

    #[test]
    fn test_swap_selector() {
        let mut decoder = FunctionDecoder::new();
        decoder.register_abi_json("Router", router_abi_json()).unwrap();

        let functions = decoder.get_functions("Router").unwrap();
        let swap = &functions[0];

        // swapExactTokensForTokens(uint256,uint256,address[],address,uint256) → 0x38ed1739
        assert_eq!(swap.selector, "0x38ed1739");
    }

    // === Selector index lookup ===

    #[test]
    fn test_get_function_by_selector() {
        let mut decoder = FunctionDecoder::new();
        decoder.register_abi_json("ERC20", erc20_abi_json()).unwrap();

        let matches = decoder.get_function_by_selector("0xa9059cbb").unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].0, "ERC20");
        assert_eq!(matches[0].1.name, "transfer");
    }

    #[test]
    fn test_get_function_by_unknown_selector() {
        let decoder = FunctionDecoder::new();
        assert!(decoder.get_function_by_selector("0xdeadbeef").is_none());
    }

    #[test]
    fn test_all_selectors() {
        let mut decoder = FunctionDecoder::new();
        decoder.register_abi_json("ERC20", erc20_abi_json()).unwrap();

        let selectors = decoder.all_selectors();
        assert_eq!(selectors.len(), 3);
    }

    // === Function params ===

    #[test]
    fn test_function_inputs() {
        let mut decoder = FunctionDecoder::new();
        decoder.register_abi_json("ERC20", erc20_abi_json()).unwrap();

        let functions = decoder.get_functions("ERC20").unwrap();
        let transfer = functions.iter().find(|f| f.name == "transfer").unwrap();

        assert_eq!(transfer.inputs.len(), 2);
        assert_eq!(transfer.inputs[0], ("to".to_string(), "address".to_string()));
        assert_eq!(transfer.inputs[1], ("amount".to_string(), "uint256".to_string()));
    }

    #[test]
    fn test_function_outputs_unnamed() {
        let mut decoder = FunctionDecoder::new();
        decoder.register_abi_json("ERC20", erc20_abi_json()).unwrap();

        let functions = decoder.get_functions("ERC20").unwrap();
        let transfer = functions.iter().find(|f| f.name == "transfer").unwrap();

        assert_eq!(transfer.outputs.len(), 1);
        assert_eq!(transfer.outputs[0].0, "output_0"); // auto-named
        assert_eq!(transfer.outputs[0].1, "bool");
    }

    // === Decode trace ===

    #[test]
    fn test_decode_trace_transfer() {
        let mut decoder = FunctionDecoder::new();
        decoder.register_abi_json("ERC20", erc20_abi_json()).unwrap();

        let trace = RawTraceRecord {
            chain_id: 1,
            block_number: 18000000,
            tx_index: 5,
            trace_address: "0.1".to_string(),
            tx_hash: "0xdef".to_string(),
            block_hash: "0xabc".to_string(),
            block_timestamp: 1700000000,
            call_type: "call".to_string(),
            from_address: "0xaaaa".to_string(),
            to_address: "0xbbbb".to_string(),
            // transfer(to=0xcccc..., amount=1000000)
            input: format!(
                "0xa9059cbb{}{}",
                "000000000000000000000000cccccccccccccccccccccccccccccccccccccccc",
                "00000000000000000000000000000000000000000000000000000000000f4240",
            ),
            output: "0x0000000000000000000000000000000000000000000000000000000000000001".to_string(),
            value: "0".to_string(),
            gas: 100000,
            gas_used: 50000,
            error: None,
            function_name: None,
            function_sig: Some("0xa9059cbb".to_string()),
        };

        let decoded = decoder.decode_trace(&trace).unwrap();
        assert_eq!(decoded.table_name, "trace_e_r_c20_transfer");
        assert_eq!(decoded.function_name, "transfer");
        assert_eq!(decoded.input_columns, vec!["\"to\"", "amount"]);
        assert_eq!(
            decoded.input_values[0],
            "0xcccccccccccccccccccccccccccccccccccccccc"
        );
        assert_eq!(decoded.input_values[1], "1000000");
    }

    #[test]
    fn test_decode_trace_no_selector() {
        let decoder = FunctionDecoder::new();

        let trace = RawTraceRecord {
            chain_id: 1,
            block_number: 100,
            tx_index: 0,
            trace_address: "".to_string(),
            tx_hash: "0x".to_string(),
            block_hash: "0x".to_string(),
            block_timestamp: 0,
            call_type: "call".to_string(),
            from_address: "0xa".to_string(),
            to_address: "0xb".to_string(),
            input: "0x".to_string(),
            output: "0x".to_string(),
            value: "0".to_string(),
            gas: 0,
            gas_used: 0,
            error: None,
            function_name: None,
            function_sig: None,
        };

        assert!(decoder.decode_trace(&trace).is_none());
    }

    #[test]
    fn test_decode_trace_unknown_selector() {
        let mut decoder = FunctionDecoder::new();
        decoder.register_abi_json("ERC20", erc20_abi_json()).unwrap();

        let trace = RawTraceRecord {
            chain_id: 1,
            block_number: 100,
            tx_index: 0,
            trace_address: "".to_string(),
            tx_hash: "0x".to_string(),
            block_hash: "0x".to_string(),
            block_timestamp: 0,
            call_type: "call".to_string(),
            from_address: "0xa".to_string(),
            to_address: "0xb".to_string(),
            input: "0xdeadbeef".to_string(),
            output: "0x".to_string(),
            value: "0".to_string(),
            gas: 0,
            gas_used: 0,
            error: None,
            function_name: None,
            function_sig: Some("0xdeadbeef".to_string()),
        };

        assert!(decoder.decode_trace(&trace).is_none());
    }

    #[test]
    fn test_decode_trace_short_input() {
        let mut decoder = FunctionDecoder::new();
        decoder.register_abi_json("ERC20", erc20_abi_json()).unwrap();

        let trace = RawTraceRecord {
            chain_id: 1,
            block_number: 100,
            tx_index: 0,
            trace_address: "".to_string(),
            tx_hash: "0x".to_string(),
            block_hash: "0x".to_string(),
            block_timestamp: 0,
            call_type: "call".to_string(),
            from_address: "0xa".to_string(),
            to_address: "0xb".to_string(),
            // Only selector, no params
            input: "0xa9059cbb".to_string(),
            output: "0x".to_string(),
            value: "0".to_string(),
            gas: 0,
            gas_used: 0,
            error: None,
            function_name: None,
            function_sig: Some("0xa9059cbb".to_string()),
        };

        let decoded = decoder.decode_trace(&trace).unwrap();
        // Should decode but with null values
        assert_eq!(decoded.input_values, vec!["\\N", "\\N"]);
    }

    // === Table SQL generation ===

    #[test]
    fn test_generate_trace_table_sql() {
        let mut decoder = FunctionDecoder::new();
        decoder.register_abi_json("ERC20", erc20_abi_json()).unwrap();

        let functions = decoder.get_functions("ERC20").unwrap();
        let transfer = functions.iter().find(|f| f.name == "transfer").unwrap();

        let sql = decoder.generate_trace_table_sql("kyomei_data", "ERC20", transfer);

        assert!(sql.contains("CREATE TABLE IF NOT EXISTS kyomei_data.trace_e_r_c20_transfer"));
        assert!(sql.contains("chain_id INTEGER NOT NULL"));
        assert!(sql.contains("block_number BIGINT NOT NULL"));
        assert!(sql.contains("tx_index INTEGER NOT NULL"));
        assert!(sql.contains("trace_address TEXT NOT NULL"));
        assert!(sql.contains("from_address TEXT"));
        assert!(sql.contains("to_address TEXT"));
        assert!(sql.contains("\"to\" TEXT")); // function param, reserved keyword
        assert!(sql.contains("amount NUMERIC(78,0)")); // uint256
        assert!(sql.contains("PRIMARY KEY (chain_id, block_number, tx_index, trace_address)"));
    }

    #[test]
    fn test_generate_trace_table_sql_router() {
        let mut decoder = FunctionDecoder::new();
        decoder.register_abi_json("Router", router_abi_json()).unwrap();

        let functions = decoder.get_functions("Router").unwrap();
        let swap = &functions[0];

        let sql = decoder.generate_trace_table_sql("kyomei_data", "Router", swap);

        assert!(sql.contains("trace_router_swap_exact_tokens_for_tokens"));
        assert!(sql.contains("amount_in NUMERIC(78,0)"));
        assert!(sql.contains("amount_out_min NUMERIC(78,0)"));
        assert!(sql.contains("path JSONB")); // address[]
        assert!(sql.contains("\"to\" TEXT")); // reserved
        assert!(sql.contains("deadline NUMERIC(78,0)"));
    }

    #[test]
    fn test_nonexistent_contract_returns_none() {
        let decoder = FunctionDecoder::new();
        assert!(decoder.get_functions("NonExistent").is_none());
    }

    // === Same selector different contracts ===

    #[test]
    fn test_same_function_different_contracts() {
        let mut decoder = FunctionDecoder::new();
        decoder.register_abi_json("TokenA", erc20_abi_json()).unwrap();
        decoder.register_abi_json("TokenB", erc20_abi_json()).unwrap();

        let matches = decoder.get_function_by_selector("0xa9059cbb").unwrap();
        assert_eq!(matches.len(), 2);
    }
}
