use alloy_json_abi::{Event, JsonAbi};
use alloy_primitives::{keccak256, U256};
use anyhow::{Context, Result};
use std::collections::HashMap;
use tracing::{debug, trace};

use crate::types::RawEventRecord;

/// Parsed event info from an ABI
#[derive(Debug, Clone)]
pub struct ParsedEvent {
    /// The event name (e.g., "Transfer")
    pub name: String,
    /// The full signature string (e.g., "Transfer(address,address,uint256)")
    #[allow(dead_code)]
    pub signature_string: String,
    /// The keccak256 hash of the signature (topic0)
    pub signature: String,
    /// The event's ABI definition
    #[allow(dead_code)]
    pub event: Event,
    /// Indexed parameter names and Solidity types
    pub indexed_params: Vec<(String, String)>,
    /// Non-indexed parameter names and Solidity types
    pub data_params: Vec<(String, String)>,
}

/// Pre-computed decode metadata for a signature index entry.
/// Avoids recomputing table_name and param_columns on every decode call.
#[derive(Debug, Clone)]
pub struct SignatureEntry {
    pub contract_name: String,
    pub parsed: ParsedEvent,
    /// Pre-computed table name (e.g., "event_uniswap_v2_pair_swap")
    table_name: String,
    /// Pre-computed column names for decoded params
    param_columns: Vec<String>,
}

/// ABI decoder: loads ABI files, parses events, and computes topic0 signatures
pub struct AbiDecoder {
    /// contract_name → list of parsed events
    contracts: HashMap<String, Vec<ParsedEvent>>,
    /// topic0 → list of pre-computed signature entries for fast lookup
    signature_index: HashMap<String, Vec<SignatureEntry>>,
}

impl AbiDecoder {
    pub fn new() -> Self {
        Self {
            contracts: HashMap::new(),
            signature_index: HashMap::new(),
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

    /// Register a parsed ABI
    pub fn register_abi(&mut self, contract_name: &str, abi: &JsonAbi) -> Result<()> {
        let mut events = Vec::new();

        for (event_name, event_list) in abi.events.iter() {
            for event in event_list {
                let parsed = parse_event(event_name, event)?;

                // Pre-compute table_name and param_columns for fast decode
                let table_name = format!(
                    "event_{}_{}",
                    to_snake_case(contract_name),
                    to_snake_case(&parsed.name)
                );
                let param_columns: Vec<String> = parsed
                    .indexed_params
                    .iter()
                    .chain(parsed.data_params.iter())
                    .map(|(name, _)| quote_if_reserved(&to_snake_case(name)))
                    .collect();

                // Add to signature index with cached metadata
                self.signature_index
                    .entry(parsed.signature.clone())
                    .or_default()
                    .push(SignatureEntry {
                        contract_name: contract_name.to_string(),
                        parsed: parsed.clone(),
                        table_name,
                        param_columns,
                    });

                events.push(parsed);
            }
        }

        debug!(
            contract = contract_name,
            event_count = events.len(),
            "Registered ABI"
        );

        self.contracts
            .insert(contract_name.to_string(), events);

        Ok(())
    }

    /// Get all registered events for a contract
    pub fn get_events(&self, contract_name: &str) -> Option<&[ParsedEvent]> {
        self.contracts.get(contract_name).map(|v| v.as_slice())
    }

    /// Look up an event by topic0 signature
    pub fn get_event_by_signature(&self, topic0: &str) -> Option<&[SignatureEntry]> {
        self.signature_index.get(topic0).map(|v| v.as_slice())
    }

    /// Get all unique topic0 signatures across all registered contracts
    pub fn all_signatures(&self) -> Vec<&str> {
        self.signature_index.keys().map(|s| s.as_str()).collect()
    }

    /// Get a map of topic0 → event_name for all registered events
    pub fn signature_to_event_name(&self) -> HashMap<&str, &str> {
        self.signature_index
            .iter()
            .map(|(sig, entries)| (sig.as_str(), entries[0].parsed.name.as_str()))
            .collect()
    }

    /// Iterate over all registered contracts and their events
    pub fn all_contract_events(&self) -> impl Iterator<Item = (&str, &[ParsedEvent])> {
        self.contracts
            .iter()
            .map(|(name, events)| (name.as_str(), events.as_slice()))
    }

    /// Get the topic0 signatures for a specific contract
    pub fn contract_signatures(&self, contract_name: &str) -> Vec<String> {
        self.contracts
            .get(contract_name)
            .map(|events| events.iter().map(|e| e.signature.clone()).collect())
            .unwrap_or_default()
    }

    /// Map a Solidity type to a PostgreSQL column type
    pub fn solidity_to_pg_type(sol_type: &str) -> &'static str {
        // Check arrays and tuples first (before base type matches)
        if sol_type.ends_with(']') {
            return "JSONB";
        }
        if sol_type.starts_with('(') {
            return "JSONB";
        }

        match sol_type {
            // Address types
            "address" => "TEXT",

            // Boolean
            "bool" => "BOOLEAN",

            // Bytes types
            "bytes" => "TEXT",
            "bytes1" | "bytes2" | "bytes3" | "bytes4" | "bytes5" | "bytes6" | "bytes7"
            | "bytes8" | "bytes9" | "bytes10" | "bytes11" | "bytes12" | "bytes13" | "bytes14"
            | "bytes15" | "bytes16" | "bytes17" | "bytes18" | "bytes19" | "bytes20"
            | "bytes21" | "bytes22" | "bytes23" | "bytes24" | "bytes25" | "bytes26"
            | "bytes27" | "bytes28" | "bytes29" | "bytes30" | "bytes31" | "bytes32" => "TEXT",

            // String
            "string" => "TEXT",

            // Integer types (small enough to fit i32/i64)
            "int8" | "int16" | "int24" | "int32" | "uint8" | "uint16" | "uint24" => "INTEGER",
            "int40" | "int48" | "int56" | "int64" | "uint32" | "uint40" | "uint48"
            | "uint56" => "BIGINT",

            // Large integer types → NUMERIC for arbitrary precision
            _ if sol_type.starts_with("int") || sol_type.starts_with("uint") => "NUMERIC(78,0)",

            // Default fallback
            _ => "TEXT",
        }
    }

    /// Decode a raw event into a DecodedEventRow for insertion into the per-event table.
    /// Uses pre-computed table_name and param_columns from registration time.
    pub fn decode_raw_event(&self, event: &RawEventRecord) -> Option<DecodedEventRow> {
        let topic0 = event.topic0.as_deref()?;
        let matches = self.get_event_by_signature(topic0)?;
        if matches.is_empty() {
            return None;
        }
        // When multiple contracts share the same event signature (e.g., Transfer),
        // use the first match. Callers that need contract-specific disambiguation
        // should use decode_raw_event_for_contract instead.
        let entry = &matches[0];
        self.decode_with_entry(event, entry)
    }

    /// Decode a raw event for a specific contract name. Useful when the same event
    /// signature is registered by multiple contracts (e.g., ERC20 Transfer).
    /// Falls back to first match if no contract-specific match is found.
    pub fn decode_raw_event_for_contract(
        &self,
        event: &RawEventRecord,
        contract_name: &str,
    ) -> Option<DecodedEventRow> {
        let topic0 = event.topic0.as_deref()?;
        let matches = self.get_event_by_signature(topic0)?;
        if matches.is_empty() {
            return None;
        }
        let entry = matches
            .iter()
            .find(|e| e.contract_name == contract_name)
            .unwrap_or(&matches[0]);
        self.decode_with_entry(event, entry)
    }

    /// Decode a raw event directly into COPY-format bytes, skipping the DecodedEventRow
    /// intermediary. Returns (table_name, param_columns, encoded_row_bytes) borrowed from
    /// the cached SignatureEntry. This eliminates 7+ String clones per event.
    ///
    /// When multiple contracts share the same event signature, emits a row for each
    /// matching contract so events land in all relevant decoded tables.
    pub fn decode_raw_event_to_copy_rows<'a>(
        &'a self,
        event: &RawEventRecord,
    ) -> Vec<(&'a str, &'a [String], Vec<u8>)> {
        let topic0 = match event.topic0.as_deref() {
            Some(t) => t,
            None => return Vec::new(),
        };
        let matches = match self.get_event_by_signature(topic0) {
            Some(m) if !m.is_empty() => m,
            _ => return Vec::new(),
        };

        let mut results = Vec::with_capacity(matches.len());
        for entry in matches {
            if let Some(row) = self.encode_copy_row_for_entry(event, entry) {
                results.push((&*entry.table_name, entry.param_columns.as_slice(), row));
            }
        }
        results
    }

    /// Backwards-compatible single-match version. Picks the first matching contract.
    pub fn decode_raw_event_to_copy_row<'a>(
        &'a self,
        event: &RawEventRecord,
    ) -> Option<(&'a str, &'a [String], Vec<u8>)> {
        let topic0 = event.topic0.as_deref()?;
        let matches = self.get_event_by_signature(topic0)?;
        if matches.is_empty() {
            return None;
        }
        let entry = &matches[0];
        self.encode_copy_row_for_entry(event, entry)
            .map(|row| (&*entry.table_name, entry.param_columns.as_slice(), row))
    }

    /// Internal: decode a raw event using a specific SignatureEntry, returning a DecodedEventRow.
    fn decode_with_entry(&self, event: &RawEventRecord, entry: &SignatureEntry) -> Option<DecodedEventRow> {
        let parsed = &entry.parsed;
        let mut param_values = Vec::with_capacity(entry.param_columns.len());

        // Decode indexed params from topics (always static 32-byte words)
        let topics = [&event.topic1, &event.topic2, &event.topic3];
        for (i, (_name, sol_type)) in parsed.indexed_params.iter().enumerate() {
            let value = topics
                .get(i)
                .and_then(|t| t.as_deref())
                .map(|t| decode_value(t, sol_type))
                .unwrap_or_else(|| "\\N".to_string());
            param_values.push(value);
        }

        // Decode non-indexed params from data (handles both static and dynamic types)
        let data_hex = event.data.strip_prefix("0x").unwrap_or(&event.data);
        let data_values = decode_data_params(data_hex, &parsed.data_params);
        param_values.extend(data_values);

        Some(DecodedEventRow {
            table_name: entry.table_name.clone(),
            contract_name: entry.contract_name.clone(),
            event_name: parsed.name.clone(),
            param_columns: entry.param_columns.clone(),
            chain_id: event.chain_id,
            block_number: event.block_number,
            tx_index: event.tx_index,
            log_index: event.log_index,
            block_hash: event.block_hash.clone(),
            block_timestamp: event.block_timestamp,
            tx_hash: event.tx_hash.clone(),
            address: event.address.clone(),
            param_values,
        })
    }

    /// Internal: encode a COPY-format row for a specific SignatureEntry.
    fn encode_copy_row_for_entry(&self, event: &RawEventRecord, entry: &SignatureEntry) -> Option<Vec<u8>> {
        let parsed = &entry.parsed;
        let mut row = Vec::with_capacity(256);

        use std::io::Write;
        write!(row, "{}", event.chain_id).unwrap();
        row.push(b'\t');
        write!(row, "{}", event.block_number).unwrap();
        row.push(b'\t');
        write!(row, "{}", event.tx_index).unwrap();
        row.push(b'\t');
        write!(row, "{}", event.log_index).unwrap();
        row.push(b'\t');
        row.extend_from_slice(event.block_hash.as_bytes());
        row.push(b'\t');
        write!(row, "{}", event.block_timestamp).unwrap();
        row.push(b'\t');
        row.extend_from_slice(event.tx_hash.as_bytes());
        row.push(b'\t');
        row.extend_from_slice(event.address.as_bytes());

        // Decode indexed params from topics (always static 32-byte words)
        let topics = [&event.topic1, &event.topic2, &event.topic3];
        for (i, (_name, sol_type)) in parsed.indexed_params.iter().enumerate() {
            row.push(b'\t');
            let value = topics
                .get(i)
                .and_then(|t| t.as_deref())
                .map(|t| decode_value(t, sol_type))
                .unwrap_or_else(|| "\\N".to_string());
            row.extend_from_slice(value.as_bytes());
        }

        // Decode non-indexed params from data (handles both static and dynamic types)
        let data_hex = event.data.strip_prefix("0x").unwrap_or(&event.data);
        let data_values = decode_data_params(data_hex, &parsed.data_params);
        for value in &data_values {
            row.push(b'\t');
            row.extend_from_slice(value.as_bytes());
        }

        row.push(b'\n');
        Some(row)
    }

    /// Generate CREATE TABLE SQL for a per-event decoded table
    pub fn generate_event_table_sql(
        &self,
        schema: &str,
        contract_name: &str,
        event: &ParsedEvent,
    ) -> String {
        let table_name = format!(
            "event_{}_{}",
            to_snake_case(contract_name),
            to_snake_case(&event.name)
        );

        let mut columns = vec![
            "chain_id INTEGER NOT NULL".to_string(),
            "block_number BIGINT NOT NULL".to_string(),
            "tx_index INTEGER NOT NULL".to_string(),
            "log_index INTEGER NOT NULL".to_string(),
            "block_hash TEXT NOT NULL".to_string(),
            "block_timestamp BIGINT NOT NULL".to_string(),
            "tx_hash TEXT NOT NULL".to_string(),
            "address TEXT NOT NULL".to_string(),
        ];

        // Add decoded indexed params (from topics)
        for (name, sol_type) in &event.indexed_params {
            let pg_type = Self::solidity_to_pg_type(sol_type);
            let col_name = to_snake_case(name);
            // Quote reserved SQL keywords
            let quoted = quote_if_reserved(&col_name);
            columns.push(format!("{} {}", quoted, pg_type));
        }

        // Add decoded non-indexed params (from data)
        for (name, sol_type) in &event.data_params {
            let pg_type = Self::solidity_to_pg_type(sol_type);
            let col_name = to_snake_case(name);
            let quoted = quote_if_reserved(&col_name);
            columns.push(format!("{} {}", quoted, pg_type));
        }

        let columns_sql = columns.join(",\n            ");

        format!(
            r#"CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
            {columns_sql},
            PRIMARY KEY (chain_id, block_number, tx_index, log_index)
        )"#
        )
    }
}

/// Decoded event row ready for insertion into a per-event table
#[derive(Debug, Clone)]
pub struct DecodedEventRow {
    /// Target table name (e.g., "event_uniswap_v2_pair_swap")
    pub table_name: String,
    /// Original contract name (e.g., "UniswapV2Pair")
    pub contract_name: String,
    /// Original event name (e.g., "Swap")
    pub event_name: String,
    /// Decoded parameter column names (in order)
    pub param_columns: Vec<String>,
    pub chain_id: i32,
    pub block_number: i64,
    pub tx_index: i32,
    pub log_index: i32,
    pub block_hash: String,
    pub block_timestamp: i64,
    pub tx_hash: String,
    pub address: String,
    /// Decoded parameter values as strings (matching param_columns order)
    pub param_values: Vec<String>,
}

/// Returns true if a Solidity type is dynamic (ABI-encoded with an offset pointer).
/// Dynamic types: string, bytes, T[] (dynamic arrays), and tuples containing dynamic types.
fn is_dynamic_type(sol_type: &str) -> bool {
    sol_type == "string"
        || sol_type == "bytes"
        || sol_type.ends_with("[]")
        || sol_type.starts_with('(')
}

/// Decode non-indexed data params from hex-encoded event data, handling both static
/// and dynamic ABI types correctly. Static types appear as 32-byte words at position
/// `i * 32`. Dynamic types have an offset pointer at that position, pointing to the
/// actual data further in the payload.
fn decode_data_params(data_hex: &str, data_params: &[(String, String)]) -> Vec<String> {
    let mut values = Vec::with_capacity(data_params.len());
    for (i, (_name, sol_type)) in data_params.iter().enumerate() {
        let head_offset = i * 64; // each slot is 32 bytes = 64 hex chars
        if data_hex.len() < head_offset + 64 {
            values.push("\\N".to_string());
            continue;
        }
        let head_word = &data_hex[head_offset..head_offset + 64];

        if is_dynamic_type(sol_type) {
            // Dynamic type: head_word is an offset (in bytes) from start of data
            let byte_offset = match u64::from_str_radix(head_word.trim_start_matches('0').max("0"), 16) {
                Ok(v) => v as usize,
                Err(_) => {
                    values.push(format!("0x{}", head_word));
                    continue;
                }
            };
            let hex_offset = byte_offset * 2;

            if sol_type == "string" {
                // string: offset → 32-byte length word → UTF-8 data
                if data_hex.len() >= hex_offset + 64 {
                    let len_word = &data_hex[hex_offset..hex_offset + 64];
                    let byte_len = u64::from_str_radix(len_word.trim_start_matches('0').max("0"), 16)
                        .unwrap_or(0) as usize;
                    let str_hex_start = hex_offset + 64;
                    let str_hex_end = str_hex_start + byte_len * 2;
                    if data_hex.len() >= str_hex_end {
                        let str_hex = &data_hex[str_hex_start..str_hex_end];
                        match hex::decode(str_hex) {
                            Ok(bytes) => match String::from_utf8(bytes) {
                                Ok(s) => {
                                    // Escape COPY-special characters
                                    let escaped = s.replace('\\', "\\\\")
                                        .replace('\t', "\\t")
                                        .replace('\n', "\\n")
                                        .replace('\r', "\\r");
                                    values.push(escaped);
                                }
                                Err(_) => values.push(format!("0x{}", str_hex)),
                            },
                            Err(_) => values.push(format!("0x{}", str_hex)),
                        }
                    } else {
                        values.push("\\N".to_string());
                    }
                } else {
                    values.push("\\N".to_string());
                }
            } else {
                // bytes, arrays, tuples: store as hex blob from offset onward
                if data_hex.len() > hex_offset {
                    // Read the length word, then the raw data
                    if data_hex.len() >= hex_offset + 64 {
                        let len_word = &data_hex[hex_offset..hex_offset + 64];
                        let byte_len = u64::from_str_radix(len_word.trim_start_matches('0').max("0"), 16)
                            .unwrap_or(0) as usize;
                        let data_start = hex_offset + 64;
                        let data_end = (data_start + byte_len * 2).min(data_hex.len());
                        values.push(format!("0x{}", &data_hex[data_start..data_end]));
                    } else {
                        values.push(format!("0x{}", &data_hex[hex_offset..]));
                    }
                } else {
                    values.push("\\N".to_string());
                }
            }
        } else {
            // Static type: decode directly from the 32-byte word
            values.push(decode_value_raw(head_word, sol_type));
        }
    }
    values
}

/// Decode a 32-byte hex word into a string value based on the Solidity type
/// Decode a hex word (with or without 0x prefix) into a string value.
pub fn decode_value(hex_word: &str, sol_type: &str) -> String {
    let hex = hex_word.strip_prefix("0x").unwrap_or(hex_word);
    decode_value_raw(hex, sol_type)
}

/// Decode a raw hex string (WITHOUT 0x prefix) into a string value.
/// Use this when you already have the hex without prefix to avoid an intermediate allocation.
pub fn decode_value_raw(hex: &str, sol_type: &str) -> String {
    if hex.is_empty() {
        return "\\N".to_string();
    }

    match sol_type {
        "address" => {
            if hex.len() >= 40 {
                format!("0x{}", &hex[hex.len() - 40..].to_lowercase())
            } else {
                format!("0x{}", hex.to_lowercase())
            }
        }
        "bool" => {
            let last_char = hex.chars().last().unwrap_or('0');
            if last_char == '1' {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        "string" | "bytes" => {
            format!("0x{}", hex)
        }
        t if t.starts_with("uint") => {
            let trimmed = hex.trim_start_matches('0');
            if trimmed.is_empty() {
                "0".to_string()
            } else if trimmed.len() <= 32 {
                match u128::from_str_radix(trimmed, 16) {
                    Ok(val) => val.to_string(),
                    Err(_) => format!("0x{}", hex),
                }
            } else {
                match U256::from_str_radix(hex, 16) {
                    Ok(val) => val.to_string(),
                    Err(_) => format!("0x{}", hex),
                }
            }
        },
        t if t.starts_with("int") => decode_signed_int(hex, t),
        t if t.starts_with("bytes") => {
            let byte_count = t[5..].parse::<usize>().unwrap_or(32);
            let char_count = byte_count * 2;
            if hex.len() >= char_count {
                format!("0x{}", &hex[..char_count].to_lowercase())
            } else {
                format!("0x{}", hex.to_lowercase())
            }
        }
        _ => {
            format!("0x{}", hex)
        }
    }
}

/// Decode a signed integer from a hex word
fn decode_signed_int(hex: &str, sol_type: &str) -> String {
    let val = match U256::from_str_radix(hex, 16) {
        Ok(v) => v,
        Err(_) => return format!("0x{}", hex),
    };

    let bit_width: usize = sol_type[3..].parse().unwrap_or(256);
    if bit_width == 0 || bit_width > 256 {
        return val.to_string();
    }

    let sign_bit = U256::from(1) << (bit_width - 1);

    if val & sign_bit != U256::ZERO {
        // Negative: two's complement
        if bit_width == 256 {
            let abs = (!val).wrapping_add(U256::from(1));
            format!("-{}", abs)
        } else {
            let modulus = U256::from(1) << bit_width;
            let abs = modulus - val;
            format!("-{}", abs)
        }
    } else {
        val.to_string()
    }
}

/// Parse a single event from the ABI into a ParsedEvent
fn parse_event(name: &str, event: &Event) -> Result<ParsedEvent> {
    let mut indexed_params = Vec::new();
    let mut data_params = Vec::new();

    for input in &event.inputs {
        let param_name = input.name.clone();
        let param_type = input.ty.clone();

        if input.indexed {
            indexed_params.push((param_name, param_type));
        } else {
            data_params.push((param_name, param_type));
        }
    }

    // Use alloy's built-in signature computation
    let sig_string = event.signature();
    let hash = keccak256(sig_string.as_bytes());
    let signature = format!("0x{}", hex::encode(hash));

    trace!(
        event = name,
        signature = %signature,
        sig_string = %sig_string,
        "Parsed event"
    );

    Ok(ParsedEvent {
        name: name.to_string(),
        signature_string: sig_string,
        signature,
        event: event.clone(),
        indexed_params,
        data_params,
    })
}


/// Convert PascalCase/camelCase to snake_case
pub fn to_snake_case(s: &str) -> String {
    let mut result = String::with_capacity(s.len() + 4);
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() {
            if i > 0 {
                result.push('_');
            }
            result.push(ch.to_lowercase().next().unwrap());
        } else {
            result.push(ch);
        }
    }
    result
}

/// Quote a column name if it's a SQL reserved keyword
pub fn quote_if_reserved(name: &str) -> String {
    const RESERVED: &[&str] = &[
        "to", "from", "order", "select", "insert", "update", "delete", "table", "index",
        "create", "drop", "alter", "column", "key", "primary", "foreign", "references",
        "constraint", "check", "default", "null", "not", "and", "or", "in", "is", "like",
        "between", "exists", "case", "when", "then", "else", "end", "as", "on", "join",
        "left", "right", "inner", "outer", "cross", "group", "by", "having", "limit",
        "offset", "union", "all", "distinct", "into", "values", "set", "where", "type",
        "user", "value", "data", "timestamp",
    ];

    if RESERVED.contains(&name.to_lowercase().as_str()) {
        format!("\"{}\"", name)
    } else {
        name.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_abi_json() -> &'static str {
        r#"[
            {
                "type": "event",
                "name": "Transfer",
                "anonymous": false,
                "inputs": [
                    { "type": "address", "name": "from", "indexed": true },
                    { "type": "address", "name": "to", "indexed": true },
                    { "type": "uint256", "name": "value", "indexed": false }
                ]
            },
            {
                "type": "event",
                "name": "Approval",
                "anonymous": false,
                "inputs": [
                    { "type": "address", "name": "owner", "indexed": true },
                    { "type": "address", "name": "spender", "indexed": true },
                    { "type": "uint256", "name": "value", "indexed": false }
                ]
            },
            {
                "type": "function",
                "name": "transfer",
                "inputs": [
                    { "type": "address", "name": "to" },
                    { "type": "uint256", "name": "amount" }
                ],
                "outputs": [{ "type": "bool" }],
                "stateMutability": "nonpayable"
            }
        ]"#
    }

    fn uniswap_factory_abi_json() -> &'static str {
        r#"[
            {
                "type": "event",
                "name": "PairCreated",
                "anonymous": false,
                "inputs": [
                    { "type": "address", "name": "token0", "indexed": true },
                    { "type": "address", "name": "token1", "indexed": true },
                    { "type": "address", "name": "pair", "indexed": false },
                    { "type": "uint256", "name": "pairIndex", "indexed": false }
                ]
            }
        ]"#
    }

    fn uniswap_pair_abi_json() -> &'static str {
        r#"[
            {
                "type": "event",
                "name": "Swap",
                "anonymous": false,
                "inputs": [
                    { "type": "address", "name": "sender", "indexed": true },
                    { "type": "uint256", "name": "amount0In", "indexed": false },
                    { "type": "uint256", "name": "amount1In", "indexed": false },
                    { "type": "uint256", "name": "amount0Out", "indexed": false },
                    { "type": "uint256", "name": "amount1Out", "indexed": false },
                    { "type": "address", "name": "to", "indexed": true }
                ]
            },
            {
                "type": "event",
                "name": "Sync",
                "anonymous": false,
                "inputs": [
                    { "type": "uint112", "name": "reserve0", "indexed": false },
                    { "type": "uint112", "name": "reserve1", "indexed": false }
                ]
            },
            {
                "type": "event",
                "name": "Mint",
                "anonymous": false,
                "inputs": [
                    { "type": "address", "name": "sender", "indexed": true },
                    { "type": "uint256", "name": "amount0", "indexed": false },
                    { "type": "uint256", "name": "amount1", "indexed": false }
                ]
            },
            {
                "type": "event",
                "name": "Burn",
                "anonymous": false,
                "inputs": [
                    { "type": "address", "name": "sender", "indexed": true },
                    { "type": "uint256", "name": "amount0", "indexed": false },
                    { "type": "uint256", "name": "amount1", "indexed": false },
                    { "type": "address", "name": "to", "indexed": true }
                ]
            }
        ]"#
    }

    #[test]
    fn test_register_and_get_events() {
        let mut decoder = AbiDecoder::new();
        decoder
            .register_abi_json("ERC20", sample_abi_json())
            .unwrap();

        let events = decoder.get_events("ERC20").unwrap();
        assert_eq!(events.len(), 2); // Transfer + Approval (function is skipped)
        assert_eq!(events[0].name, "Approval");
        assert_eq!(events[1].name, "Transfer");
    }

    #[test]
    fn test_transfer_signature() {
        let mut decoder = AbiDecoder::new();
        decoder
            .register_abi_json("ERC20", sample_abi_json())
            .unwrap();

        let events = decoder.get_events("ERC20").unwrap();
        let transfer = events.iter().find(|e| e.name == "Transfer").unwrap();

        // Transfer(address,address,uint256) keccak256
        assert_eq!(transfer.signature_string, "Transfer(address,address,uint256)");
        assert_eq!(
            transfer.signature,
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        );
    }

    #[test]
    fn test_event_params_classification() {
        let mut decoder = AbiDecoder::new();
        decoder
            .register_abi_json("ERC20", sample_abi_json())
            .unwrap();

        let events = decoder.get_events("ERC20").unwrap();
        let transfer = events.iter().find(|e| e.name == "Transfer").unwrap();

        assert_eq!(transfer.indexed_params.len(), 2);
        assert_eq!(transfer.indexed_params[0], ("from".to_string(), "address".to_string()));
        assert_eq!(transfer.indexed_params[1], ("to".to_string(), "address".to_string()));

        assert_eq!(transfer.data_params.len(), 1);
        assert_eq!(transfer.data_params[0], ("value".to_string(), "uint256".to_string()));
    }

    #[test]
    fn test_signature_index_lookup() {
        let mut decoder = AbiDecoder::new();
        decoder
            .register_abi_json("ERC20", sample_abi_json())
            .unwrap();

        let transfer_sig = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
        let matches = decoder.get_event_by_signature(transfer_sig).unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].contract_name, "ERC20");
        assert_eq!(matches[0].parsed.name, "Transfer");
    }

    #[test]
    fn test_pair_created_signature() {
        let mut decoder = AbiDecoder::new();
        decoder
            .register_abi_json("UniswapV2Factory", uniswap_factory_abi_json())
            .unwrap();

        let events = decoder.get_events("UniswapV2Factory").unwrap();
        let pair_created = &events[0];

        assert_eq!(pair_created.signature_string, "PairCreated(address,address,address,uint256)");
        // Well-known PairCreated topic0
        assert_eq!(
            pair_created.signature,
            "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
        );
    }

    #[test]
    fn test_multiple_contracts() {
        let mut decoder = AbiDecoder::new();
        decoder
            .register_abi_json("UniswapV2Factory", uniswap_factory_abi_json())
            .unwrap();
        decoder
            .register_abi_json("UniswapV2Pair", uniswap_pair_abi_json())
            .unwrap();

        // Factory has 1 event
        assert_eq!(decoder.get_events("UniswapV2Factory").unwrap().len(), 1);
        // Pair has 4 events
        assert_eq!(decoder.get_events("UniswapV2Pair").unwrap().len(), 4);

        // All signatures should be unique
        let all_sigs = decoder.all_signatures();
        assert_eq!(all_sigs.len(), 5); // PairCreated + Swap + Sync + Mint + Burn
    }

    #[test]
    fn test_solidity_to_pg_type_mapping() {
        assert_eq!(AbiDecoder::solidity_to_pg_type("address"), "TEXT");
        assert_eq!(AbiDecoder::solidity_to_pg_type("bool"), "BOOLEAN");
        assert_eq!(AbiDecoder::solidity_to_pg_type("uint256"), "NUMERIC(78,0)");
        assert_eq!(AbiDecoder::solidity_to_pg_type("uint8"), "INTEGER");
        assert_eq!(AbiDecoder::solidity_to_pg_type("uint32"), "BIGINT");
        assert_eq!(AbiDecoder::solidity_to_pg_type("int64"), "BIGINT");
        assert_eq!(AbiDecoder::solidity_to_pg_type("bytes32"), "TEXT");
        assert_eq!(AbiDecoder::solidity_to_pg_type("string"), "TEXT");
        assert_eq!(AbiDecoder::solidity_to_pg_type("bytes"), "TEXT");
        assert_eq!(AbiDecoder::solidity_to_pg_type("uint256[]"), "JSONB");
    }

    #[test]
    fn test_generate_swap_table_sql() {
        let mut decoder = AbiDecoder::new();
        decoder
            .register_abi_json("UniswapV2Pair", uniswap_pair_abi_json())
            .unwrap();

        let events = decoder.get_events("UniswapV2Pair").unwrap();
        let swap = events.iter().find(|e| e.name == "Swap").unwrap();

        let sql = decoder.generate_event_table_sql("kyomei_data", "UniswapV2Pair", swap);

        assert!(sql.contains("CREATE TABLE IF NOT EXISTS kyomei_data.event_uniswap_v2_pair_swap"));
        assert!(sql.contains("chain_id INTEGER NOT NULL"));
        assert!(sql.contains("block_number BIGINT NOT NULL"));
        assert!(sql.contains("sender TEXT")); // indexed address
        assert!(sql.contains("\"to\" TEXT")); // indexed address, quoted (reserved keyword)
        assert!(sql.contains("amount0_in NUMERIC(78,0)")); // uint256
        assert!(sql.contains("amount1_out NUMERIC(78,0)")); // uint256
        assert!(sql.contains("PRIMARY KEY (chain_id, block_number, tx_index, log_index)"));
    }

    #[test]
    fn test_quote_reserved_keywords() {
        assert_eq!(quote_if_reserved("sender"), "sender");
        assert_eq!(quote_if_reserved("to"), "\"to\"");
        assert_eq!(quote_if_reserved("from"), "\"from\"");
        assert_eq!(quote_if_reserved("order"), "\"order\"");
        assert_eq!(quote_if_reserved("value"), "\"value\"");
        assert_eq!(quote_if_reserved("amount"), "amount");
    }

    #[test]
    fn test_contract_signatures() {
        let mut decoder = AbiDecoder::new();
        decoder
            .register_abi_json("UniswapV2Pair", uniswap_pair_abi_json())
            .unwrap();

        let sigs = decoder.contract_signatures("UniswapV2Pair");
        assert_eq!(sigs.len(), 4);
        // All should be valid hex strings
        for sig in &sigs {
            assert!(sig.starts_with("0x"));
            assert_eq!(sig.len(), 66); // "0x" + 64 hex chars
        }
    }

    #[test]
    fn test_to_snake_case() {
        assert_eq!(to_snake_case("PairCreated"), "pair_created");
        assert_eq!(to_snake_case("Transfer"), "transfer");
        assert_eq!(to_snake_case("amount0In"), "amount0_in");
        assert_eq!(to_snake_case("UniswapV2Factory"), "uniswap_v2_factory");
        assert_eq!(to_snake_case("already_snake"), "already_snake");
    }

    // === Additional ABI edge case tests ===

    #[test]
    fn test_empty_abi_json() {
        let mut decoder = AbiDecoder::new();
        let result = decoder.register_abi_json("Empty", "[]");
        assert!(result.is_ok());
        let events = decoder.get_events("Empty").unwrap();
        assert_eq!(events.len(), 0);
    }

    #[test]
    fn test_invalid_abi_json() {
        let mut decoder = AbiDecoder::new();
        let result = decoder.register_abi_json("Bad", "not json");
        assert!(result.is_err());
    }

    #[test]
    fn test_abi_functions_only_no_events() {
        let mut decoder = AbiDecoder::new();
        let result = decoder.register_abi_json(
            "FunctionsOnly",
            r#"[{
                "type": "function",
                "name": "balanceOf",
                "inputs": [{ "type": "address", "name": "account" }],
                "outputs": [{ "type": "uint256" }],
                "stateMutability": "view"
            }]"#,
        );
        assert!(result.is_ok());
        let events = decoder.get_events("FunctionsOnly").unwrap();
        assert_eq!(events.len(), 0);
    }

    #[test]
    fn test_nonexistent_contract_returns_none() {
        let decoder = AbiDecoder::new();
        assert!(decoder.get_events("NonExistent").is_none());
    }

    #[test]
    fn test_nonexistent_signature_returns_none() {
        let decoder = AbiDecoder::new();
        assert!(decoder.get_event_by_signature("0xdeadbeef").is_none());
    }

    #[test]
    fn test_contract_signatures_nonexistent_returns_empty() {
        let decoder = AbiDecoder::new();
        assert!(decoder.contract_signatures("NonExistent").is_empty());
    }

    #[test]
    fn test_signature_to_event_name_map() {
        let mut decoder = AbiDecoder::new();
        decoder.register_abi_json("ERC20", sample_abi_json()).unwrap();
        let map = decoder.signature_to_event_name();
        assert_eq!(map.len(), 2);
        // Check that Transfer signature maps to "Transfer"
        let transfer_sig = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
        assert_eq!(map.get(transfer_sig), Some(&"Transfer"));
    }

    #[test]
    fn test_solidity_to_pg_type_int_variants() {
        assert_eq!(AbiDecoder::solidity_to_pg_type("int8"), "INTEGER");
        assert_eq!(AbiDecoder::solidity_to_pg_type("int16"), "INTEGER");
        assert_eq!(AbiDecoder::solidity_to_pg_type("int24"), "INTEGER");
        assert_eq!(AbiDecoder::solidity_to_pg_type("int32"), "INTEGER");
        assert_eq!(AbiDecoder::solidity_to_pg_type("int40"), "BIGINT");
        assert_eq!(AbiDecoder::solidity_to_pg_type("int48"), "BIGINT");
        assert_eq!(AbiDecoder::solidity_to_pg_type("int56"), "BIGINT");
        assert_eq!(AbiDecoder::solidity_to_pg_type("int64"), "BIGINT");
    }

    #[test]
    fn test_solidity_to_pg_type_uint_variants() {
        assert_eq!(AbiDecoder::solidity_to_pg_type("uint8"), "INTEGER");
        assert_eq!(AbiDecoder::solidity_to_pg_type("uint16"), "INTEGER");
        assert_eq!(AbiDecoder::solidity_to_pg_type("uint24"), "INTEGER");
        assert_eq!(AbiDecoder::solidity_to_pg_type("uint32"), "BIGINT");
        assert_eq!(AbiDecoder::solidity_to_pg_type("uint40"), "BIGINT");
        assert_eq!(AbiDecoder::solidity_to_pg_type("uint48"), "BIGINT");
        assert_eq!(AbiDecoder::solidity_to_pg_type("uint56"), "BIGINT");
    }

    #[test]
    fn test_solidity_to_pg_type_large_ints() {
        assert_eq!(AbiDecoder::solidity_to_pg_type("uint128"), "NUMERIC(78,0)");
        assert_eq!(AbiDecoder::solidity_to_pg_type("uint256"), "NUMERIC(78,0)");
        assert_eq!(AbiDecoder::solidity_to_pg_type("int128"), "NUMERIC(78,0)");
        assert_eq!(AbiDecoder::solidity_to_pg_type("int256"), "NUMERIC(78,0)");
        assert_eq!(AbiDecoder::solidity_to_pg_type("uint112"), "NUMERIC(78,0)");
    }

    #[test]
    fn test_solidity_to_pg_type_bytes_variants() {
        assert_eq!(AbiDecoder::solidity_to_pg_type("bytes1"), "TEXT");
        assert_eq!(AbiDecoder::solidity_to_pg_type("bytes4"), "TEXT");
        assert_eq!(AbiDecoder::solidity_to_pg_type("bytes16"), "TEXT");
        assert_eq!(AbiDecoder::solidity_to_pg_type("bytes20"), "TEXT");
        assert_eq!(AbiDecoder::solidity_to_pg_type("bytes32"), "TEXT");
    }

    #[test]
    fn test_solidity_to_pg_type_array_types() {
        assert_eq!(AbiDecoder::solidity_to_pg_type("uint256[]"), "JSONB");
        assert_eq!(AbiDecoder::solidity_to_pg_type("address[]"), "JSONB");
        assert_eq!(AbiDecoder::solidity_to_pg_type("bytes32[]"), "JSONB");
        assert_eq!(AbiDecoder::solidity_to_pg_type("uint256[3]"), "JSONB");
    }

    #[test]
    fn test_solidity_to_pg_type_tuple() {
        assert_eq!(AbiDecoder::solidity_to_pg_type("(uint256,address)"), "JSONB");
        assert_eq!(AbiDecoder::solidity_to_pg_type("(bool,string)"), "JSONB");
    }

    #[test]
    fn test_solidity_to_pg_type_unknown_defaults_to_text() {
        assert_eq!(AbiDecoder::solidity_to_pg_type("custom_type"), "TEXT");
    }

    #[test]
    fn test_quote_if_reserved_more_keywords() {
        assert_eq!(quote_if_reserved("select"), "\"select\"");
        assert_eq!(quote_if_reserved("table"), "\"table\"");
        assert_eq!(quote_if_reserved("index"), "\"index\"");
        assert_eq!(quote_if_reserved("type"), "\"type\"");
        assert_eq!(quote_if_reserved("user"), "\"user\"");
        assert_eq!(quote_if_reserved("data"), "\"data\"");
        assert_eq!(quote_if_reserved("timestamp"), "\"timestamp\"");
    }

    #[test]
    fn test_quote_if_reserved_non_reserved() {
        assert_eq!(quote_if_reserved("sender"), "sender");
        assert_eq!(quote_if_reserved("amount0_in"), "amount0_in");
        assert_eq!(quote_if_reserved("pair_index"), "pair_index");
        assert_eq!(quote_if_reserved("reserve0"), "reserve0");
    }

    #[test]
    fn test_generate_sync_table_sql() {
        let mut decoder = AbiDecoder::new();
        decoder
            .register_abi_json("UniswapV2Pair", uniswap_pair_abi_json())
            .unwrap();

        let events = decoder.get_events("UniswapV2Pair").unwrap();
        let sync = events.iter().find(|e| e.name == "Sync").unwrap();

        let sql = decoder.generate_event_table_sql("kyomei_data", "UniswapV2Pair", sync);

        assert!(sql.contains("event_uniswap_v2_pair_sync"));
        assert!(sql.contains("reserve0 NUMERIC(78,0)"));
        assert!(sql.contains("reserve1 NUMERIC(78,0)"));
    }

    #[test]
    fn test_generate_pair_created_table_sql() {
        let mut decoder = AbiDecoder::new();
        decoder
            .register_abi_json("UniswapV2Factory", uniswap_factory_abi_json())
            .unwrap();

        let events = decoder.get_events("UniswapV2Factory").unwrap();
        let pair_created = &events[0];

        let sql = decoder.generate_event_table_sql("kyomei_data", "UniswapV2Factory", pair_created);

        assert!(sql.contains("event_uniswap_v2_factory_pair_created"));
        assert!(sql.contains("token0 TEXT")); // indexed address
        assert!(sql.contains("token1 TEXT")); // indexed address
        assert!(sql.contains("pair TEXT")); // non-indexed address
        assert!(sql.contains("pair_index NUMERIC(78,0)")); // uint256
    }

    #[test]
    fn test_same_event_different_contracts() {
        let mut decoder = AbiDecoder::new();
        decoder.register_abi_json("TokenA", sample_abi_json()).unwrap();
        decoder.register_abi_json("TokenB", sample_abi_json()).unwrap();

        // Transfer signature should map to both contracts
        let transfer_sig = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
        let matches = decoder.get_event_by_signature(transfer_sig).unwrap();
        assert_eq!(matches.len(), 2);
    }

    // === Decode tests ===

    #[test]
    fn test_decode_value_address() {
        let word = "0x000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2";
        assert_eq!(
            decode_value(word, "address"),
            "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
        );
    }

    #[test]
    fn test_decode_value_uint256() {
        // 1000000 in hex = 0xF4240
        let word = "0x00000000000000000000000000000000000000000000000000000000000f4240";
        assert_eq!(decode_value(word, "uint256"), "1000000");
    }

    #[test]
    fn test_decode_value_uint112() {
        let word = "0x0000000000000000000000000000000000000000000000000000000000000064";
        assert_eq!(decode_value(word, "uint112"), "100");
    }

    #[test]
    fn test_decode_value_bool_true() {
        let word = "0x0000000000000000000000000000000000000000000000000000000000000001";
        assert_eq!(decode_value(word, "bool"), "true");
    }

    #[test]
    fn test_decode_value_bool_false() {
        let word = "0x0000000000000000000000000000000000000000000000000000000000000000";
        assert_eq!(decode_value(word, "bool"), "false");
    }

    #[test]
    fn test_decode_value_bytes32() {
        let word = "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
        let result = decode_value(word, "bytes32");
        assert_eq!(
            result,
            "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
        );
    }

    #[test]
    fn test_decode_value_int256_negative() {
        // -1 in two's complement = 0xfff...fff
        let word = "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        assert_eq!(decode_value(word, "int256"), "-1");
    }

    #[test]
    fn test_decode_value_int256_positive() {
        let word = "0x0000000000000000000000000000000000000000000000000000000000000064";
        assert_eq!(decode_value(word, "int256"), "100");
    }

    #[test]
    fn test_decode_value_empty_hex() {
        assert_eq!(decode_value("0x", "uint256"), "\\N");
        assert_eq!(decode_value("", "address"), "\\N");
    }

    #[test]
    fn test_decode_raw_event_sync() {
        let mut decoder = AbiDecoder::new();
        decoder
            .register_abi_json("UniswapV2Pair", uniswap_pair_abi_json())
            .unwrap();

        // Sync event: Sync(uint112 reserve0, uint112 reserve1) - both non-indexed
        let sync_events = decoder.get_events("UniswapV2Pair").unwrap();
        let sync = sync_events.iter().find(|e| e.name == "Sync").unwrap();

        let event = RawEventRecord {
            chain_id: 1,
            block_number: 18000000,
            tx_index: 5,
            log_index: 3,
            block_hash: "0xblockhash".to_string(),
            block_timestamp: 1700000000,
            tx_hash: "0xtxhash".to_string(),
            address: "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc".to_string(),
            topic0: Some(sync.signature.clone()),
            topic1: None,
            topic2: None,
            topic3: None,
            data: format!(
                "0x{}{}",
                "0000000000000000000000000000000000000000000000000000000000000064", // reserve0 = 100
                "00000000000000000000000000000000000000000000000000000000000000c8", // reserve1 = 200
            ),
        };

        let decoded = decoder.decode_raw_event(&event).unwrap();
        assert_eq!(decoded.table_name, "event_uniswap_v2_pair_sync");
        assert_eq!(decoded.param_columns, vec!["reserve0", "reserve1"]);
        assert_eq!(decoded.param_values, vec!["100", "200"]);
    }

    #[test]
    fn test_decode_raw_event_pair_created() {
        let mut decoder = AbiDecoder::new();
        decoder
            .register_abi_json("UniswapV2Factory", uniswap_factory_abi_json())
            .unwrap();

        let events = decoder.get_events("UniswapV2Factory").unwrap();
        let pair_created = &events[0];

        let event = RawEventRecord {
            chain_id: 1,
            block_number: 10000835,
            tx_index: 0,
            log_index: 0,
            block_hash: "0xhash".to_string(),
            block_timestamp: 1600000000,
            tx_hash: "0xtx".to_string(),
            address: "0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f".to_string(),
            topic0: Some(pair_created.signature.clone()),
            topic1: Some(
                "0x000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
                    .to_string(),
            ), // token0
            topic2: Some(
                "0x000000000000000000000000dac17f958d2ee523a2206206994597c13d831ec7"
                    .to_string(),
            ), // token1
            topic3: None,
            data: format!(
                "0x{}{}",
                "000000000000000000000000b4e16d0168e52d35cacd2c6185b44281ec28c9dc", // pair address
                "0000000000000000000000000000000000000000000000000000000000000001", // pairIndex = 1
            ),
        };

        let decoded = decoder.decode_raw_event(&event).unwrap();
        assert_eq!(
            decoded.table_name,
            "event_uniswap_v2_factory_pair_created"
        );
        assert_eq!(
            decoded.param_values[0],
            "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
        ); // token0
        assert_eq!(
            decoded.param_values[1],
            "0xdac17f958d2ee523a2206206994597c13d831ec7"
        ); // token1
        assert_eq!(
            decoded.param_values[2],
            "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"
        ); // pair
        assert_eq!(decoded.param_values[3], "1"); // pairIndex
    }

    #[test]
    fn test_decode_raw_event_no_topic0() {
        let decoder = AbiDecoder::new();
        let event = RawEventRecord {
            chain_id: 1,
            block_number: 100,
            tx_index: 0,
            log_index: 0,
            block_hash: "0x".to_string(),
            block_timestamp: 0,
            tx_hash: "0x".to_string(),
            address: "0x".to_string(),
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
            data: "0x".to_string(),
        };
        assert!(decoder.decode_raw_event(&event).is_none());
    }

    #[test]
    fn test_decode_raw_event_unknown_signature() {
        let decoder = AbiDecoder::new();
        let event = RawEventRecord {
            chain_id: 1,
            block_number: 100,
            tx_index: 0,
            log_index: 0,
            block_hash: "0x".to_string(),
            block_timestamp: 0,
            tx_hash: "0x".to_string(),
            address: "0x".to_string(),
            topic0: Some("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_string()),
            topic1: None,
            topic2: None,
            topic3: None,
            data: "0x".to_string(),
        };
        assert!(decoder.decode_raw_event(&event).is_none());
    }
}
