use crate::config::{ContractConfig, FilterCondition};
use std::collections::HashMap;
use tracing::debug;

/// Engine that evaluates event filters to decide whether decoded events should be stored.
/// Filters are configured per (contract_name, event_name).
pub struct EventFilterEngine {
    /// (contract_name, event_name) → list of conditions (all must pass)
    filters: HashMap<(String, String), Vec<FilterCondition>>,
}

impl EventFilterEngine {
    /// Build the filter engine from contract configs
    pub fn from_contracts(contracts: &[ContractConfig]) -> Self {
        let mut filters = HashMap::new();

        for contract in contracts {
            for filter in &contract.filters {
                let key = (contract.name.clone(), filter.event.clone());
                filters.insert(key, filter.conditions.clone());
            }
        }

        if !filters.is_empty() {
            debug!(filter_count = filters.len(), "Event filter engine initialized");
        }

        Self { filters }
    }

    /// Check whether any filters are configured
    pub fn has_filters(&self) -> bool {
        !self.filters.is_empty()
    }

    /// Check if an event should be included (stored) based on configured filters.
    /// Returns true if the event passes all filter conditions, or if no filter is configured.
    ///
    /// `decoded_params` is a slice of (param_name, param_value) pairs from the decoded event.
    pub fn should_include(
        &self,
        contract_name: &str,
        event_name: &str,
        decoded_params: &[(&str, &str)],
    ) -> bool {
        let key = (contract_name.to_string(), event_name.to_string());
        let conditions = match self.filters.get(&key) {
            Some(c) => c,
            None => return true, // no filter → include everything
        };

        // All conditions must pass
        conditions.iter().all(|cond| {
            let param_value = decoded_params
                .iter()
                .find(|(name, _)| *name == cond.field)
                .map(|(_, v)| *v);

            match param_value {
                Some(value) => evaluate_condition(&cond.op, value, &cond.value),
                None => true, // field not found → don't filter out
            }
        })
    }
}

/// Arbitrary-precision decimal integer for filter comparisons.
/// Avoids f64 precision loss for uint256 values (which can be up to 2^256).
#[derive(Debug, Clone)]
struct BigDecimalInt {
    negative: bool,
    /// Decimal digits (most significant first), no leading zeros
    digits: Vec<u8>,
}

impl BigDecimalInt {
    fn parse(s: &str) -> Option<Self> {
        let s = s.trim();
        if s.is_empty() {
            return None;
        }
        let (negative, digits_str) = if let Some(rest) = s.strip_prefix('-') {
            (true, rest)
        } else {
            (false, s)
        };
        if digits_str.is_empty() || !digits_str.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let trimmed = digits_str.trim_start_matches('0');
        let digits: Vec<u8> = if trimmed.is_empty() {
            vec![0]
        } else {
            trimmed.bytes().map(|b| b - b'0').collect()
        };
        let negative = negative && !(digits.len() == 1 && digits[0] == 0);
        Some(Self { negative, digits })
    }
}

impl PartialEq for BigDecimalInt {
    fn eq(&self, other: &Self) -> bool {
        self.negative == other.negative && self.digits == other.digits
    }
}
impl Eq for BigDecimalInt {}

impl PartialOrd for BigDecimalInt {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BigDecimalInt {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (self.negative, other.negative) {
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => {
                self.digits.len().cmp(&other.digits.len())
                    .then_with(|| self.digits.cmp(&other.digits))
            }
            (true, true) => {
                other.digits.len().cmp(&self.digits.len())
                    .then_with(|| other.digits.cmp(&self.digits))
            }
        }
    }
}

/// Evaluate a single condition. Attempts arbitrary-precision numeric comparison first,
/// falls back to string comparison.
fn evaluate_condition(op: &str, actual: &str, expected: &str) -> bool {
    if let (Some(a), Some(e)) = (BigDecimalInt::parse(actual), BigDecimalInt::parse(expected)) {
        return match op {
            "eq" => a == e,
            "neq" => a != e,
            "gt" => a > e,
            "gte" => a >= e,
            "lt" => a < e,
            "lte" => a <= e,
            "contains" => actual.contains(expected),
            _ => true,
        };
    }

    // String comparison
    match op {
        "eq" => actual == expected,
        "neq" => actual != expected,
        "gt" => actual > expected,
        "gte" => actual >= expected,
        "lt" => actual < expected,
        "lte" => actual <= expected,
        "contains" => actual.contains(expected),
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{EventFilterConfig, FilterCondition};

    fn make_contract(name: &str, filters: Vec<EventFilterConfig>) -> ContractConfig {
        ContractConfig {
            name: name.to_string(),
            address: Some("0x1234567890abcdef1234567890abcdef12345678".to_string()),
            factory: None,
            abi_path: "./test.json".to_string(),
            start_block: None,
            filters,
            views: vec![],
        }
    }

    #[test]
    fn test_no_filters_includes_everything() {
        let engine = EventFilterEngine::from_contracts(&[]);
        assert!(!engine.has_filters());
        assert!(engine.should_include("USDC", "Transfer", &[("value", "100")]));
    }

    #[test]
    fn test_eq_numeric() {
        let contracts = vec![make_contract("USDC", vec![
            EventFilterConfig {
                event: "Transfer".to_string(),
                conditions: vec![FilterCondition {
                    field: "value".to_string(),
                    op: "eq".to_string(),
                    value: "1000000".to_string(),
                }],
            },
        ])];
        let engine = EventFilterEngine::from_contracts(&contracts);
        assert!(engine.has_filters());
        assert!(engine.should_include("USDC", "Transfer", &[("value", "1000000")]));
        assert!(!engine.should_include("USDC", "Transfer", &[("value", "999999")]));
    }

    #[test]
    fn test_gte_numeric() {
        let contracts = vec![make_contract("USDC", vec![
            EventFilterConfig {
                event: "Transfer".to_string(),
                conditions: vec![FilterCondition {
                    field: "value".to_string(),
                    op: "gte".to_string(),
                    value: "1000000".to_string(),
                }],
            },
        ])];
        let engine = EventFilterEngine::from_contracts(&contracts);
        assert!(engine.should_include("USDC", "Transfer", &[("value", "1000000")]));
        assert!(engine.should_include("USDC", "Transfer", &[("value", "2000000")]));
        assert!(!engine.should_include("USDC", "Transfer", &[("value", "999999")]));
    }

    #[test]
    fn test_gt_numeric() {
        let contracts = vec![make_contract("USDC", vec![
            EventFilterConfig {
                event: "Transfer".to_string(),
                conditions: vec![FilterCondition {
                    field: "value".to_string(),
                    op: "gt".to_string(),
                    value: "0".to_string(),
                }],
            },
        ])];
        let engine = EventFilterEngine::from_contracts(&contracts);
        assert!(engine.should_include("USDC", "Transfer", &[("value", "1")]));
        assert!(!engine.should_include("USDC", "Transfer", &[("value", "0")]));
    }

    #[test]
    fn test_lt_lte() {
        let contracts = vec![make_contract("Token", vec![
            EventFilterConfig {
                event: "Transfer".to_string(),
                conditions: vec![FilterCondition {
                    field: "value".to_string(),
                    op: "lt".to_string(),
                    value: "100".to_string(),
                }],
            },
        ])];
        let engine = EventFilterEngine::from_contracts(&contracts);
        assert!(engine.should_include("Token", "Transfer", &[("value", "99")]));
        assert!(!engine.should_include("Token", "Transfer", &[("value", "100")]));
        assert!(!engine.should_include("Token", "Transfer", &[("value", "101")]));
    }

    #[test]
    fn test_neq() {
        let contracts = vec![make_contract("Token", vec![
            EventFilterConfig {
                event: "Transfer".to_string(),
                conditions: vec![FilterCondition {
                    field: "value".to_string(),
                    op: "neq".to_string(),
                    value: "0".to_string(),
                }],
            },
        ])];
        let engine = EventFilterEngine::from_contracts(&contracts);
        assert!(engine.should_include("Token", "Transfer", &[("value", "1")]));
        assert!(!engine.should_include("Token", "Transfer", &[("value", "0")]));
    }

    #[test]
    fn test_contains_string() {
        let contracts = vec![make_contract("Token", vec![
            EventFilterConfig {
                event: "Transfer".to_string(),
                conditions: vec![FilterCondition {
                    field: "from".to_string(),
                    op: "contains".to_string(),
                    value: "dead".to_string(),
                }],
            },
        ])];
        let engine = EventFilterEngine::from_contracts(&contracts);
        assert!(engine.should_include("Token", "Transfer", &[("from", "0xdead000")]));
        assert!(!engine.should_include("Token", "Transfer", &[("from", "0xbeef000")]));
    }

    #[test]
    fn test_multiple_conditions_all_must_pass() {
        let contracts = vec![make_contract("USDC", vec![
            EventFilterConfig {
                event: "Transfer".to_string(),
                conditions: vec![
                    FilterCondition {
                        field: "value".to_string(),
                        op: "gte".to_string(),
                        value: "1000000".to_string(),
                    },
                    FilterCondition {
                        field: "value".to_string(),
                        op: "lte".to_string(),
                        value: "10000000".to_string(),
                    },
                ],
            },
        ])];
        let engine = EventFilterEngine::from_contracts(&contracts);
        assert!(engine.should_include("USDC", "Transfer", &[("value", "5000000")]));
        assert!(!engine.should_include("USDC", "Transfer", &[("value", "500")]));
        assert!(!engine.should_include("USDC", "Transfer", &[("value", "20000000")]));
    }

    #[test]
    fn test_no_filter_for_event_includes_it() {
        let contracts = vec![make_contract("USDC", vec![
            EventFilterConfig {
                event: "Transfer".to_string(),
                conditions: vec![FilterCondition {
                    field: "value".to_string(),
                    op: "gt".to_string(),
                    value: "0".to_string(),
                }],
            },
        ])];
        let engine = EventFilterEngine::from_contracts(&contracts);
        // Approval has no filter → should be included
        assert!(engine.should_include("USDC", "Approval", &[("value", "0")]));
    }

    #[test]
    fn test_missing_field_does_not_filter_out() {
        let contracts = vec![make_contract("USDC", vec![
            EventFilterConfig {
                event: "Transfer".to_string(),
                conditions: vec![FilterCondition {
                    field: "nonexistent".to_string(),
                    op: "gt".to_string(),
                    value: "0".to_string(),
                }],
            },
        ])];
        let engine = EventFilterEngine::from_contracts(&contracts);
        // Field doesn't exist → condition passes (don't filter out)
        assert!(engine.should_include("USDC", "Transfer", &[("value", "100")]));
    }

    #[test]
    fn test_unknown_operator_passes() {
        assert!(evaluate_condition("invalid_op", "100", "50"));
    }

    #[test]
    fn test_string_eq_comparison() {
        let contracts = vec![make_contract("Token", vec![
            EventFilterConfig {
                event: "Transfer".to_string(),
                conditions: vec![FilterCondition {
                    field: "to".to_string(),
                    op: "eq".to_string(),
                    value: "0xdead".to_string(),
                }],
            },
        ])];
        let engine = EventFilterEngine::from_contracts(&contracts);
        assert!(engine.should_include("Token", "Transfer", &[("to", "0xdead")]));
        assert!(!engine.should_include("Token", "Transfer", &[("to", "0xbeef")]));
    }

    #[test]
    fn test_empty_conditions_includes_everything() {
        // A filter with an event but empty conditions → always includes
        let contracts = vec![make_contract("USDC", vec![
            EventFilterConfig {
                event: "Transfer".to_string(),
                conditions: vec![],
            },
        ])];
        let engine = EventFilterEngine::from_contracts(&contracts);
        assert!(engine.has_filters());
        assert!(engine.should_include("USDC", "Transfer", &[("value", "0")]));
        assert!(engine.should_include("USDC", "Transfer", &[]));
    }

    #[test]
    fn test_negative_numeric_values() {
        let contracts = vec![make_contract("Token", vec![
            EventFilterConfig {
                event: "PriceUpdate".to_string(),
                conditions: vec![FilterCondition {
                    field: "delta".to_string(),
                    op: "lt".to_string(),
                    value: "0".to_string(),
                }],
            },
        ])];
        let engine = EventFilterEngine::from_contracts(&contracts);
        assert!(engine.should_include("Token", "PriceUpdate", &[("delta", "-100")]));
        assert!(!engine.should_include("Token", "PriceUpdate", &[("delta", "50")]));
    }

    #[test]
    fn test_large_numeric_values() {
        // Test with values larger than u64::MAX (common in EVM uint256)
        let contracts = vec![make_contract("USDC", vec![
            EventFilterConfig {
                event: "Transfer".to_string(),
                conditions: vec![FilterCondition {
                    field: "value".to_string(),
                    op: "gte".to_string(),
                    value: "1000000000000000000".to_string(), // 1e18
                }],
            },
        ])];
        let engine = EventFilterEngine::from_contracts(&contracts);
        assert!(engine.should_include("USDC", "Transfer", &[("value", "5000000000000000000")])); // 5e18
        assert!(!engine.should_include("USDC", "Transfer", &[("value", "100")]));
    }

    #[test]
    fn test_non_numeric_values_use_string_comparison() {
        // Hex addresses are non-numeric → string comparison
        let contracts = vec![make_contract("Token", vec![
            EventFilterConfig {
                event: "Transfer".to_string(),
                conditions: vec![FilterCondition {
                    field: "from".to_string(),
                    op: "eq".to_string(),
                    value: "0xdead000000000000000000000000000000000000".to_string(),
                }],
            },
        ])];
        let engine = EventFilterEngine::from_contracts(&contracts);
        assert!(engine.should_include("Token", "Transfer", &[("from", "0xdead000000000000000000000000000000000000")]));
        assert!(!engine.should_include("Token", "Transfer", &[("from", "0xbeef000000000000000000000000000000000000")]));
    }

    #[test]
    fn test_contains_empty_string_matches_everything() {
        assert!(evaluate_condition("contains", "hello world", ""));
        assert!(evaluate_condition("contains", "", ""));
    }

    #[test]
    fn test_contains_case_sensitive() {
        assert!(!evaluate_condition("contains", "Hello", "hello"));
        assert!(evaluate_condition("contains", "Hello", "Hell"));
    }

    #[test]
    fn test_multiple_contracts_independent_filters() {
        let contracts = vec![
            make_contract("USDC", vec![
                EventFilterConfig {
                    event: "Transfer".to_string(),
                    conditions: vec![FilterCondition {
                        field: "value".to_string(),
                        op: "gte".to_string(),
                        value: "1000000".to_string(),
                    }],
                },
            ]),
            make_contract("WETH", vec![
                EventFilterConfig {
                    event: "Transfer".to_string(),
                    conditions: vec![FilterCondition {
                        field: "value".to_string(),
                        op: "gte".to_string(),
                        value: "1000000000000000000".to_string(), // 1 ETH
                    }],
                },
            ]),
        ];
        let engine = EventFilterEngine::from_contracts(&contracts);

        // USDC filter: >= 1M (passes)
        assert!(engine.should_include("USDC", "Transfer", &[("value", "5000000")]));
        // WETH filter: >= 1e18 (fails for same value)
        assert!(!engine.should_include("WETH", "Transfer", &[("value", "5000000")]));
        // Unknown contract → no filter → passes
        assert!(engine.should_include("DAI", "Transfer", &[("value", "1")]));
    }

    #[test]
    fn test_same_contract_different_events_different_filters() {
        let contracts = vec![make_contract("Token", vec![
            EventFilterConfig {
                event: "Transfer".to_string(),
                conditions: vec![FilterCondition {
                    field: "value".to_string(),
                    op: "gt".to_string(),
                    value: "0".to_string(),
                }],
            },
            EventFilterConfig {
                event: "Approval".to_string(),
                conditions: vec![FilterCondition {
                    field: "spender".to_string(),
                    op: "neq".to_string(),
                    value: "0x0000000000000000000000000000000000000000".to_string(),
                }],
            },
        ])];
        let engine = EventFilterEngine::from_contracts(&contracts);

        // Transfer filter applies to Transfer
        assert!(!engine.should_include("Token", "Transfer", &[("value", "0")]));
        assert!(engine.should_include("Token", "Transfer", &[("value", "1")]));

        // Approval filter applies to Approval (different field)
        assert!(!engine.should_include("Token", "Approval", &[("spender", "0x0000000000000000000000000000000000000000")]));
        assert!(engine.should_include("Token", "Approval", &[("spender", "0xdead")]));
    }

    #[test]
    fn test_empty_decoded_params() {
        let contracts = vec![make_contract("Token", vec![
            EventFilterConfig {
                event: "Transfer".to_string(),
                conditions: vec![FilterCondition {
                    field: "value".to_string(),
                    op: "gt".to_string(),
                    value: "0".to_string(),
                }],
            },
        ])];
        let engine = EventFilterEngine::from_contracts(&contracts);
        // Empty params → field not found → condition passes (don't filter out)
        assert!(engine.should_include("Token", "Transfer", &[]));
    }

    #[test]
    fn test_multiple_params_only_filtered_one_checked() {
        let contracts = vec![make_contract("Token", vec![
            EventFilterConfig {
                event: "Transfer".to_string(),
                conditions: vec![FilterCondition {
                    field: "value".to_string(),
                    op: "gt".to_string(),
                    value: "100".to_string(),
                }],
            },
        ])];
        let engine = EventFilterEngine::from_contracts(&contracts);
        // Has from/to/value params but only value is filtered
        assert!(engine.should_include("Token", "Transfer", &[
            ("from", "0xaaaa"),
            ("to", "0xbbbb"),
            ("value", "200"),
        ]));
        assert!(!engine.should_include("Token", "Transfer", &[
            ("from", "0xaaaa"),
            ("to", "0xbbbb"),
            ("value", "50"),
        ]));
    }

    #[test]
    fn test_all_operators_string_fallback() {
        // Non-numeric strings: all operators work via string comparison
        assert!(evaluate_condition("eq", "abc", "abc"));
        assert!(!evaluate_condition("eq", "abc", "def"));
        assert!(evaluate_condition("neq", "abc", "def"));
        assert!(!evaluate_condition("neq", "abc", "abc"));
        assert!(evaluate_condition("gt", "b", "a"));
        assert!(!evaluate_condition("gt", "a", "b"));
        assert!(evaluate_condition("gte", "a", "a"));
        assert!(evaluate_condition("lt", "a", "b"));
        assert!(evaluate_condition("lte", "b", "b"));
        assert!(evaluate_condition("contains", "abcdef", "cde"));
    }

    #[test]
    fn test_has_filters_reflects_state() {
        let empty = EventFilterEngine::from_contracts(&[]);
        assert!(!empty.has_filters());

        let no_filters = EventFilterEngine::from_contracts(&[make_contract("T", vec![])]);
        assert!(!no_filters.has_filters());

        let with_filters = EventFilterEngine::from_contracts(&[make_contract("T", vec![
            EventFilterConfig {
                event: "E".to_string(),
                conditions: vec![FilterCondition {
                    field: "f".to_string(),
                    op: "eq".to_string(),
                    value: "v".to_string(),
                }],
            },
        ])]);
        assert!(with_filters.has_filters());
    }

    #[test]
    fn test_filter_config_from_yaml() {
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
    address: "0x1234567890abcdef1234567890abcdef12345678"
    abi_path: "./test.json"
    filters:
      - event: "Transfer"
        conditions:
          - field: "value"
            op: "gte"
            value: "1000000"
      - event: "Approval"
        conditions:
          - field: "value"
            op: "gt"
            value: "0"
"#;
        let config: crate::config::IndexerConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.contracts[0].filters.len(), 2);
        assert_eq!(config.contracts[0].filters[0].event, "Transfer");
        assert_eq!(config.contracts[0].filters[0].conditions[0].field, "value");
        assert_eq!(config.contracts[0].filters[0].conditions[0].op, "gte");
        assert_eq!(config.contracts[0].filters[0].conditions[0].value, "1000000");
    }
}
