use anyhow::Result;
use sqlx::PgPool;
use tracing::debug;

/// Creates SQL VIEWs in the user schema that expose raw events as human-readable tables.
/// Ported from packages/syncer/src/services/ViewCreator.ts
pub struct ViewCreator {
    pool: PgPool,
    data_schema: String,
    sync_schema: String,
    user_schema: String,
}

impl ViewCreator {
    pub fn new(pool: PgPool, data_schema: String, sync_schema: String, user_schema: String) -> Self {
        Self {
            pool,
            data_schema,
            sync_schema,
            user_schema,
        }
    }

    /// Create a view for a specific event sourced from the decoded event table
    pub async fn create_event_view(
        &self,
        contract_name: &str,
        event_name: &str,
        _event_signature: &str,
        chain_id: i32,
        address: Option<&str>,
        is_factory: bool,
        factory_contract_name: Option<&str>,
    ) -> Result<()> {
        let view_name = format!(
            "event_{}_{}",
            to_snake_case(contract_name),
            to_snake_case(event_name)
        );

        // Source is the decoded event table in the sync schema
        let source_table = format!("{}.{}", self.sync_schema, view_name);

        // Note: schema, view, and table names are validated as safe SQL identifiers
        // at config load time (validate_sql_identifier). Address values and contract
        // names used in WHERE clauses are parameterized or pre-validated.
        let address_filter = if is_factory {
            let factory_name = factory_contract_name.unwrap_or(contract_name);
            // factory_name is validated as a SQL identifier at config load time
            format!(
                "AND address IN (SELECT child_address FROM {}.factory_children WHERE chain_id = {} AND contract_name = '{}')",
                self.data_schema, chain_id, factory_name
            )
        } else if let Some(addr) = address {
            // addr is validated as an Ethereum hex address at config load time
            format!("AND address = '{}'", addr.to_lowercase())
        } else {
            String::new()
        };

        let sql = format!(
            r#"
            CREATE OR REPLACE VIEW {user_schema}.{view_name} AS
            SELECT *
            FROM {source_table}
            WHERE chain_id = {chain_id}
              {address_filter}
            ORDER BY block_number, tx_index, log_index
            "#,
            user_schema = self.user_schema,
        );

        sqlx::query(&sql).execute(&self.pool).await?;

        debug!(
            view = %format!("{}.{}", self.user_schema, view_name),
            "Created event view"
        );

        Ok(())
    }

    /// Create a catch-all view for all events of a contract
    pub async fn create_all_events_view(
        &self,
        contract_name: &str,
        chain_id: i32,
        addresses: &[String],
    ) -> Result<()> {
        if addresses.is_empty() {
            return Ok(());
        }

        let view_name = format!("event_{}_all", to_snake_case(contract_name));
        let address_list = addresses
            .iter()
            .map(|a| format!("'{}'", a.to_lowercase()))
            .collect::<Vec<_>>()
            .join(", ");

        let sql = format!(
            r#"
            CREATE OR REPLACE VIEW {user_schema}.{view_name} AS
            SELECT
                chain_id,
                block_number,
                block_timestamp,
                block_hash,
                tx_hash,
                tx_index,
                log_index,
                address,
                topic0,
                topic1,
                topic2,
                topic3,
                data
            FROM {data_schema}.raw_events
            WHERE chain_id = {chain_id}
              AND address IN ({address_list})
            ORDER BY block_number, tx_index, log_index
            "#,
            user_schema = self.user_schema,
            data_schema = self.data_schema,
        );

        sqlx::query(&sql).execute(&self.pool).await?;

        debug!(
            view = %format!("{}.{}", self.user_schema, view_name),
            "Created all-events view"
        );

        Ok(())
    }

    /// Create a view for raw traces in the user schema
    pub async fn create_traces_view(&self, chain_id: i32) -> Result<()> {
        let sql = format!(
            r#"
            CREATE OR REPLACE VIEW {user_schema}.raw_traces AS
            SELECT *
            FROM {data_schema}.raw_traces
            WHERE chain_id = {chain_id}
            ORDER BY block_number, tx_index, trace_address
            "#,
            user_schema = self.user_schema,
            data_schema = self.data_schema,
        );

        sqlx::query(&sql).execute(&self.pool).await?;

        debug!(
            view = %format!("{}.raw_traces", self.user_schema),
            "Created raw traces view"
        );

        Ok(())
    }

    /// Create a view for account events in the user schema
    pub async fn create_account_events_view(&self, chain_id: i32) -> Result<()> {
        let sql = format!(
            r#"
            CREATE OR REPLACE VIEW {user_schema}.account_events AS
            SELECT *
            FROM {data_schema}.raw_account_events
            WHERE chain_id = {chain_id}
            ORDER BY block_number, tx_index, event_type
            "#,
            user_schema = self.user_schema,
            data_schema = self.data_schema,
        );

        sqlx::query(&sql).execute(&self.pool).await?;

        debug!(
            view = %format!("{}.account_events", self.user_schema),
            "Created account events view"
        );

        Ok(())
    }

    /// Create a view for a continuous aggregate in the user schema
    pub async fn create_aggregate_view(
        &self,
        contract_name: &str,
        event_name: &str,
        suffix: &str,
        chain_id: i32,
        address: Option<&str>,
        is_factory: bool,
        factory_contract_name: Option<&str>,
    ) -> Result<()> {
        let table_name = format!(
            "event_{}_{}",
            to_snake_case(contract_name),
            to_snake_case(event_name)
        );
        let agg_name = format!("{}_{}", table_name, suffix);
        let source = format!("{}.{}", self.sync_schema, agg_name);

        // Schema/contract names validated as safe identifiers at config load time
        let address_filter = if is_factory {
            let factory_name = factory_contract_name.unwrap_or(contract_name);
            format!(
                "AND address IN (SELECT child_address FROM {}.factory_children WHERE chain_id = {} AND contract_name = '{}')",
                self.data_schema, chain_id, factory_name
            )
        } else if let Some(addr) = address {
            format!("AND address = '{}'", addr.to_lowercase())
        } else {
            String::new()
        };

        let sql = format!(
            r#"
            CREATE OR REPLACE VIEW {user_schema}.{agg_name} AS
            SELECT *
            FROM {source}
            WHERE chain_id = {chain_id}
              {address_filter}
            ORDER BY bucket
            "#,
            user_schema = self.user_schema,
        );

        sqlx::query(&sql).execute(&self.pool).await?;

        debug!(
            view = %format!("{}.{}", self.user_schema, agg_name),
            "Created aggregate view"
        );

        Ok(())
    }
}

/// Convert a PascalCase or camelCase string to snake_case
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_snake_case() {
        assert_eq!(to_snake_case("PairCreated"), "pair_created");
        assert_eq!(to_snake_case("Transfer"), "transfer");
        assert_eq!(to_snake_case("Swap"), "swap");
        assert_eq!(to_snake_case("UniswapV2Factory"), "uniswap_v2_factory");
        assert_eq!(to_snake_case("already_snake"), "already_snake");
        assert_eq!(to_snake_case("camelCase"), "camel_case");
    }

    #[test]
    fn test_to_snake_case_empty() {
        assert_eq!(to_snake_case(""), "");
    }

    #[test]
    fn test_to_snake_case_single_uppercase() {
        assert_eq!(to_snake_case("A"), "a");
    }

    #[test]
    fn test_to_snake_case_consecutive_uppercase() {
        assert_eq!(to_snake_case("ABCDef"), "a_b_c_def");
    }

    #[test]
    fn test_to_snake_case_all_lowercase() {
        assert_eq!(to_snake_case("lowercase"), "lowercase");
    }

    #[test]
    fn test_to_snake_case_with_numbers() {
        assert_eq!(to_snake_case("amount0In"), "amount0_in");
        assert_eq!(to_snake_case("reserve0"), "reserve0");
        assert_eq!(to_snake_case("V2Pair"), "v2_pair");
    }

    #[test]
    fn test_event_view_name_format() {
        let view_name = format!(
            "event_{}_{}",
            to_snake_case("UniswapV2Pair"),
            to_snake_case("Swap")
        );
        assert_eq!(view_name, "event_uniswap_v2_pair_swap");
    }

    #[test]
    fn test_event_view_name_format_factory() {
        let view_name = format!(
            "event_{}_{}",
            to_snake_case("UniswapV2Factory"),
            to_snake_case("PairCreated")
        );
        assert_eq!(view_name, "event_uniswap_v2_factory_pair_created");
    }

    #[test]
    fn test_all_events_view_name_format() {
        let view_name = format!("event_{}_all", to_snake_case("UniswapV2Pair"));
        assert_eq!(view_name, "event_uniswap_v2_pair_all");
    }
}
