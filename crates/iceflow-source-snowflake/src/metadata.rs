use anyhow::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableMetadata {
    pub primary_keys: Vec<String>,
    pub columns: Vec<String>,
    pub schema_fingerprint: String,
}

pub fn load_table_metadata(
    client: &dyn crate::client::SnowflakeClient,
    binding: &crate::binding::SnowflakeConnectorBinding,
) -> Result<TableMetadata> {
    let primary_keys = load_primary_keys(client, &binding.source_schema, &binding.source_table)?;
    if primary_keys.is_empty() {
        return Err(Error::msg("Snowflake v1 requires a declared primary key"));
    }

    let described = describe_table(client, binding)?;
    let columns = described
        .rows
        .iter()
        .filter_map(|row| row.first().cloned())
        .filter(|column| !column.trim().is_empty())
        .collect::<Vec<_>>();
    if columns.is_empty() {
        return Err(Error::msg("Snowflake table metadata returned no columns"));
    }
    let schema_fingerprint = schema_fingerprint(binding, &described.rows);

    Ok(TableMetadata {
        primary_keys,
        columns,
        schema_fingerprint,
    })
}

pub fn load_schema_fingerprint(
    client: &dyn crate::client::SnowflakeClient,
    binding: &crate::binding::SnowflakeConnectorBinding,
) -> Result<String> {
    let described = describe_table(client, binding)?;
    Ok(schema_fingerprint(binding, &described.rows))
}

fn describe_table(
    client: &dyn crate::client::SnowflakeClient,
    binding: &crate::binding::SnowflakeConnectorBinding,
) -> Result<crate::client::RowSet> {
    client.query_rows(&format!(
        "DESCRIBE TABLE {}",
        crate::client::qualified_table_name(&binding.source_schema, &binding.source_table),
    ))
}

fn schema_fingerprint(
    binding: &crate::binding::SnowflakeConnectorBinding,
    rows: &[Vec<String>],
) -> String {
    format!(
        "{}:{}:{}",
        binding.source_schema,
        binding.source_table,
        rows.iter()
            .map(|row| row.join("|"))
            .collect::<Vec<_>>()
            .join(";"),
    )
}

fn load_primary_keys(
    client: &dyn crate::client::SnowflakeClient,
    source_schema: &str,
    source_table: &str,
) -> Result<Vec<String>> {
    let show = client.exec(&format!(
        "SHOW PRIMARY KEYS IN TABLE {}",
        crate::client::qualified_table_name(source_schema, source_table),
    ))?;
    let rows = client.query_rows(&format!(
        "SELECT \"column_name\" FROM TABLE(RESULT_SCAN('{}')) ORDER BY \"key_sequence\"",
        crate::client::quote_literal_value(&show.query_id),
    ))?;

    Ok(rows
        .rows
        .into_iter()
        .filter_map(|row| row.first().cloned())
        .filter(|column| !column.trim().is_empty())
        .collect())
}

#[cfg(test)]
mod tests {
    use crate::client::{RowSet, SnowflakeClient, StatementOutcome};

    struct FakeMetadataClient;

    impl SnowflakeClient for FakeMetadataClient {
        fn exec(&self, sql: &str) -> anyhow::Result<StatementOutcome> {
            assert!(sql.starts_with("SHOW PRIMARY KEYS"));
            Ok(StatementOutcome {
                query_id: "show-pk-query".to_string(),
            })
        }

        fn query_rows(&self, sql: &str) -> anyhow::Result<RowSet> {
            if sql.contains("RESULT_SCAN('show-pk-query')") {
                return Ok(RowSet {
                    query_id: "pk-query".to_string(),
                    columns: vec!["COLUMN_NAME".to_string()],
                    rows: vec![vec!["CUSTOMER_ID".to_string()]],
                });
            }

            if sql.starts_with("DESCRIBE TABLE") {
                return Ok(RowSet {
                    query_id: "describe-query".to_string(),
                    columns: Vec::new(),
                    rows: vec![vec![
                        "CUSTOMER_ID".to_string(),
                        "VARCHAR".to_string(),
                        "COLUMN".to_string(),
                    ]],
                });
            }

            panic!("unexpected metadata query: {sql}");
        }
    }

    #[test]
    fn load_table_metadata_requires_primary_key() {
        let binding =
            crate::SnowflakeConnectorBinding::from_request(crate::SnowflakeBindingRequest {
                connector_name: "snowflake_customer_state_append".to_string(),
                tables: vec![crate::SnowflakeTableBinding {
                    source_schema: "PUBLIC".to_string(),
                    source_table: "CUSTOMER_STATE".to_string(),
                    destination_namespace: "customer_state".to_string(),
                    destination_table: "customer_state".to_string(),
                    table_mode: "append_only".to_string(),
                }],
                durable_checkpoint: None,
            })
            .expect("binding");

        let metadata = super::load_table_metadata(&FakeMetadataClient, &binding).expect("metadata");

        assert_eq!(metadata.primary_keys, vec!["CUSTOMER_ID".to_string()]);
        assert_eq!(metadata.columns, vec!["CUSTOMER_ID".to_string()]);
        assert!(metadata.schema_fingerprint.contains("CUSTOMER_STATE"));
    }
}
