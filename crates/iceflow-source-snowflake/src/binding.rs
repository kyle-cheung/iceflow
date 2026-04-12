use anyhow::{Error, Result};
use iceflow_types::CheckpointId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnowflakeTableBinding {
    pub source_schema: String,
    pub source_table: String,
    pub destination_namespace: String,
    pub destination_table: String,
    pub table_mode: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnowflakeBindingRequest {
    pub connector_name: String,
    pub tables: Vec<SnowflakeTableBinding>,
    pub durable_checkpoint: Option<CheckpointId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnowflakeConnectorBinding {
    pub connector_name: String,
    pub source_schema: String,
    pub source_table: String,
    pub destination_namespace: String,
    pub destination_table: String,
    pub table_mode: String,
    pub managed_stream_name: String,
    pub durable_checkpoint: Option<CheckpointId>,
}

impl SnowflakeConnectorBinding {
    pub fn from_request(req: SnowflakeBindingRequest) -> Result<Self> {
        let [table] = req.tables.as_slice() else {
            return Err(Error::msg(
                "Snowflake v1 requires exactly one selected table",
            ));
        };
        if table.table_mode != "append_only" {
            return Err(Error::msg(
                "Snowflake v1 requires table_mode = \"append_only\"",
            ));
        }

        Ok(Self {
            connector_name: req.connector_name.clone(),
            source_schema: table.source_schema.clone(),
            source_table: table.source_table.clone(),
            destination_namespace: table.destination_namespace.clone(),
            destination_table: table.destination_table.clone(),
            table_mode: table.table_mode.clone(),
            managed_stream_name: build_managed_stream_name(
                &req.connector_name,
                &table.source_schema,
                &table.source_table,
            ),
            durable_checkpoint: req.durable_checkpoint,
        })
    }
}

fn build_managed_stream_name(
    connector_name: &str,
    source_schema: &str,
    source_table: &str,
) -> String {
    const MAX_IDENTIFIER_LEN: usize = 255;
    const PREFIX: &str = "_iceflow_";

    let mut connector = sanitize_component(connector_name);
    let mut schema = sanitize_component(source_schema);
    let mut table = sanitize_component(source_table);
    let suffix = stable_hash_suffix(&[connector_name, source_schema, source_table]);
    let fixed_len = PREFIX.len() + 3 + suffix.len();
    truncate_components_to_budget(
        &mut connector,
        &mut schema,
        &mut table,
        MAX_IDENTIFIER_LEN.saturating_sub(fixed_len),
    );

    format!("{PREFIX}{connector}_{schema}_{table}_{suffix}")
}

fn stable_hash_suffix(parts: &[&str]) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for part in parts {
        for byte in part.as_bytes().iter().chain(std::iter::once(&0xff)) {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }

    format!("{:08x}", (hash & 0xffff_ffff) as u32)
}

fn sanitize_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            'a'..='z' | '0'..='9' => ch,
            'A'..='Z' => ch.to_ascii_lowercase(),
            _ => '_',
        })
        .collect()
}

fn truncate_components_to_budget(
    connector: &mut String,
    schema: &mut String,
    table: &mut String,
    budget: usize,
) {
    while connector.len() + schema.len() + table.len() > budget {
        if connector.len() >= schema.len() && connector.len() >= table.len() {
            connector.pop();
        } else if schema.len() >= table.len() {
            schema.pop();
        } else {
            table.pop();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_name_is_connector_scoped_and_hashed() {
        let binding = SnowflakeConnectorBinding::from_request(SnowflakeBindingRequest {
            connector_name: "snowflake_customer_state_append".to_string(),
            tables: vec![SnowflakeTableBinding {
                source_schema: "PUBLIC".to_string(),
                source_table: "CUSTOMER_STATE".to_string(),
                destination_namespace: "customer_state".to_string(),
                destination_table: "customer_state".to_string(),
                table_mode: "append_only".to_string(),
            }],
            durable_checkpoint: None,
        })
        .expect("valid binding");

        assert!(binding
            .managed_stream_name
            .starts_with("_iceflow_snowflake_customer_state_append_public_customer_state_"));
    }

    #[test]
    fn stream_name_respects_snowflake_identifier_length_limit() {
        let connector_name = "connector".repeat(80);
        let source_schema = "schema".repeat(80);
        let source_table = "table".repeat(80);
        let suffix = stable_hash_suffix(&[&connector_name, &source_schema, &source_table]);

        let binding = SnowflakeConnectorBinding::from_request(SnowflakeBindingRequest {
            connector_name: connector_name.clone(),
            tables: vec![SnowflakeTableBinding {
                source_schema: source_schema.clone(),
                source_table: source_table.clone(),
                destination_namespace: "customer_state".to_string(),
                destination_table: "customer_state".to_string(),
                table_mode: "append_only".to_string(),
            }],
            durable_checkpoint: None,
        })
        .expect("valid binding");

        assert!(binding.managed_stream_name.len() <= 255);
        assert!(binding.managed_stream_name.ends_with(&format!("_{suffix}")));
        assert!(binding.managed_stream_name.starts_with("_iceflow_"));
    }
}
