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
    let connector = sanitize_component(connector_name);
    let schema = sanitize_component(source_schema);
    let table = sanitize_component(source_table);
    let suffix = stable_hash_suffix(&[connector_name, source_schema, source_table]);

    format!("_iceflow_{connector}_{schema}_{table}_{suffix}")
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
}
