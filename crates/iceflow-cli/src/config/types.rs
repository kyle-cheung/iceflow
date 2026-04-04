use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SourceConfig {
    pub version: u32,
    pub kind: String,
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct DestinationConfig {
    pub version: u32,
    pub kind: String,
    #[serde(default)]
    pub catalog: Option<String>,
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CatalogConfig {
    pub version: u32,
    pub kind: String,
    pub endpoint: String,
    pub warehouse: String,
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ConnectorConfig {
    pub version: u32,
    pub source: String,
    pub destination: String,
    #[serde(default)]
    pub catalog: Option<String>,
    #[serde(default)]
    pub capture: CaptureSettings,
    pub tables: Vec<TableEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
pub struct CaptureSettings {
    #[serde(default)]
    pub publication_name: Option<String>,
    #[serde(default)]
    pub slot_name: Option<String>,
    #[serde(default)]
    pub bootstrap_policy: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct TableEntry {
    pub source_schema: String,
    pub source_table: String,
    pub destination_namespace: String,
    pub destination_table: String,
    pub table_mode: String,
    #[serde(default)]
    pub key_columns: Vec<String>,
    #[serde(default)]
    pub ordering_field: Option<String>,
}
