use crate::{BatchId, Operation, SourceClass, TableId, TableMode};
use chrono::{DateTime, Utc};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestFile {
    pub file_uri: String,
    pub file_kind: String,
    pub content_hash: String,
    pub file_size_bytes: u64,
    pub record_count: u64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchManifest {
    pub batch_id: BatchId,
    pub table_id: TableId,
    pub table_mode: TableMode,
    pub source_id: String,
    pub source_class: SourceClass,
    pub source_checkpoint_start: String,
    pub source_checkpoint_end: String,
    pub ordering_field: String,
    pub ordering_min: i64,
    pub ordering_max: i64,
    pub schema_version: i32,
    pub schema_fingerprint: String,
    pub record_count: u64,
    pub op_counts: BTreeMap<Operation, u64>,
    pub file_set: Vec<ManifestFile>,
    pub content_hash: String,
    pub created_at: DateTime<Utc>,
}
