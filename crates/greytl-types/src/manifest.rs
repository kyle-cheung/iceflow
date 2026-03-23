use crate::{BatchId, CheckpointId, Operation, SourceClass, TableId, TableMode};
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

impl ManifestFile {
    pub fn checksum(&self) -> String {
        stable_checksum([
            self.file_uri.clone(),
            self.file_kind.clone(),
            self.content_hash.clone(),
            self.file_size_bytes.to_string(),
            self.record_count.to_string(),
        ])
    }
}

/// Replayable batch manifest.
///
/// Ownership boundary:
/// - source adapter owns `source_id`, `source_class`, and checkpoint span
/// - DuckDB worker owns ordering span, `record_count`, `op_counts`, `file_set`, and `content_hash`
/// - control plane freezes the final manifest URI and batch identity before commit begins
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchManifest {
    pub batch_id: BatchId,
    pub table_id: TableId,
    pub table_mode: TableMode,
    pub source_id: String,
    pub source_class: SourceClass,
    pub source_checkpoint_start: CheckpointId,
    pub source_checkpoint_end: CheckpointId,
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

impl BatchManifest {
    pub fn checksum(&self) -> String {
        stable_checksum(self.checksum_components())
    }

    pub fn replay_identity(&self) -> String {
        stable_checksum(self.replay_identity_components())
    }

    fn checksum_components(&self) -> Vec<String> {
        let mut parts = self.replay_identity_components();
        parts.push(format!("{:?}", self.created_at));
        parts
    }

    fn replay_identity_components(&self) -> Vec<String> {
        let mut parts = vec![
            self.batch_id.to_string(),
            self.table_id.to_string(),
            format!("{:?}", self.table_mode),
            self.source_id.clone(),
            format!("{:?}", self.source_class),
            self.source_checkpoint_start.to_string(),
            self.source_checkpoint_end.to_string(),
            self.ordering_field.clone(),
            self.ordering_min.to_string(),
            self.ordering_max.to_string(),
            self.schema_version.to_string(),
            self.schema_fingerprint.clone(),
            self.record_count.to_string(),
            self.content_hash.clone(),
        ];

        let mut op_counts: Vec<_> = self
            .op_counts
            .iter()
            .map(|(op, count)| format!("{:?}={}", op, count))
            .collect();
        op_counts.sort();
        parts.extend(op_counts);

        let mut file_checksums: Vec<_> = self
            .file_set
            .iter()
            .map(|file| format!("{}:{}", file.file_uri, file.checksum()))
            .collect();
        file_checksums.sort();
        parts.extend(file_checksums);

        parts
    }
}

fn stable_checksum(parts: impl IntoIterator<Item = String>) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for part in parts {
        for byte in part.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}
