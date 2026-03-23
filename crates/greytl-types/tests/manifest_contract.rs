use greytl_types::{BatchManifest, BatchId, ManifestFile, Operation, TableId, TableMode};
use std::collections::BTreeMap;

#[test]
fn batch_manifest_captures_replay_boundary_fields() {
    let manifest = BatchManifest {
        batch_id: BatchId::from("batch-0001"),
        table_id: TableId::from("customer_state"),
        table_mode: TableMode::KeyedUpsert,
        source_id: "source-a".to_string(),
        source_class: greytl_types::SourceClass::DatabaseCdc,
        source_checkpoint_start: "cp-1".to_string(),
        source_checkpoint_end: "cp-2".to_string(),
        ordering_field: "source_position".to_string(),
        ordering_min: 1,
        ordering_max: 7,
        schema_version: 3,
        schema_fingerprint: "fingerprint".to_string(),
        record_count: 2,
        op_counts: BTreeMap::from([(Operation::Upsert, 2)]),
        file_set: vec![ManifestFile {
            file_uri: "file:///tmp/batch-0001.parquet".to_string(),
            file_kind: "parquet".to_string(),
            content_hash: "hash".to_string(),
            file_size_bytes: 128,
            record_count: 2,
            created_at: chrono::Utc::now(),
        }],
        content_hash: "content-hash".to_string(),
        created_at: chrono::Utc::now(),
    };

    assert_eq!(manifest.batch_id, BatchId::from("batch-0001"));
    assert_eq!(manifest.record_count, 2);
    assert_eq!(manifest.file_set.len(), 1);
}
