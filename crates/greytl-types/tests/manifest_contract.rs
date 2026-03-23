use greytl_types::{
    checkpoint, BatchId, BatchManifest, ManifestFile, Operation, TableId, TableMode,
};
use std::collections::BTreeMap;
fn fixed_time(offset_secs: u64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp(offset_secs as i64, 0).expect("valid timestamp")
}

fn manifest_fixture(content_hash: &str, file_a_hash: &str) -> BatchManifest {
    BatchManifest {
        batch_id: BatchId::from("batch-0001"),
        table_id: TableId::from("customer_state"),
        table_mode: TableMode::KeyedUpsert,
        source_id: "source-a".to_string(),
        source_class: greytl_types::SourceClass::DatabaseCdc,
        source_checkpoint_start: checkpoint("cp-1"),
        source_checkpoint_end: checkpoint("cp-2"),
        ordering_field: "source_position".to_string(),
        ordering_min: 1,
        ordering_max: 7,
        schema_version: 3,
        schema_fingerprint: "fingerprint".to_string(),
        record_count: 2,
        op_counts: BTreeMap::from([(Operation::Upsert, 2)]),
        file_set: vec![
            ManifestFile {
                file_uri: "file:///tmp/batch-0001-a.parquet".to_string(),
                file_kind: "parquet".to_string(),
                content_hash: file_a_hash.to_string(),
                file_size_bytes: 128,
                record_count: 1,
                created_at: fixed_time(1),
            },
            ManifestFile {
                file_uri: "file:///tmp/batch-0001-b.parquet".to_string(),
                file_kind: "parquet".to_string(),
                content_hash: "file-hash-b".to_string(),
                file_size_bytes: 256,
                record_count: 1,
                created_at: fixed_time(2),
            },
        ],
        content_hash: content_hash.to_string(),
        created_at: fixed_time(3),
    }
}

#[test]
fn batch_manifest_captures_replay_boundary_fields() {
    let manifest = manifest_fixture("content-hash", "file-hash-a");

    assert_eq!(manifest.batch_id, BatchId::from("batch-0001"));
    assert_eq!(manifest.record_count, 2);
    assert_eq!(manifest.file_set.len(), 2);
}

#[test]
fn manifest_checksum_changes_when_content_changes() {
    let manifest = manifest_fixture("content-hash", "file-hash-a");
    let changed = manifest_fixture("different-content", "file-hash-a");

    assert_ne!(manifest.checksum(), changed.checksum());
}

#[test]
fn replay_identity_is_stable_across_file_order() {
    let manifest = manifest_fixture("content-hash", "file-hash-a");
    let mut reordered = manifest.clone();
    reordered.file_set.reverse();

    assert_eq!(manifest.replay_identity(), reordered.replay_identity());
}

#[test]
fn replay_identity_ignores_created_at() {
    let manifest = manifest_fixture("content-hash", "file-hash-a");
    let mut later = manifest.clone();
    later.created_at = fixed_time(4);

    assert_eq!(manifest.replay_identity(), later.replay_identity());
}

#[test]
fn manifest_file_checksum_reflects_content_hash() {
    let file = ManifestFile {
        file_uri: "file:///tmp/a.parquet".to_string(),
        file_kind: "parquet".to_string(),
        content_hash: "hash-a".to_string(),
        file_size_bytes: 1,
        record_count: 1,
        created_at: fixed_time(1),
    };
    let changed = ManifestFile {
        content_hash: "hash-b".to_string(),
        ..file.clone()
    };

    assert_ne!(file.checksum(), changed.checksum());
}
