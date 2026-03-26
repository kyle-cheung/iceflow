mod support;

use anyhow::Result;
use greytl_sink::{CommitRequest, PolarisSink, Sink};
use greytl_types::{
    checkpoint, BatchId, BatchManifest, ManifestFile, Operation, SourceClass, TableId, TableMode,
};
use std::collections::BTreeMap;

#[test]
fn polaris_sink_rejects_keyed_upsert_for_now() -> Result<()> {
    let sink = PolarisSink::new(
        "http://127.0.0.1:8181/api/catalog",
        "quickstart_catalog",
        "customer_state",
        "file:///tmp/warehouse/customer_state",
    );
    let request = sample_keyed_upsert_commit_request();

    let err = support::block_on(async { sink.prepare_commit(request).await })
        .expect_err("keyed upsert should be constrained");

    assert!(err.to_string().contains("append_only"));
    Ok(())
}

fn sample_keyed_upsert_commit_request() -> CommitRequest {
    CommitRequest {
        batch_id: BatchId::from("batch-keyed-upsert-0001"),
        destination_uri: "file:///tmp/warehouse/customer_state".to_string(),
        manifest: BatchManifest {
            batch_id: BatchId::from("batch-keyed-upsert-0001"),
            table_id: TableId::from("customer_state"),
            table_mode: TableMode::KeyedUpsert,
            source_id: "source-a".to_string(),
            source_class: SourceClass::DatabaseCdc,
            source_checkpoint_start: checkpoint("cp-10"),
            source_checkpoint_end: checkpoint("cp-25"),
            ordering_field: "source_position".to_string(),
            ordering_min: 10,
            ordering_max: 25,
            schema_version: 1,
            schema_fingerprint: "schema-v1".to_string(),
            record_count: 3,
            op_counts: BTreeMap::from([(Operation::Upsert, 2), (Operation::Delete, 1)]),
            file_set: vec![ManifestFile {
                file_uri: support::sample_source_file_uri("polaris-keyed-upsert"),
                file_kind: "parquet".to_string(),
                content_hash: "hash-keyed-upsert".to_string(),
                file_size_bytes: 128,
                record_count: 3,
                created_at: support::fixed_time(1),
            }],
            content_hash: "content-keyed-upsert".to_string(),
            created_at: support::fixed_time(2),
        },
        idempotency_key: "batch-keyed-upsert-0001:keyed-upsert".into(),
    }
}
