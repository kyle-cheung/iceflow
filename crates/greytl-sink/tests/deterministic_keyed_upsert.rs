mod support;

use anyhow::Result;
use greytl_sink::{plan_keyed_upsert_commit, CommitRequest, IdempotencyKey, Sink, TestDoubleSink};
use greytl_source::SourceBatch;
use greytl_types::{
    checkpoint, key, ordering, table_id, BatchId, BatchManifest, LogicalMutation, ManifestFile,
    Operation, SourceClass, TableId, TableMode,
};
use greytl_worker_duckdb::DuckDbWorker;
use serde_json::json;
use std::collections::BTreeMap;

#[test]
fn keyed_upsert_latest_wins_and_plans_equality_deletes_plus_appends() -> Result<()> {
    block_on(async {
        let worker = DuckDbWorker::in_memory()?;
        let normalized = worker.normalize(sample_keyed_upsert_batch()).await?;

        assert_eq!(normalized.record_count(), 3);
        assert_eq!(normalized.records()[0].key, key([("customer_id", 1)]));
        assert_eq!(normalized.records()[0].ordering_value, 20);
        assert_eq!(normalized.records()[0].op, greytl_types::Operation::Delete);
        assert!(normalized.records()[0].after.is_none());

        let planned = plan_keyed_upsert_commit(normalized.records())?;
        assert_eq!(planned.equality_delete_keys.len(), 3);
        assert_eq!(planned.append_rows.len(), 2);
        Ok(())
    })
}

#[test]
fn keyed_upsert_plan_rejects_unreduced_rows() -> Result<()> {
    let batch = sample_keyed_upsert_batch();
    let err = plan_keyed_upsert_commit(&batch.records)
        .expect_err("planner should reject unreduced duplicate keys");

    assert!(err
        .to_string()
        .contains("normalized latest-per-key records"));
    Ok(())
}

#[test]
fn keyed_upsert_delete_tombstones_keep_after_null_and_do_not_append() -> Result<()> {
    block_on(async {
        let worker = DuckDbWorker::in_memory()?;
        let normalized = worker.normalize(sample_delete_tombstone_batch()).await?;

        assert_eq!(normalized.record_count(), 1);
        assert_eq!(normalized.records()[0].key, key([("customer_id", 4)]));
        assert!(normalized.records()[0].after.is_none());

        let planned = plan_keyed_upsert_commit(normalized.records())?;
        assert_eq!(planned.equality_delete_keys.len(), 1);
        assert!(planned.append_rows.is_empty());
        Ok(())
    })
}

#[test]
fn keyed_upsert_duplicate_replay_is_idempotent() -> Result<()> {
    block_on(async {
        let sink = TestDoubleSink::new();
        let request = sample_keyed_upsert_commit_request("schema-v1");

        let first = sink
            .commit(sink.prepare_commit(request.clone()).await?)
            .await?;
        let second = sink.commit(sink.prepare_commit(request).await?).await?;

        assert_eq!(first.snapshot_id, second.snapshot_id);
        assert_eq!(first.snapshot, second.snapshot);
        Ok(())
    })
}

#[test]
fn keyed_upsert_ordering_violation_is_quarantined() -> Result<()> {
    block_on(async {
        let worker = DuckDbWorker::in_memory()?;
        let err = worker
            .normalize(sample_ordering_violation_batch())
            .await
            .expect_err("ordering violation should be rejected");

        assert!(err.to_string().contains("ordering violation quarantine"));
        Ok(())
    })
}

#[test]
fn keyed_upsert_schema_change_during_retry_is_rejected() -> Result<()> {
    block_on(async {
        let sink = TestDoubleSink::new();
        let request = sample_keyed_upsert_commit_request("schema-v1");

        let first = sink.prepare_commit(request.clone()).await?;
        sink.commit(first).await?;

        let retry_request = with_schema_version(request, 2);
        let prepared_retry = sink.prepare_commit(retry_request).await?;
        let err = sink
            .commit(prepared_retry)
            .await
            .expect_err("schema change should be rejected on retry");

        assert!(err.to_string().contains("different manifest"));
        Ok(())
    })
}

fn sample_keyed_upsert_batch() -> SourceBatch {
    SourceBatch {
        batch_file: "fixtures/customer_state/batch-0001.jsonl".to_string(),
        records: vec![
            keyed_upsert(1, 10, Some(json!({ "customer_id": 1, "status": "trial" }))),
            keyed_upsert(2, 15, Some(json!({ "customer_id": 2, "status": "active" }))),
            keyed_delete(1, 20),
            keyed_upsert(3, 25, Some(json!({ "customer_id": 3, "status": "trial" }))),
        ],
    }
}

fn sample_delete_tombstone_batch() -> SourceBatch {
    SourceBatch {
        batch_file: "fixtures/customer_state/batch-0002.jsonl".to_string(),
        records: vec![keyed_delete(4, 30)],
    }
}

fn sample_ordering_violation_batch() -> SourceBatch {
    SourceBatch {
        batch_file: "fixtures/customer_state/batch-0003.jsonl".to_string(),
        records: vec![
            keyed_upsert(1, 20, Some(json!({ "customer_id": 1, "status": "active" }))),
            keyed_upsert(1, 10, Some(json!({ "customer_id": 1, "status": "trial" }))),
        ],
    }
}

fn sample_keyed_upsert_commit_request(schema_fingerprint: &str) -> CommitRequest {
    let source_file_uri = support::sample_source_file_uri("keyed-upsert");
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
            schema_fingerprint: schema_fingerprint.to_string(),
            record_count: 3,
            op_counts: BTreeMap::from([(Operation::Upsert, 2), (Operation::Delete, 1)]),
            file_set: vec![ManifestFile {
                file_uri: source_file_uri,
                file_kind: "parquet".to_string(),
                content_hash: "hash-keyed-upsert".to_string(),
                file_size_bytes: 128,
                record_count: 3,
                created_at: support::fixed_time(1),
            }],
            content_hash: "content-keyed-upsert".to_string(),
            created_at: support::fixed_time(2),
        },
        idempotency_key: IdempotencyKey::from("batch-keyed-upsert-0001:keyed-upsert"),
    }
}

fn with_schema_version(mut request: CommitRequest, schema_version: i32) -> CommitRequest {
    request.manifest.schema_version = schema_version;
    request
}

fn keyed_upsert(
    customer_id: i64,
    ordering_value: i64,
    after: Option<serde_json::Value>,
) -> LogicalMutation {
    let builder = LogicalMutation::upsert(
        table_id("customer_state"),
        "source-a",
        SourceClass::DatabaseCdc,
        TableMode::KeyedUpsert,
        key([("customer_id", customer_id)]),
        ordering("source_position", ordering_value),
        checkpoint(format!("cp-{ordering_value}")),
        1,
        support::fixed_time(1),
        BTreeMap::new(),
    );

    match after {
        Some(after) => builder.with_after(after).build().expect("valid upsert"),
        None => builder.build().expect("valid upsert"),
    }
}

fn keyed_delete(customer_id: i64, ordering_value: i64) -> LogicalMutation {
    LogicalMutation::delete(
        table_id("customer_state"),
        "source-a",
        SourceClass::DatabaseCdc,
        TableMode::KeyedUpsert,
        key([("customer_id", customer_id)]),
        ordering("source_position", ordering_value),
        checkpoint(format!("cp-{ordering_value}")),
        1,
        support::fixed_time(1),
        BTreeMap::new(),
    )
    .build()
    .expect("valid delete")
}

fn block_on<F>(future: F) -> F::Output
where
    F: std::future::Future,
{
    support::block_on(future)
}
