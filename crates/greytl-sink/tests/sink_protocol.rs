mod support;

use anyhow::Result;
use greytl_sink::{CommitRequest, LookupResult, ResolvedOutcome, Sink, TestDoubleSink};
use greytl_types::{
    checkpoint, BatchId, BatchManifest, ManifestFile, Operation, SourceClass, TableId, TableMode,
};
use std::collections::BTreeMap;

#[test]
fn duplicate_append_only_replay_reuses_the_same_commit_identity() -> Result<()> {
    block_on(async {
        let sink = TestDoubleSink::new();
        let request = sample_append_commit_request();

        let prepared = sink.prepare_commit(request.clone()).await?;
        let first = sink.commit(prepared.clone()).await?;

        let lookup = sink.lookup_commit(&request.idempotency_key).await?;
        assert!(matches!(lookup, LookupResult::Found(_)));

        let second = sink.resolve_uncertain_commit(&first.attempt()).await?;
        match second {
            ResolvedOutcome::Committed(second) => {
                assert_eq!(first.snapshot_id, second.snapshot_id);
            }
            other => panic!("unexpected resolution: {other:?}"),
        }
        Ok(())
    })
}

#[test]
fn uncertain_commit_resolution_is_scoped_to_destination_uri() -> Result<()> {
    block_on(async {
        let sink = TestDoubleSink::new();
        let request = sample_append_commit_request();
        let prepared = sink.prepare_commit(request.clone()).await?;
        let committed = sink.commit(prepared).await?;

        let wrong_destination = greytl_sink::CommitLocator {
            destination_uri: "file:///tmp/warehouse/other_table".to_string(),
            idempotency_key: committed.idempotency_key,
        };
        let resolution = sink.resolve_uncertain_commit(&wrong_destination).await?;
        assert!(matches!(resolution, ResolvedOutcome::NotCommitted));
        Ok(())
    })
}

#[test]
fn lookup_commit_rejects_ambiguous_idempotency_keys_across_destinations() -> Result<()> {
    block_on(async {
        let sink = TestDoubleSink::new();
        let first = sample_append_commit_request();
        let mut second = sample_append_commit_request();
        second.destination_uri = "file:///tmp/warehouse/orders_events_copy".to_string();

        let first = sink.prepare_commit(first).await?;
        sink.commit(first).await?;
        let second = sink.prepare_commit(second).await?;
        sink.commit(second).await?;

        assert!(sink
            .lookup_commit(&"batch-append-0001:append".into())
            .await
            .is_err());
        Ok(())
    })
}

#[test]
fn prepare_commit_normalizes_trailing_slashes_in_destination_uri() -> Result<()> {
    block_on(async {
        let sink = TestDoubleSink::new();
        let first = sample_append_commit_request();
        let mut second = sample_append_commit_request();
        second.destination_uri = "file:///tmp/warehouse/orders_events/".to_string();

        let first = sink.prepare_commit(first).await?;
        let second = sink.prepare_commit(second).await?;

        assert_eq!(first.snapshot_id, second.snapshot_id);
        assert_eq!(first.snapshot, second.snapshot);
        Ok(())
    })
}

fn sample_append_commit_request() -> CommitRequest {
    CommitRequest {
        batch_id: BatchId::from("batch-append-0001"),
        destination_uri: "file:///tmp/warehouse/orders_events".to_string(),
        manifest: BatchManifest {
            batch_id: BatchId::from("batch-append-0001"),
            table_id: TableId::from("orders_events"),
            table_mode: TableMode::AppendOnly,
            source_id: "source-a".to_string(),
            source_class: SourceClass::FileOrObjectDrop,
            source_checkpoint_start: checkpoint("cp-1"),
            source_checkpoint_end: checkpoint("cp-3"),
            ordering_field: "line_number".to_string(),
            ordering_min: 11,
            ordering_max: 13,
            schema_version: 1,
            schema_fingerprint: "schema".to_string(),
            record_count: 3,
            op_counts: BTreeMap::from([(Operation::Insert, 3)]),
            file_set: vec![ManifestFile {
                file_uri: "file:///tmp/batch-append-0001.parquet".to_string(),
                file_kind: "parquet".to_string(),
                content_hash: "hash-a".to_string(),
                file_size_bytes: 128,
                record_count: 3,
                created_at: support::fixed_time(1),
            }],
            content_hash: "content-a".to_string(),
            created_at: support::fixed_time(2),
        },
        idempotency_key: "batch-append-0001:append".into(),
    }
}

fn block_on<F>(future: F) -> F::Output
where
    F: std::future::Future,
{
    support::block_on(future)
}
