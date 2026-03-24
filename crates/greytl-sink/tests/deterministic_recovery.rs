mod support;

use anyhow::Result;
use greytl_sink::{
    CommitLocator, CommitRequest, FilesystemSink, ResolvedOutcome, Sink, SinkFailpoint,
    TestDoubleSink,
};
use greytl_state::{AttemptResolution, CleanupAction, StateStore, TestStateStore};
use greytl_types::{
    checkpoint, BatchId, BatchManifest, ManifestFile, Operation, SourceClass, TableId, TableMode,
};
use std::collections::BTreeMap;

#[test]
fn commit_failure_after_files_written_leaves_batch_recovery_candidates() -> Result<()> {
    block_on(async {
        let store = TestStateStore::new().await?;
        let manifest = sample_append_commit_request().manifest;
        let batch_id = store.register_batch(manifest.clone()).await?;
        let files = manifest
            .file_set
            .iter()
            .map(|file| greytl_state::BatchFile {
                batch_id: batch_id.clone(),
                file_uri: file.file_uri.clone(),
                file_kind: file.file_kind.clone(),
                content_hash: file.content_hash.clone(),
                file_size_bytes: file.file_size_bytes,
                record_count: file.record_count,
                created_at: file.created_at.clone(),
            })
            .collect();
        store.record_files(batch_id.clone(), files).await?;
        let attempt = store
            .begin_commit(batch_id.clone(), greytl_state::CommitRequest {
                destination_uri: "file:///tmp/warehouse/orders_events".to_string(),
                snapshot: greytl_state::SnapshotRef {
                    uri: "pending://orders_events/batch-append-0001".to_string(),
                },
                actor: "integration-test".to_string(),
            })
            .await?;

        let sink = TestDoubleSink::with_failpoint(SinkFailpoint::FailCommitOnce);
        let request = with_attempt(sample_append_commit_request(), &attempt.idempotency_key.to_string());
        let prepared = sink.prepare_commit(request).await?;
        assert!(sink.commit(prepared).await.is_err());

        store
            .resolve_commit(attempt.id, AttemptResolution::FailedRetryable)
            .await?;

        assert_eq!(store.list_recovery_candidates().await?, vec![batch_id]);
        Ok(())
    })
}

#[test]
fn filesystem_sink_can_resolve_uncertain_commit_after_restart() -> Result<()> {
    block_on(async {
        let root = warehouse_root("filesystem-recovery");
        let sink = FilesystemSink::new(root.clone());
        let request = support::with_destination(
            sample_append_commit_request(),
            root.join("warehouse/orders_events"),
        );

        let prepared = sink.prepare_commit(request.clone()).await?;
        let outcome = sink.commit(prepared).await?;

        let reloaded = FilesystemSink::new(root);
        let resolution = reloaded.resolve_uncertain_commit(&outcome.attempt()).await?;
        assert!(matches!(resolution, ResolvedOutcome::Committed(_)));
        Ok(())
    })
}

#[test]
fn lost_ack_commit_is_resolved_as_committed() -> Result<()> {
    block_on(async {
        let sink = TestDoubleSink::with_failpoint(SinkFailpoint::LoseAckOnce);
        let request = sample_append_commit_request();
        let prepared = sink.prepare_commit(request.clone()).await?;

        assert!(sink.commit(prepared).await.is_err());

        let resolution = sink
            .resolve_uncertain_commit(&CommitLocator {
                destination_uri: request.destination_uri,
                idempotency_key: request.idempotency_key,
            })
            .await?;
        assert!(matches!(resolution, ResolvedOutcome::Committed(_)));
        Ok(())
    })
}

#[test]
fn ambiguous_resolution_yields_orphan_candidates_until_cleanup_is_recorded() -> Result<()> {
    block_on(async {
        let store = TestStateStore::new().await?;
        let manifest = sample_append_commit_request().manifest;
        let batch_id = store.register_batch(manifest.clone()).await?;
        let files = manifest_batch_files(&batch_id, &manifest);
        let attempt = store
            .begin_commit(batch_id.clone(), greytl_state::CommitRequest {
                destination_uri: "file:///tmp/warehouse/orders_events".to_string(),
                snapshot: greytl_state::SnapshotRef {
                    uri: "pending://orders_events/batch-append-0001".to_string(),
                },
                actor: "integration-test".to_string(),
            })
            .await?;

        store.record_files(batch_id.clone(), files.clone()).await?;
        store
            .resolve_commit(attempt.id, AttemptResolution::Ambiguous)
            .await?;

        assert_eq!(store.list_orphan_candidates().await?, files);

        store
            .record_cleanup(files[0].clone(), CleanupAction::Deleted)
            .await?;
        assert!(store.list_orphan_candidates().await?.is_empty());
        Ok(())
    })
}

fn with_attempt(mut request: CommitRequest, idempotency_key: &str) -> CommitRequest {
    request.idempotency_key = idempotency_key.to_string().into();
    request
}

fn sample_append_commit_request() -> CommitRequest {
    let source_file_uri = sample_source_file_uri("batch-append-0001-recovery");
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
                file_uri: source_file_uri,
                file_kind: "parquet".to_string(),
                content_hash: "hash-a".to_string(),
                file_size_bytes: 128,
                record_count: 3,
                created_at: fixed_time(1),
            }],
            content_hash: "content-a".to_string(),
            created_at: fixed_time(2),
        },
        idempotency_key: "batch-append-0001:append".into(),
    }
}

fn manifest_batch_files(
    batch_id: &BatchId,
    manifest: &BatchManifest,
) -> Vec<greytl_state::BatchFile> {
    manifest
        .file_set
        .iter()
        .map(|file| greytl_state::BatchFile {
            batch_id: batch_id.clone(),
            file_uri: file.file_uri.clone(),
            file_kind: file.file_kind.clone(),
            content_hash: file.content_hash.clone(),
            file_size_bytes: file.file_size_bytes,
            record_count: file.record_count,
            created_at: file.created_at.clone(),
        })
        .collect()
}

fn fixed_time(secs: u64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp(secs as i64, 0).expect("valid timestamp")
}

fn warehouse_root(name: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir().join("greytl-sink-tests").join(name);
    let _ = std::fs::remove_dir_all(&root);
    root
}

fn sample_source_file_uri(name: &str) -> String {
    let root = std::env::temp_dir().join("greytl-sink-source");
    std::fs::create_dir_all(&root).expect("create source fixture dir");
    let path = root.join(format!("{name}.parquet"));
    std::fs::write(&path, b"parquet-fixture").expect("write source fixture file");
    format!("file://{}", path.display())
}

fn block_on<F>(future: F) -> F::Output
where
    F: std::future::Future,
{
    support::block_on(future)
}
