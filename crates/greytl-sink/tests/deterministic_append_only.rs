mod support;

use anyhow::Result;
use greytl_sink::{
    CommitLocator, CommitRequest, FilesystemSink, ResolvedOutcome, Sink, SinkFailpoint,
    TestDoubleSink,
};
use greytl_source::{FileSource, SnapshotRequest, SourceAdapter};
use greytl_state::{
    checkpoint_ack, checkpoint_ref, AttemptResolution, BatchStatus,
    CommitRequest as StateCommitRequest, SnapshotRef, StateStore, TestStateStore,
};
use greytl_worker_duckdb::DuckDbWorker;
use greytl_types::{
    checkpoint, BatchId, BatchManifest, ManifestFile, Operation, SourceClass, TableId, TableMode,
};
use std::collections::BTreeMap;

#[test]
fn checkpoint_advances_only_after_durable_linkage() -> Result<()> {
    block_on(async {
        let store = TestStateStore::new().await?;
        let sink = TestDoubleSink::new();
        let request = sample_append_commit_request();
        let batch_id = store.register_batch(request.manifest.clone()).await?;

        let files = request
            .manifest
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
            .begin_commit(batch_id.clone(), sample_state_commit_request())
            .await?;

        let prepared = sink.prepare_commit(with_attempt(request, &attempt.idempotency_key.to_string())).await?;
        let outcome = sink.commit(prepared).await?;
        store.resolve_commit(attempt.id.clone(), AttemptResolution::Committed).await?;
        store
            .link_checkpoint_pending(
                batch_id.clone(),
                checkpoint_ref("source-a", checkpoint("cp-3")),
                outcome.snapshot.clone(),
            )
            .await?;

        assert_eq!(store.batch_status(batch_id.clone()).await?, BatchStatus::CheckpointPending);

        store
            .mark_checkpoint_durable(
                batch_id.clone(),
                checkpoint_ack("source-a", checkpoint("cp-3"), outcome.snapshot),
            )
            .await?;

        assert_eq!(store.batch_status(batch_id).await?, BatchStatus::Checkpointed);
        Ok(())
    })
}

#[test]
fn filesystem_sink_persists_append_commit_across_reloads() -> Result<()> {
    block_on(async {
        let root = warehouse_root("filesystem-persist");
        let sink = FilesystemSink::new(root.clone());
        let request = sample_append_commit_request();

        let prepared = sink.prepare_commit(request.clone()).await?;
        let committed = sink.commit(prepared).await?;

        let reloaded = FilesystemSink::new(root);
        let snapshot = reloaded.lookup_snapshot(&committed.snapshot).await?;
        assert!(snapshot.is_some());
        Ok(())
    })
}

#[test]
fn append_only_worker_output_commits_through_filesystem_sink() -> Result<()> {
    block_on(async {
        let source = FileSource::from_fixture_dir("fixtures/reference_workload_v0/orders_events");
        let batch = source
            .snapshot(SnapshotRequest { batch_index: 1 })
            .await?
            .expect("orders_events batch fixture");
        let worker = DuckDbWorker::in_memory()?;
        let materialized = worker.materialize(batch).await?;
        let root = warehouse_root("worker-append-only");
        let sink = FilesystemSink::new(root.clone());
        let destination_path = root.join("warehouse/orders_events");
        let destination_uri = format!("file://{}", destination_path.display());
        let request = CommitRequest {
            batch_id: materialized.manifest.batch_id.clone(),
            destination_uri,
            manifest: materialized.manifest.clone(),
            idempotency_key: format!("{}:1", materialized.manifest.batch_id.as_str()).into(),
        };

        let prepared = sink.prepare_commit(request).await?;
        let outcome = sink.commit(prepared).await?;

        let reloaded = FilesystemSink::new(root);
        let snapshot = reloaded
            .lookup_snapshot(&outcome.snapshot)
            .await?
            .expect("persisted snapshot");
        assert_eq!(snapshot.batch_id, materialized.manifest.batch_id);
        assert_eq!(snapshot.record_count, materialized.manifest.record_count);
        for committed_file in &snapshot.committed_files {
            let path = file_uri_path(committed_file);
            assert!(path.starts_with(&destination_path));
            assert!(path.exists(), "committed data file should exist at {}", path.display());
        }
        Ok(())
    })
}

#[test]
fn lost_ack_append_commit_can_be_resolved_and_checkpointed() -> Result<()> {
    block_on(async {
        let store = TestStateStore::new().await?;
        let sink = TestDoubleSink::with_failpoint(SinkFailpoint::LoseAckOnce);
        let request = sample_append_commit_request();
        let batch_id = store.register_batch(request.manifest.clone()).await?;
        let files = batch_files_from_manifest(&batch_id, &request.manifest);
        store.record_files(batch_id.clone(), files).await?;
        let attempt = store
            .begin_commit(batch_id.clone(), sample_state_commit_request())
            .await?;
        let request = with_attempt(request, &attempt.idempotency_key.to_string());
        let prepared = sink.prepare_commit(request.clone()).await?;

        assert!(sink.commit(prepared).await.is_err());
        store
            .resolve_commit(attempt.id.clone(), AttemptResolution::Unknown)
            .await?;
        assert_eq!(
            store.batch_status(batch_id.clone()).await?,
            BatchStatus::CommitUncertain
        );

        store.mark_attempt_resolving(attempt.id.clone()).await?;
        let resolution = sink
            .resolve_uncertain_commit(&CommitLocator {
                destination_uri: request.destination_uri.clone(),
                idempotency_key: request.idempotency_key.clone(),
            })
            .await?;
        let outcome = match resolution {
            ResolvedOutcome::Committed(outcome) => outcome,
            other => panic!("unexpected resolution: {other:?}"),
        };

        store
            .resolve_commit(attempt.id, AttemptResolution::Committed)
            .await?;
        store
            .link_checkpoint_pending(
                batch_id.clone(),
                checkpoint_ref("source-a", checkpoint("cp-3")),
                outcome.snapshot.clone(),
            )
            .await?;
        store
            .mark_checkpoint_durable(
                batch_id.clone(),
                checkpoint_ack("source-a", checkpoint("cp-3"), outcome.snapshot),
            )
            .await?;

        assert_eq!(store.batch_status(batch_id).await?, BatchStatus::Checkpointed);
        Ok(())
    })
}

#[test]
fn filesystem_sink_duplicate_replay_reuses_snapshot_identity() -> Result<()> {
    block_on(async {
        let root = warehouse_root("filesystem-duplicate-replay");
        let sink = FilesystemSink::new(root.clone());
        let request = sample_append_commit_request();

        let first = sink.prepare_commit(request.clone()).await?;
        let first = sink.commit(first).await?;

        let reloaded = FilesystemSink::new(root);
        let second = reloaded.prepare_commit(request).await?;
        let second = reloaded.commit(second).await?;

        assert_eq!(first.snapshot_id, second.snapshot_id);
        assert_eq!(first.snapshot, second.snapshot);
        Ok(())
    })
}

fn sample_state_commit_request() -> StateCommitRequest {
    StateCommitRequest {
        destination_uri: "file:///tmp/warehouse/orders_events".to_string(),
        snapshot: SnapshotRef {
            uri: "pending://orders_events/batch-append-0001".to_string(),
        },
        actor: "integration-test".to_string(),
    }
}

fn with_attempt(mut request: CommitRequest, idempotency_key: &str) -> CommitRequest {
    request.idempotency_key = idempotency_key.to_string().into();
    request
}

fn batch_files_from_manifest(
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

fn sample_append_commit_request() -> CommitRequest {
    let source_file_uri = sample_source_file_uri("batch-append-0001");
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

fn file_uri_path(uri: &str) -> std::path::PathBuf {
    uri.strip_prefix("file://")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| panic!("expected file uri, got {uri}"))
}

fn block_on<F>(future: F) -> F::Output
where
    F: std::future::Future,
{
    support::block_on(future)
}
