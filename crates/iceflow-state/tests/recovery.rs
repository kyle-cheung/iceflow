use anyhow::Result;
use iceflow_state::{
    checkpoint_ack, checkpoint_ref, AttemptResolution, AttemptStatus, BatchStatus, CommitRequest,
    QuarantineReason, SnapshotRef, SqliteStateStore, StateStore, TestStateStore,
};
use iceflow_types::{
    checkpoint, BatchId, BatchManifest, ManifestFile, Operation, SourceClass, TableId, TableMode,
};
use std::collections::BTreeMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

#[test]
fn ambiguous_resolution_quarantines_the_batch() -> Result<()> {
    block_on(async {
        let store = TestStateStore::new().await?;
        let batch_id = store.register_batch(sample_manifest()).await?;
        let attempt = store
            .begin_commit(batch_id.clone(), sample_commit_request())
            .await?;

        store
            .resolve_commit(attempt.id.clone(), AttemptResolution::Ambiguous)
            .await?;

        assert_eq!(
            store.batch_status(batch_id).await?,
            BatchStatus::Quarantined
        );
        assert_eq!(
            store.attempt_status(attempt.id).await?,
            AttemptStatus::AmbiguousManual
        );
        Ok(())
    })
}

#[test]
fn durable_checkpoint_is_only_recorded_after_ack() -> Result<()> {
    block_on(async {
        let store = TestStateStore::new().await?;
        let manifest = sample_manifest();
        let batch_id = store.register_batch(manifest.clone()).await?;
        let attempt = store
            .begin_commit(batch_id.clone(), sample_commit_request())
            .await?;
        let snapshot = sample_snapshot();
        let pending_checkpoint = checkpoint_ref("source-a", checkpoint("cp-2"));

        store
            .resolve_commit(attempt.id.clone(), AttemptResolution::Committed)
            .await?;
        store
            .link_checkpoint_pending(
                batch_id.clone(),
                pending_checkpoint.clone(),
                snapshot.clone(),
            )
            .await?;

        assert_eq!(
            store.batch_status(batch_id.clone()).await?,
            BatchStatus::CheckpointPending
        );
        assert_eq!(store.durable_checkpoint(batch_id.clone()).await?, None);
        assert_eq!(
            store.list_recovery_candidates().await?,
            vec![batch_id.clone()]
        );

        store
            .mark_checkpoint_durable(
                batch_id.clone(),
                checkpoint_ack("source-a", checkpoint("cp-2"), snapshot),
            )
            .await?;

        assert_eq!(
            store.batch_status(batch_id.clone()).await?,
            BatchStatus::Checkpointed
        );
        assert_eq!(
            store.durable_checkpoint(batch_id.clone()).await?,
            Some(pending_checkpoint)
        );
        assert!(store.list_recovery_candidates().await?.is_empty());
        Ok(())
    })
}

#[test]
fn last_durable_checkpoint_for_table_returns_none_without_durable_checkpoints() -> Result<()> {
    block_on(async {
        let store = TestStateStore::new().await?;
        let batch_id = store.register_batch(sample_manifest()).await?;
        let attempt = store
            .begin_commit(batch_id.clone(), sample_commit_request())
            .await?;

        store
            .resolve_commit(attempt.id, AttemptResolution::Committed)
            .await?;

        assert_eq!(
            store
                .last_durable_checkpoint_for_table(&TableId::from("customer_state"))
                .await?,
            None
        );
        assert_eq!(
            store
                .last_durable_checkpoint_for_table(&TableId::from("other_table"))
                .await?,
            None
        );
        Ok(())
    })
}

#[test]
fn last_durable_checkpoint_for_table_returns_most_recent() -> Result<()> {
    block_on(async {
        let store = TestStateStore::new().await?;

        let first_manifest = sample_manifest_for("batch-0001", "customer_state", "cp-1", "cp-2", 3);
        let first_batch_id = store.register_batch(first_manifest).await?;
        durable_checkpoint_batch(
            &store,
            &first_batch_id,
            checkpoint("cp-2"),
            "delta://customer_state/version/42",
        )
        .await?;

        let second_manifest =
            sample_manifest_for("batch-0002", "customer_state", "cp-2", "cp-3", 4);
        let second_batch_id = store.register_batch(second_manifest).await?;
        durable_checkpoint_batch(
            &store,
            &second_batch_id,
            checkpoint("cp-3"),
            "delta://customer_state/version/43",
        )
        .await?;

        assert_eq!(
            store
                .last_durable_checkpoint_for_table(&TableId::from("customer_state"))
                .await?,
            Some(checkpoint_ref("source-a", checkpoint("cp-3")))
        );
        assert_eq!(
            store
                .last_durable_checkpoint_for_table(&TableId::from("other_table"))
                .await?,
            None
        );
        Ok(())
    })
}

#[test]
fn last_durable_checkpoint_for_table_prefers_newer_batch_over_later_link_time() -> Result<()> {
    block_on(async {
        let store = TestStateStore::new().await?;

        let first_batch_id = store
            .register_batch(sample_manifest_for(
                "batch-0001",
                "customer_state",
                "cp-1",
                "cp-2",
                3,
            ))
            .await?;
        let second_batch_id = store
            .register_batch(sample_manifest_for(
                "batch-0002",
                "customer_state",
                "cp-2",
                "cp-3",
                4,
            ))
            .await?;

        durable_checkpoint_batch(
            &store,
            &second_batch_id,
            checkpoint("cp-3"),
            "delta://customer_state/version/43",
        )
        .await?;
        durable_checkpoint_batch(
            &store,
            &first_batch_id,
            checkpoint("cp-2"),
            "delta://customer_state/version/42",
        )
        .await?;

        assert_eq!(
            store
                .last_durable_checkpoint_for_table(&TableId::from("customer_state"))
                .await?,
            Some(checkpoint_ref("source-a", checkpoint("cp-3")))
        );
        Ok(())
    })
}

#[test]
fn resolving_attempts_are_recovery_candidates() -> Result<()> {
    block_on(async {
        let store = TestStateStore::new().await?;
        let batch_id = store.register_batch(sample_manifest()).await?;
        let attempt = store
            .begin_commit(batch_id.clone(), sample_commit_request())
            .await?;

        store
            .force_attempt_status(attempt.id.clone(), AttemptStatus::Resolving)
            .await?;

        assert_eq!(store.list_recovery_candidates().await?, vec![batch_id]);
        Ok(())
    })
}

#[test]
fn failed_retryable_commits_require_revalidation_before_retry() -> Result<()> {
    block_on(async {
        let store = TestStateStore::new().await?;
        let batch_id = store.register_batch(sample_manifest()).await?;
        let attempt = store
            .begin_commit(batch_id.clone(), sample_commit_request())
            .await?;

        store
            .resolve_commit(attempt.id.clone(), AttemptResolution::FailedRetryable)
            .await?;

        assert_eq!(
            store.batch_status(batch_id.clone()).await?,
            BatchStatus::SchemaRevalidating
        );
        assert_eq!(
            store.attempt_status(attempt.id).await?,
            AttemptStatus::FailedRetryable
        );

        store.mark_retry_ready(batch_id.clone()).await?;

        assert_eq!(store.batch_status(batch_id).await?, BatchStatus::RetryReady);
        Ok(())
    })
}

#[test]
fn mark_attempt_resolving_moves_batch_to_commit_uncertain() -> Result<()> {
    block_on(async {
        let store = TestStateStore::new().await?;
        let batch_id = store.register_batch(sample_manifest()).await?;
        let attempt = store
            .begin_commit(batch_id.clone(), sample_commit_request())
            .await?;

        store.mark_attempt_resolving(attempt.id.clone()).await?;

        assert_eq!(
            store.attempt_status(attempt.id).await?,
            AttemptStatus::Resolving
        );
        assert_eq!(
            store.batch_status(batch_id).await?,
            BatchStatus::CommitUncertain
        );
        Ok(())
    })
}

#[test]
fn sqlite_connections_use_full_synchronous_mode() -> Result<()> {
    block_on(async {
        let store = TestStateStore::new().await?;

        assert_eq!(store.synchronous_mode().await?, 2);
        Ok(())
    })
}

#[test]
fn read_only_checkpoint_lookup_does_not_create_parent_dirs_for_missing_db() -> Result<()> {
    block_on(async {
        let state_path = next_temp_state_db_path("read-only-missing-db");
        let state_dir = state_path.parent().expect("state dir").to_path_buf();

        assert!(!state_dir.exists());
        let err = SqliteStateStore::read_only_last_durable_checkpoint_for_existing_db(
            &state_path,
            &TableId::from("customer_state"),
        )
        .await
        .expect_err("missing db should fail");

        assert!(
            err.to_string().contains("sqlite open failed"),
            "expected sqlite open failure, got {err}"
        );
        assert!(
            !state_dir.exists(),
            "read-only lookup should not create parent dir {:?}",
            state_dir
        );
        Ok(())
    })
}

#[test]
fn quarantined_batches_remain_recovery_candidates() -> Result<()> {
    block_on(async {
        let store = TestStateStore::new().await?;
        let batch_id = store.register_batch(sample_manifest()).await?;

        store
            .mark_quarantine(batch_id.clone(), QuarantineReason::SchemaViolation)
            .await?;

        assert_eq!(
            store.batch_status(batch_id.clone()).await?,
            BatchStatus::Quarantined
        );
        assert_eq!(store.list_recovery_candidates().await?, vec![batch_id]);
        Ok(())
    })
}

#[test]
fn durable_checkpoint_cannot_be_downgraded_to_pending() -> Result<()> {
    block_on(async {
        let store = TestStateStore::new().await?;
        let batch_id = store.register_batch(sample_manifest()).await?;
        let attempt = store
            .begin_commit(batch_id.clone(), sample_commit_request())
            .await?;
        let snapshot = sample_snapshot();
        let checkpoint_ref_value = checkpoint_ref("source-a", checkpoint("cp-2"));

        store
            .resolve_commit(attempt.id, AttemptResolution::Committed)
            .await?;
        store
            .link_checkpoint_pending(
                batch_id.clone(),
                checkpoint_ref_value.clone(),
                snapshot.clone(),
            )
            .await?;
        store
            .mark_checkpoint_durable(
                batch_id.clone(),
                checkpoint_ack("source-a", checkpoint("cp-2"), snapshot.clone()),
            )
            .await?;

        assert!(store
            .link_checkpoint_pending(batch_id.clone(), checkpoint_ref_value.clone(), snapshot)
            .await
            .is_err());
        assert_eq!(
            store.batch_status(batch_id.clone()).await?,
            BatchStatus::Checkpointed
        );
        assert_eq!(
            store.durable_checkpoint(batch_id).await?,
            Some(checkpoint_ref_value)
        );
        Ok(())
    })
}

fn sample_commit_request() -> CommitRequest {
    CommitRequest {
        destination_uri: "s3://warehouse/customer_state".to_string(),
        snapshot: sample_snapshot(),
        actor: "integration-test".to_string(),
    }
}

fn sample_snapshot() -> SnapshotRef {
    SnapshotRef {
        uri: "delta://customer_state/version/42".to_string(),
    }
}

fn sample_manifest() -> BatchManifest {
    sample_manifest_for("batch-0001", "customer_state", "cp-1", "cp-2", 3)
}

fn sample_manifest_for(
    batch_id: &str,
    table_id: &str,
    checkpoint_start: &str,
    checkpoint_end: &str,
    created_at_offset_secs: u64,
) -> BatchManifest {
    BatchManifest {
        batch_id: BatchId::from(batch_id),
        table_id: TableId::from(table_id),
        table_mode: TableMode::KeyedUpsert,
        source_id: "source-a".to_string(),
        source_class: SourceClass::DatabaseCdc,
        source_checkpoint_start: checkpoint(checkpoint_start),
        source_checkpoint_end: checkpoint(checkpoint_end),
        ordering_field: "source_position".to_string(),
        ordering_min: 1,
        ordering_max: 7,
        schema_version: 3,
        schema_fingerprint: "schema-fingerprint".to_string(),
        record_count: 2,
        op_counts: BTreeMap::from([(Operation::Upsert, 2)]),
        file_set: vec![ManifestFile {
            file_uri: format!("file:///tmp/{batch_id}.parquet"),
            file_kind: "parquet".to_string(),
            content_hash: format!("hash-{batch_id}"),
            file_size_bytes: 128,
            record_count: 2,
            created_at: fixed_time(created_at_offset_secs),
        }],
        content_hash: format!("content-hash-{batch_id}"),
        created_at: fixed_time(created_at_offset_secs),
    }
}

fn fixed_time(offset_secs: u64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp(offset_secs as i64, 0).expect("valid timestamp")
}

fn next_temp_state_db_path(label: &str) -> PathBuf {
    static NEXT_TEMP_STATE_DB_ID: AtomicU64 = AtomicU64::new(0);

    std::env::temp_dir().join(format!(
        "iceflow-state-{label}-{}-{}/state.sqlite3",
        std::process::id(),
        NEXT_TEMP_STATE_DB_ID.fetch_add(1, Ordering::Relaxed)
    ))
}

async fn durable_checkpoint_batch(
    store: &TestStateStore,
    batch_id: &BatchId,
    checkpoint_id: iceflow_types::CheckpointId,
    snapshot_uri: &str,
) -> Result<()> {
    let attempt = store
        .begin_commit(batch_id.clone(), sample_commit_request())
        .await?;
    let snapshot = SnapshotRef {
        uri: snapshot_uri.to_string(),
    };

    store
        .resolve_commit(attempt.id, AttemptResolution::Committed)
        .await?;
    store
        .link_checkpoint_pending(
            batch_id.clone(),
            checkpoint_ref("source-a", checkpoint_id.clone()),
            snapshot.clone(),
        )
        .await?;
    store
        .mark_checkpoint_durable(
            batch_id.clone(),
            checkpoint_ack("source-a", checkpoint_id, snapshot),
        )
        .await
}

fn block_on<F>(future: F) -> F::Output
where
    F: Future,
{
    let waker = unsafe { Waker::from_raw(dummy_raw_waker()) };
    let mut future = pin!(future);
    let mut cx = Context::from_waker(&waker);

    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

unsafe fn dummy_raw_waker() -> RawWaker {
    RawWaker::new(std::ptr::null(), &DUMMY_WAKER_VTABLE)
}

unsafe fn clone_dummy(_: *const ()) -> RawWaker {
    dummy_raw_waker()
}

unsafe fn wake_dummy(_: *const ()) {}

static DUMMY_WAKER_VTABLE: RawWakerVTable =
    RawWakerVTable::new(clone_dummy, wake_dummy, wake_dummy, wake_dummy);
