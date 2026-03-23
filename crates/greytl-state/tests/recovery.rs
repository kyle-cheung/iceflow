use anyhow::Result;
use greytl_state::{
    checkpoint_ack, checkpoint_ref, AttemptResolution, AttemptStatus, BatchStatus, CommitRequest,
    SnapshotRef, StateStore, TestStateStore,
};
use greytl_types::{
    checkpoint, BatchId, BatchManifest, ManifestFile, Operation, SourceClass, TableId, TableMode,
};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
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
    BatchManifest {
        batch_id: BatchId::from("batch-0001"),
        table_id: TableId::from("customer_state"),
        table_mode: TableMode::KeyedUpsert,
        source_id: "source-a".to_string(),
        source_class: SourceClass::DatabaseCdc,
        source_checkpoint_start: checkpoint("cp-1"),
        source_checkpoint_end: checkpoint("cp-2"),
        ordering_field: "source_position".to_string(),
        ordering_min: 1,
        ordering_max: 7,
        schema_version: 3,
        schema_fingerprint: "schema-fingerprint".to_string(),
        record_count: 2,
        op_counts: BTreeMap::from([(Operation::Upsert, 2)]),
        file_set: vec![ManifestFile {
            file_uri: "file:///tmp/batch-0001.parquet".to_string(),
            file_kind: "parquet".to_string(),
            content_hash: "hash-a".to_string(),
            file_size_bytes: 128,
            record_count: 2,
            created_at: fixed_time(1),
        }],
        content_hash: "content-hash".to_string(),
        created_at: fixed_time(3),
    }
}

fn fixed_time(offset_secs: u64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp(offset_secs as i64, 0).expect("valid timestamp")
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
