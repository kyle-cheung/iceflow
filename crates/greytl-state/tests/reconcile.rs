use anyhow::Result;
use greytl_state::{
    AttemptResolution, BatchFile, CleanupAction, CommitRequest, SnapshotRef, StateStore,
    TestStateStore,
};
use greytl_types::{
    checkpoint, BatchId, BatchManifest, ManifestFile, Operation, SourceClass, TableId, TableMode,
};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

#[test]
fn quarantined_batch_files_become_orphans_until_cleanup_is_recorded() -> Result<()> {
    block_on(async {
        let store = TestStateStore::new().await?;
        let manifest = sample_manifest();
        let batch_id = store.register_batch(manifest.clone()).await?;
        let files = sample_batch_files(&batch_id);
        let attempt = store
            .begin_commit(batch_id.clone(), sample_commit_request())
            .await?;

        store.record_files(batch_id, files.clone()).await?;
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

fn sample_commit_request() -> CommitRequest {
    CommitRequest {
        destination_uri: "s3://warehouse/customer_state".to_string(),
        snapshot: SnapshotRef {
            uri: "delta://customer_state/version/42".to_string(),
        },
        actor: "integration-test".to_string(),
    }
}

fn sample_batch_files(batch_id: &BatchId) -> Vec<BatchFile> {
    vec![BatchFile {
        batch_id: batch_id.clone(),
        file_uri: "file:///tmp/batch-0001.parquet".to_string(),
        file_kind: "parquet".to_string(),
        content_hash: "hash-a".to_string(),
        file_size_bytes: 128,
        record_count: 2,
        created_at: fixed_time(1),
    }]
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
