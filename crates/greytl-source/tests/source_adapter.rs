use anyhow::Result;
use greytl_source::{CheckpointAck, FileSource, SnapshotRef, SnapshotRequest, SourceAdapter};
use greytl_types::{Operation, TableMode};
use std::path::{Path, PathBuf};

#[test]
fn file_source_emits_monotonic_ordering_for_customer_state() -> Result<()> {
    let source = FileSource::from_fixture_dir(fixture_dir("customer_state"));

    let spec = block_on(source.spec());
    assert_eq!(spec.table_mode, TableMode::KeyedUpsert);

    let batch = block_on(source.snapshot(SnapshotRequest { batch_index: 1 }))?
        .expect("first batch should exist");

    assert!(is_monotonic(
        batch.records.iter().map(|record| record.ordering_value)
    ));

    Ok(())
}

#[test]
fn file_source_discovers_append_only_orders_events() -> Result<()> {
    let source = FileSource::from_fixture_dir(fixture_dir("orders_events"));

    let spec = block_on(source.spec());
    let check = block_on(source.check())?;
    let report = block_on(source.discover())?;

    assert_eq!(spec.table_id, "orders_events");
    assert_eq!(spec.table_mode, TableMode::AppendOnly);
    assert_eq!(check.batch_count, 2);
    assert_eq!(check.record_count, 4);
    assert_eq!(
        report.batch_files,
        vec![
            "batch-0001.jsonl".to_string(),
            "batch-0002.jsonl".to_string()
        ]
    );

    Ok(())
}

#[test]
fn file_source_rejects_checkpoint_regression() -> Result<()> {
    let source = FileSource::from_fixture_dir(fixture_dir("customer_state"));

    block_on(source.checkpoint(CheckpointAck {
        source_id: "file.customer_state".to_string(),
        checkpoint: "batch-0002".into(),
        snapshot: SnapshotRef {
            uri: "file:///snapshots/customer_state/2".to_string(),
        },
    }))?;

    let err = block_on(source.checkpoint(CheckpointAck {
        source_id: "file.customer_state".to_string(),
        checkpoint: "batch-0001".into(),
        snapshot: SnapshotRef {
            uri: "file:///snapshots/customer_state/1".to_string(),
        },
    }))
    .expect_err("checkpoint regression should fail");

    assert_eq!(err.to_string(), "checkpoint regression is not allowed");
    Ok(())
}

#[test]
fn file_source_preserves_delete_records_in_customer_state() -> Result<()> {
    let source = FileSource::from_fixture_dir(fixture_dir("customer_state"));
    let second = block_on(source.snapshot(SnapshotRequest { batch_index: 2 }))?
        .expect("second batch should exist");

    assert!(second
        .records
        .iter()
        .any(|record| record.op == Operation::Delete));
    Ok(())
}

#[test]
fn source_adapter_snapshot_by_batch_index_is_deterministic() -> Result<()> {
    let source = FileSource::from_fixture_dir(fixture_dir("orders_events"));

    let batch = block_on(source.snapshot(SnapshotRequest { batch_index: 2 }))?
        .expect("second batch should exist");

    let order_ids: Vec<_> = batch
        .records
        .iter()
        .map(|record| match &record.after {
            Some(serde_json::Value::Object(fields)) => fields
                .get("order_id")
                .and_then(|value| match value {
                    serde_json::Value::String(value) => Some(value.clone()),
                    _ => None,
                })
                .unwrap_or_default(),
            _ => String::new(),
        })
        .collect();

    assert_eq!(
        order_ids,
        vec!["order-003".to_string(), "order-004".to_string()]
    );
    Ok(())
}

fn fixture_dir(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/reference_workload_v0")
        .join(name)
}

fn is_monotonic(values: impl IntoIterator<Item = i64>) -> bool {
    let mut previous = None;
    for value in values {
        if let Some(last) = previous {
            if value < last {
                return false;
            }
        }
        previous = Some(value);
    }

    true
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    struct NoopWake;

    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    let waker = Waker::from(Arc::new(NoopWake));
    let mut future = Pin::from(Box::new(future));
    let mut context = Context::from_waker(&waker);

    loop {
        match Future::poll(future.as_mut(), &mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}
