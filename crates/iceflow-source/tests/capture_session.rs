use anyhow::Result;
use iceflow_source::{
    BatchPoll, BatchRequest, FileSource, OpenCaptureRequest, SourceAdapter, SourceCapability,
    SourceTableSelection,
};
use iceflow_types::{checkpoint, TableId, TableMode};

fn fixture_dir(name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("fixtures/reference_workload_v0")
        .join(name)
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    struct NoopWake;

    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    let waker = Waker::from(Arc::new(NoopWake));
    let mut cx = Context::from_waker(&waker);
    let mut future = Pin::from(Box::new(future));
    match future.as_mut().poll(&mut cx) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("future unexpectedly pending"),
    }
}

#[test]
fn file_source_check_reports_capabilities() -> Result<()> {
    let source = FileSource::from_fixture_dir(fixture_dir("orders_events"));
    let report = block_on(source.check())?;

    assert!(report.capabilities.contains(&SourceCapability::InitialSnapshot));
    assert!(report.capabilities.contains(&SourceCapability::Resume));
    assert!(!report.capabilities.contains(&SourceCapability::ChangeFeed));
    assert!(report.warnings.is_empty());
    Ok(())
}

#[test]
fn file_source_open_capture_returns_session() -> Result<()> {
    let source = FileSource::from_fixture_dir(fixture_dir("orders_events"));
    let req = OpenCaptureRequest {
        table: SourceTableSelection {
            table_id: TableId::new("orders_events"),
            source_schema: String::new(),
            source_table: "orders_events".to_string(),
            table_mode: TableMode::AppendOnly,
        },
        resume_from: None,
    };
    let _session = block_on(source.open_capture(req))?;
    Ok(())
}

#[test]
fn file_capture_session_polls_batches_in_order() -> Result<()> {
    let source = FileSource::from_fixture_dir(fixture_dir("orders_events"));
    let req = OpenCaptureRequest {
        table: SourceTableSelection {
            table_id: TableId::new("orders_events"),
            source_schema: String::new(),
            source_table: "orders_events".to_string(),
            table_mode: TableMode::AppendOnly,
        },
        resume_from: None,
    };
    let mut session = block_on(source.open_capture(req))?;

    let batch1 = match block_on(session.poll_batch(BatchRequest::default()))? {
        BatchPoll::Batch(batch) => batch,
        other => panic!("expected Batch, got {other:?}"),
    };
    assert_eq!(batch1.batch_label.as_deref(), Some("batch-0001.jsonl"));
    assert_eq!(batch1.records.len(), 2);

    let batch2 = match block_on(session.poll_batch(BatchRequest::default()))? {
        BatchPoll::Batch(batch) => batch,
        other => panic!("expected Batch, got {other:?}"),
    };
    assert_eq!(batch2.batch_label.as_deref(), Some("batch-0002.jsonl"));
    assert_eq!(batch2.records.len(), 2);

    let poll = block_on(session.poll_batch(BatchRequest::default()))?;
    assert!(matches!(poll, BatchPoll::Exhausted));

    block_on(session.close())?;

    Ok(())
}

#[test]
fn file_capture_session_resumes_from_checkpoint() -> Result<()> {
    let source = FileSource::from_fixture_dir(fixture_dir("orders_events"));
    let req = OpenCaptureRequest {
        table: SourceTableSelection {
            table_id: TableId::new("orders_events"),
            source_schema: String::new(),
            source_table: "orders_events".to_string(),
            table_mode: TableMode::AppendOnly,
        },
        resume_from: Some(checkpoint("cp-0011")),
    };
    let mut session = block_on(source.open_capture(req))?;

    let batch = match block_on(session.poll_batch(BatchRequest::default()))? {
        BatchPoll::Batch(batch) => batch,
        other => panic!("expected Batch, got {other:?}"),
    };
    assert_eq!(batch.batch_label.as_deref(), Some("batch-0002.jsonl"));

    let poll = block_on(session.poll_batch(BatchRequest::default()))?;
    assert!(matches!(poll, BatchPoll::Exhausted));

    block_on(session.close())?;
    Ok(())
}

#[test]
fn file_capture_session_rejects_unknown_resume_checkpoint() {
    let source = FileSource::from_fixture_dir(fixture_dir("orders_events"));
    let req = OpenCaptureRequest {
        table: SourceTableSelection {
            table_id: TableId::new("orders_events"),
            source_schema: String::new(),
            source_table: "orders_events".to_string(),
            table_mode: TableMode::AppendOnly,
        },
        resume_from: Some(checkpoint("cp-9999")),
    };
    let err =
        block_on(source.open_capture(req)).expect_err("unknown resume checkpoint should fail");
    assert!(err.to_string().contains("resume checkpoint"));
}
