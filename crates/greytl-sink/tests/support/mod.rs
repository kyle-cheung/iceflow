use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use greytl_sink::CommitRequest;

pub mod polaris_mock;

pub fn block_on<F>(future: F) -> F::Output
where
    F: Future,
{
    let parker = Arc::new(ThreadWaker {
        thread: std::thread::current(),
        notified: AtomicBool::new(false),
    });
    let waker = Waker::from(Arc::clone(&parker));
    let mut future = pin!(future);
    let mut cx = Context::from_waker(&waker);

    loop {
        parker.notified.store(false, Ordering::Release);
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(value) => return value,
            Poll::Pending => {
                while !parker.notified.swap(false, Ordering::AcqRel) {
                    std::thread::park();
                }
            }
        }
    }
}

struct ThreadWaker {
    thread: std::thread::Thread,
    notified: AtomicBool,
}

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.notified.store(true, Ordering::Release);
        self.thread.unpark();
    }
}

#[allow(dead_code)]
pub fn with_destination(
    mut request: CommitRequest,
    destination_path: std::path::PathBuf,
) -> CommitRequest {
    request.destination_uri = format!("file://{}", destination_path.display());
    request
}

#[allow(dead_code)]
pub fn fixed_time(secs: u64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp(secs as i64, 0).expect("valid timestamp")
}

#[allow(dead_code)]
pub fn sample_source_file_uri(name: &str) -> String {
    let root = std::env::temp_dir().join("greytl-sink-source");
    std::fs::create_dir_all(&root).expect("create source fixture dir");
    let path = root.join(format!("{name}.parquet"));
    std::fs::write(&path, b"parquet-fixture").expect("write source fixture file");
    format!("file://{}", path.display())
}
