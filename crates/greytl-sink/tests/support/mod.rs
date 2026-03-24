use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use greytl_sink::CommitRequest;

pub fn block_on<F>(future: F) -> F::Output
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

#[allow(dead_code)]
pub fn with_destination(
    mut request: CommitRequest,
    destination_path: std::path::PathBuf,
) -> CommitRequest {
    request.destination_uri = format!("file://{}", destination_path.display());
    request
}

unsafe fn dummy_raw_waker() -> RawWaker {
    RawWaker::new(std::ptr::null(), &DUMMY_WAKER_VTABLE)
}

unsafe fn clone_dummy(_: *const ()) -> RawWaker {
    unsafe { dummy_raw_waker() }
}

unsafe fn wake_dummy(_: *const ()) {}

static DUMMY_WAKER_VTABLE: RawWakerVTable =
    RawWakerVTable::new(clone_dummy, wake_dummy, wake_dummy, wake_dummy);
