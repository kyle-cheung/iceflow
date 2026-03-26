mod support;

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

#[test]
fn support_block_on_waits_for_wake_before_repolling() {
    struct ReadyOnWake {
        started: bool,
        woke: Arc<AtomicBool>,
    }

    impl Future for ReadyOnWake {
        type Output = &'static str;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.woke.load(Ordering::Acquire) {
                return Poll::Ready("done");
            }

            if self.started {
                panic!("future was polled again before its waker fired");
            }

            self.started = true;
            let woke = Arc::clone(&self.woke);
            let waker = cx.waker().clone();
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(10));
                woke.store(true, Ordering::Release);
                waker.wake();
            });
            Poll::Pending
        }
    }

    assert_eq!(
        support::block_on(ReadyOnWake {
            started: false,
            woke: Arc::new(AtomicBool::new(false)),
        }),
        "done"
    );
}
