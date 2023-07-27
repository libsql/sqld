use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use futures::{Future, FutureExt};
use parking_lot::Mutex;
use tokio::time::{sleep_until, Sleep};

pub fn timeout_monitor() -> (TimeoutMonitor, TimeoutNotifier) {
    let inner = Arc::new(Mutex::new(TimeoutInner {
        sleep: Box::pin(sleep_until(Instant::now().into())),
        enabled: false,
        waker: None,
    }));

    (
        TimeoutMonitor {
            inner: inner.clone(),
        },
        TimeoutNotifier { inner },
    )
}

pub struct TimeoutMonitor {
    inner: Arc<Mutex<TimeoutInner>>,
}

pub struct TimeoutNotifier {
    inner: Arc<Mutex<TimeoutInner>>,
}

impl TimeoutNotifier {
    pub fn disable(&self) {
        self.inner.lock().enabled = false;
    }

    pub fn timeout_at(&self, at: Instant) {
        let mut inner = self.inner.lock();
        inner.enabled = true;
        inner.sleep.as_mut().reset(at.into());
        if let Some(waker) = inner.waker.take() {
            waker.wake()
        }
    }
}

struct TimeoutInner {
    sleep: Pin<Box<Sleep>>,
    enabled: bool,
    waker: Option<Waker>,
}

impl Future for TimeoutMonitor {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut inner = self.inner.lock();
        if inner.enabled {
            inner.sleep.poll_unpin(cx)
        } else {
            inner.waker.replace(cx.waker().clone());
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn set_timeout() {
        let (monitor, notifier) = timeout_monitor();
        notifier.timeout_at(Instant::now() + Duration::from_millis(100));
        monitor.await;
    }

    #[tokio::test]
    async fn disable_timeout() {
        let (monitor, notifier) = timeout_monitor();
        notifier.timeout_at(Instant::now() + Duration::from_millis(1));
        notifier.disable();
        assert!(tokio::time::timeout(Duration::from_millis(10), monitor).await.is_err());
    }
}
