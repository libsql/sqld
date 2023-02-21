use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use quanta::Instant;
use tower::{Layer, Service};

use crate::system::Task;

#[derive(Clone)]
pub struct IdleShutdownLayer {
    last_req: Arc<AtomicInstant>,
}

pub struct IdleShutdownFut {
    timeout: Duration,
    last_req_ts: Arc<AtomicInstant>,
    timeout_fut: tokio::time::Interval,
}

impl IdleShutdownFut {
    fn new(timeout: Duration, last_req_ts: Arc<AtomicInstant>) -> Self {
        Self {
            timeout,
            last_req_ts,
            timeout_fut: tokio::time::interval_at(tokio::time::Instant::now() + timeout, timeout),
        }
    }

    fn timed_out(&self) -> bool {
        Instant::now() - self.timeout > self.last_req_ts.last_req()
    }
}

impl Task for IdleShutdownFut {
    fn poll_run(self: Pin<&mut Self>, _cx: &mut std::task::Context) -> Poll<anyhow::Result<()>> {
        Poll::Pending
    }

    fn poll_notify_shutdown(mut self: Pin<&mut Self>, cx: &mut std::task::Context) -> Poll<()> {
        if self.timed_out() {
            tracing::info!("Http idle timeout: requesting shutdown.");
            return Poll::Ready(());
        } else {
            let elapsed_since_last_req =
                Instant::now().saturating_duration_since(self.last_req_ts.last_req());
            let timeout = tokio::time::interval_at(
                tokio::time::Instant::now() + self.timeout.saturating_sub(elapsed_since_last_req),
                self.timeout,
            );
            self.timeout_fut = timeout;
        }
        // register context
        let _ = self.timeout_fut.poll_tick(cx);
        Poll::Pending
    }

    fn shutdown_ready(&self) -> bool {
        self.timed_out()
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context,
    ) -> Poll<anyhow::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

struct AtomicInstant {
    base: Instant,
    /// offset in secs since last updated
    last: AtomicU64,
}

impl AtomicInstant {
    fn now() -> Self {
        let base = Instant::recent();
        Self {
            base,
            last: AtomicU64::new(0),
        }
    }

    fn update(&self) {
        let elapsed = Instant::recent().saturating_duration_since(self.base);
        self.last.store(elapsed.as_secs(), Ordering::Relaxed);
    }

    fn last_req(&self) -> Instant {
        self.base + Duration::from_secs(self.last.load(Ordering::Relaxed))
    }
}

impl IdleShutdownLayer {
    pub fn create_pair(idle_timeout: Duration) -> (IdleShutdownLayer, IdleShutdownFut) {
        let instant = Arc::new(AtomicInstant::now());
        let layer = IdleShutdownLayer {
            last_req: instant.clone(),
        };
        let subsystem = IdleShutdownFut::new(idle_timeout, instant);

        (layer, subsystem)
    }
}

impl<S> Layer<S> for IdleShutdownLayer {
    type Service = IdleShutdownService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        IdleShutdownService {
            inner,
            last_req: self.last_req.clone(),
        }
    }
}

#[derive(Clone)]
pub struct IdleShutdownService<S> {
    inner: S,
    last_req: Arc<AtomicInstant>,
}

impl<Req, S> Service<Req> for IdleShutdownService<S>
where
    S: Service<Req>,
{
    type Response = S::Response;

    type Error = S::Error;

    type Future = S::Future;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        self.last_req.update();
        self.inner.call(req)
    }
}
