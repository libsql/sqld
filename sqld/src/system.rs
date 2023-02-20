use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{ready, Context, Poll, Waker};

use futures::stream::FuturesUnordered;
use futures::task::{waker, ArcWake, AtomicWaker};
use futures::{FutureExt, Stream};
use tokio::task::JoinHandle;

struct SlotSubsystem {
    name: String,
    subsystem: Pin<Box<dyn Subsystem>>,
}

impl Future for SlotSubsystem {
    type Output = anyhow::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let ret = ready!(self.subsystem.as_mut().poll(cx));
        tracing::info!("task `{}` exited.", self.name);
        Poll::Ready(ret)
    }
}

pin_project_lite::pin_project! {
    pub struct System {
        #[pin]
        subsystems: FuturesUnordered<SlotSubsystem>,
        shutdown_waker: Arc<ShutdownWaker>,
        shutting_down: bool,
    }
}

struct ShutdownWaker {
    waker: AtomicWaker,
    shutdown: AtomicBool,
}

impl ShutdownWaker {
    fn register(&self, waker: &Waker) {
        self.waker.register(waker);
    }

    fn new() -> ShutdownWaker {
        Self {
            waker: AtomicWaker::new(),
            shutdown: AtomicBool::new(false),
        }
    }
}

impl ArcWake for ShutdownWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.shutdown.store(true, Ordering::Relaxed);
        arc_self.waker.wake();
    }
}

impl System {
    pub fn new() -> Self {
        Self {
            subsystems: FuturesUnordered::new(),
            shutdown_waker: Arc::new(ShutdownWaker::new()),
            shutting_down: false,
        }
    }
    pub fn register(&self, s: impl Subsystem + 'static, name: &str) {
        self.subsystems.push(SlotSubsystem {
            name: name.to_string(),
            subsystem: Box::pin(s),
        });
    }

    pub fn shutdown(&mut self) {
        self.shutting_down = true;
        self.subsystems.iter_mut().for_each(|s| s.shutdown());
    }

    fn poll_shutdown_request(&mut self, cx: &mut Context) -> bool {
        // as per https://docs.rs/futures/latest/futures/task/struct.AtomicWaker.html
        self.shutdown_waker.register(cx.waker());
        if self.shutdown_waker.shutdown.load(Ordering::Relaxed) {
            let waker = waker(self.shutdown_waker.clone());
            let mut context = Context::from_waker(&waker);
            for s in self.subsystems.iter_mut() {
                if s.subsystem.poll_should_shutdown(&mut context).is_ready() {
                    return true;
                }
            }

            // no task requested shutdown, we reset request flag
            self.shutdown_waker.shutdown.store(false, Ordering::Relaxed);

            // as per https://docs.rs/futures/latest/futures/task/struct.AtomicWaker.html
            self.shutdown_waker.clone().wake();
        }

        return false;
    }

    fn try_shutdown(&mut self) {
        if self
            .subsystems
            .iter_mut()
            .all(|s| s.subsystem.shutdown_ready())
        {
            self.subsystems
                .iter_mut()
                .for_each(|s| s.subsystem.shutdown());
            self.shutting_down = true;
        }
    }
}

impl Future for System {
    type Output = anyhow::Result<()>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // TODO: check only when woken for shutdown.
        if !self.shutting_down {
            let should_shutdown = self.poll_shutdown_request(cx);
            if should_shutdown {
                self.try_shutdown();
            }
        }

        let this = self.project();
        match ready!(this.subsystems.poll_next(cx)) {
            None => Poll::Ready(Ok(())),
            Some(ret) => Poll::Ready(ret),
        }
    }
}

struct JoinHandleSubsystem(JoinHandle<anyhow::Result<()>>);

impl<F> Subsystem for F
where
    F: Future<Output = anyhow::Result<()>>,
{
    fn shutdown(&mut self) {}
}

impl Future for JoinHandleSubsystem {
    type Output = anyhow::Result<()>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match ready!(self.0.poll_unpin(cx)) {
            Ok(Ok(())) => Poll::Ready(Ok(())),
            Ok(Err(e)) => Poll::Ready(Err(e)),
            Err(e) if e.is_cancelled() => Poll::Ready(Ok(())),
            Err(e) => Poll::Ready(Err(e.into())),
        }
    }
}

pub trait Subsystem: Future<Output = anyhow::Result<()>> {
    fn poll_should_shutdown(&mut self, _cx: &mut Context) -> Poll<()> {
        Poll::Pending
    }

    fn shutdown_ready(&self) -> bool {
        true
    }

    fn shutdown(&mut self);
}
