use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{ready, Context, Poll, Waker};

use futures::stream::FuturesUnordered;
use futures::task::{waker, ArcWake, AtomicWaker};
use futures::Stream;

struct Slot {
    name: String,
    subsystem: Pin<Box<dyn Task>>,
    terminated: bool,
}

impl Future for Slot {
    type Output = anyhow::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let ret = ready!(self.subsystem.as_mut().poll_run(cx));
        tracing::info!("task `{}` exited.", self.name);

        Poll::Ready(ret)
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
            shutdown: AtomicBool::new(true),
        }
    }
}

impl ArcWake for ShutdownWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.shutdown.store(true, Ordering::Relaxed);
        arc_self.waker.wake();
    }
}

pin_project_lite::pin_project! {
    /// The `System` coordinates a collection of conccurent `Task`s to completion, and synchronizes
    /// their lifetimes.
    ///
    /// Under normal conditions, it simply "polls" the tasks to drive them.
    /// When the system starts polling it's tasks, it calls their `poll_notify_shutdown` method.
    /// This allows the task to asynchronously notify the system that the system should shutdown.
    /// Upon receiving a shutdown notification, the system will poll all the tasks for shutdown
    /// readiness. This allows other tasks to prevent shutdown, if some criterion is not met.
    ///
    /// Once all Tasks agree on shutdown, the `System` enters the shutdown phase, and the
    /// `poll_shutdown` method of all the tasks is called until they all exit.
    pub struct System {
        #[pin]
        subsystems: FuturesUnordered<Slot>,
        shutdown_waker: Arc<ShutdownWaker>,
        shutting_down: bool,
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

    /// Register a new task with the system.
    pub fn register(&self, s: impl Task, name: &str) {
        self.subsystems.push(Slot {
            terminated: false,
            name: name.to_string(),
            subsystem: Box::pin(s),
        });
    }

    /// Shut the system down.
    pub fn shutdown(&mut self) {
        self.shutting_down = true;
    }

    fn poll_shutdown_request(self: Pin<&mut Self>, cx: &mut Context) -> bool {
        let this = self.project();
        // as per https://docs.rs/futures/latest/futures/task/struct.AtomicWaker.html
        this.shutdown_waker.register(cx.waker());
        if this.shutdown_waker.shutdown.load(Ordering::Relaxed) {
            let waker = waker(this.shutdown_waker.clone());
            let mut context = Context::from_waker(&waker);
            let mut count_ready = 0;
            for mut s in this.subsystems.iter_pin_mut() {
                if s.subsystem
                    .as_mut()
                    .poll_notify_shutdown(&mut context)
                    .is_ready()
                {
                    count_ready += 1;
                }
            }

            // no task requested shutdown, we reset request flag
            this.shutdown_waker.shutdown.store(false, Ordering::Relaxed);
            return count_ready > 0;
        }

        false
    }

    fn try_shutdown(&mut self) {
        self.shutting_down = self.subsystems.iter().all(|s| s.subsystem.shutdown_ready());
    }
}

impl Future for System {
    type Output = anyhow::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // TODO: check only when woken for shutdown.
        if !self.shutting_down {
            let should_shutdown = self.as_mut().poll_shutdown_request(cx);
            if should_shutdown {
                self.try_shutdown();
            }
        }

        // shutdown phase, cooperatively ask all tasks to perform shutdown.
        // TODO: implement grace period
        if self.shutting_down {
            let mut count = 0;
            for s in self.subsystems.iter_mut().filter(|s| !s.terminated) {
                match s.subsystem.as_mut().poll_shutdown(cx) {
                    Poll::Ready(_) => {
                        s.terminated = true;
                    }
                    Poll::Pending => count += 1,
                }
            }

            if count == 0 {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        } else {
            let this = self.project();
            match ready!(this.subsystems.poll_next(cx)) {
                None => Poll::Ready(Ok(())),
                Some(ret) => Poll::Ready(ret),
            }
        }
    }
}

/// A `Task` is an abstract representation of some conccurent work performed on a `System`.
/// It encodes the differents phases in lifetime of the unit of work:
/// - running pas: normal operation mode
/// - shutdown phase: the Task is shutting down
///
/// In addition to that, it can asynchronously communicate with the `System` to inform it that:
/// - it's requesting a system shutdown (`poll_notify_shutdown`)
/// - it's ready to be shutdown (`shutdown_ready`)
pub trait Task: 'static {
    /// Poll the task to drive the task towards completion.
    /// This is usually equivalent to the `poll` method of a future.
    fn poll_run(self: Pin<&mut Self>, cx: &mut Context) -> Poll<anyhow::Result<()>>;
    /// resolve when this task should be shut down.
    /// This is a mecanism for the task to notify the driver that it should shutdown.
    fn poll_notify_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<()>;
    /// Whether this task is ready for shutdown.
    fn shutdown_ready(&self) -> bool;
    /// Poll for shutdown completion.
    /// This is only called it all the tasks previously returned `true` to a call to
    /// `shutdown_ready`
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<anyhow::Result<()>>;
}

/// Implementation of Task for a simple future.
/// A simple future will never ask for shutdown, is always ready to be shutdown, and shutdowns
/// immediately.
///
/// The future must be cancel-safe.
impl<F> Task for F
where
    F: Future<Output = anyhow::Result<()>> + 'static,
{
    fn poll_run(self: Pin<&mut Self>, cx: &mut Context) -> Poll<anyhow::Result<()>> {
        self.poll(cx)
    }

    fn poll_notify_shutdown(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<()> {
        Poll::Pending
    }

    fn shutdown_ready(&self) -> bool {
        true
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<anyhow::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
