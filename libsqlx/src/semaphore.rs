use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use parking_lot::Condvar;
use parking_lot::Mutex;

struct SemaphoreInner {
    max_permits: usize,
    permits: Mutex<usize>,
    condvar: Condvar,
}

#[derive(Clone)]
pub struct Semaphore {
    inner: Arc<SemaphoreInner>,
}

pub struct Permit(Semaphore);

impl Drop for Permit {
    fn drop(&mut self) {
        *self.0.inner.permits.lock() -= 1;
        self.0.inner.condvar.notify_one();
    }
}

impl Semaphore {
    pub fn new(max_permits: usize) -> Self {
        Self {
            inner: Arc::new(SemaphoreInner {
                max_permits,
                permits: Mutex::new(0),
                condvar: Condvar::new(),
            }),
        }
    }

    pub fn acquire(&self) -> Permit {
        let mut permits = self.inner.permits.lock();
        self.inner
            .condvar
            .wait_while(&mut permits, |permits| *permits >= self.inner.max_permits);
        *permits += 1;
        assert!(*permits <= self.inner.max_permits);
        Permit(self.clone())
    }

    pub fn acquire_timeout(&self, timeout: Duration) -> Option<Permit> {
        let deadline = Instant::now() + timeout;
        let mut permits = self.inner.permits.lock();
        if self
            .inner
            .condvar
            .wait_while_until(
                &mut permits,
                |permits| *permits >= self.inner.max_permits,
                deadline,
            )
            .timed_out()
        {
            return None;
        }

        *permits += 1;
        assert!(*permits <= self.inner.max_permits);
        Some(Permit(self.clone()))
    }

    #[cfg(test)]
    fn try_acquire(&self) -> Option<Permit> {
        let mut permits = self.inner.permits.lock();
        if *permits >= self.inner.max_permits {
            None
        } else {
            *permits += 1;
            Some(Permit(self.clone()))
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn semaphore() {
        let sem = Semaphore::new(2);
        let permit1 = sem.acquire();
        let _permit2 = sem.acquire();

        assert!(sem.try_acquire().is_none());
        drop(permit1);
        let perm = sem.try_acquire();
        assert!(perm.is_some());
        assert!(sem.acquire_timeout(Duration::from_millis(100)).is_none());
    }
}
