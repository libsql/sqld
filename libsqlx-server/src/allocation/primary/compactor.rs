use std::time::{Duration, Instant};

use libsqlx::libsql::{LogCompactor, LogFile};

pub struct Compactor {
    max_log_size: usize,
    last_compacted_at: Instant,
    compact_interval: Option<Duration>,
}

impl Compactor {
    pub fn new(max_log_size: usize, compact_interval: Option<Duration>) -> Self {
        Self {
            max_log_size,
            last_compacted_at: Instant::now(),
            compact_interval,
        }
    }
}

impl LogCompactor for Compactor {
    fn should_compact(&self, log: &LogFile) -> bool {
        let mut should_compact = false;
        if let Some(compact_interval) = self.compact_interval {
            should_compact |= self.last_compacted_at.elapsed() >= compact_interval
        }

        should_compact |= log.size() >= self.max_log_size;

        should_compact
    }

    fn compact(
        &mut self,
        _log: LogFile,
        _path: std::path::PathBuf,
        _size_after: u32,
    ) -> Result<(), Box<dyn std::error::Error + Sync + Send + 'static>> {
        self.last_compacted_at = Instant::now();
        todo!()
    }
}
