use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use libsqlx::libsql::{LogCompactor, LogFile};
use uuid::Uuid;

use crate::{
    compactor::{CompactionJob, CompactionQueue},
    meta::DatabaseId,
};

pub struct Compactor {
    max_log_size: usize,
    last_compacted_at: Instant,
    compact_interval: Option<Duration>,
    queue: Arc<CompactionQueue>,
    database_id: DatabaseId,
}

impl Compactor {
    pub fn new(
        max_log_size: usize,
        compact_interval: Option<Duration>,
        queue: Arc<CompactionQueue>,
        database_id: DatabaseId,
    ) -> Self {
        Self {
            max_log_size,
            last_compacted_at: Instant::now(),
            compact_interval,
            queue,
            database_id,
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
        log_id: Uuid,
    ) -> Result<(), Box<dyn std::error::Error + Sync + Send + 'static>> {
        self.last_compacted_at = Instant::now();
        self.queue.push(&CompactionJob {
            database_id: self.database_id,
            log_id,
        });

        Ok(())
    }

    fn snapshot_dir(&self) -> PathBuf {
        self.queue.snapshot_queue_dir()
    }
}
