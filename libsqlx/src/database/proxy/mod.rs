use std::sync::Arc;

use super::FrameNo;

mod connection;
mod database;

pub use database::WriteProxyDatabase;

// Waits until passed frameno has been replicated back to the database
type WaitFrameNoCb = Arc<dyn Fn(FrameNo) + Sync + Send + 'static>;
