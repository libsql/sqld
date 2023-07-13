use std::sync::Arc;

use super::bus::Bus;
use super::Inbound;

#[async_trait::async_trait]
pub trait Handler: Sized + Send + Sync + 'static {
    /// Handle inbound message
    async fn handle(&self, bus: Arc<Bus<Self>>, msg: Inbound);
}
