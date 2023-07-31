use std::sync::Arc;

use super::bus::Dispatch;
use super::Inbound;

#[async_trait::async_trait]
pub trait Handler: Sized + Send + Sync + 'static {
    /// Handle inbound message
    async fn handle(&self, bus: Arc<dyn Dispatch>, msg: Inbound) -> crate::Result<()>;
}

#[cfg(test)]
#[async_trait::async_trait]
impl<F, Fut> Handler for F
where
    F: Fn(Arc<dyn Dispatch>, Inbound) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    async fn handle(&self, bus: Arc<dyn Dispatch>, msg: Inbound) -> crate::Result<()> {
        (self)(bus, msg).await;
        Ok(())
    }
}
