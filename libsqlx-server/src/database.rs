use tokio::sync::{mpsc, oneshot};

use crate::allocation::{AllocationMessage, ConnectionHandle};

pub struct Database {
    pub sender: mpsc::Sender<AllocationMessage>,
}

impl Database {
    pub async fn connect(&self) -> crate::Result<ConnectionHandle> {
        let (ret, conn) = oneshot::channel();
        self.sender
            .send(AllocationMessage::Connect { ret })
            .await
            .map_err(|_| crate::error::Error::AllocationClosed)?;

        conn.await
            .map_err(|_| crate::error::Error::ConnectionClosed)?
    }
}
