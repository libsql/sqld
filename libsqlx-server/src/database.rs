use tokio::sync::{mpsc, oneshot};

use crate::allocation::{AllocationMessage, ConnectionHandle};

pub struct Database {
    pub sender: mpsc::Sender<AllocationMessage>,
}

impl Database {
    pub async fn connect(&self) -> crate::Result<ConnectionHandle> {
        let (ret, conn) = oneshot::channel();
        self.sender.send(AllocationMessage::Connect { ret }).await.unwrap();
        conn.await.unwrap()
    }
}
