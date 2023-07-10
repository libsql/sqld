use tokio::sync::{mpsc, oneshot};

use crate::hrana::http::proto::{PipelineRequestBody, PipelineResponseBody};
use crate::allocation::{AllocationMessage, ConnectionHandle};

pub struct Database {
    pub sender: mpsc::Sender<AllocationMessage>,
}

impl Database {
    pub async fn hrana_pipeline(&self, req: PipelineRequestBody) -> crate::Result<PipelineResponseBody> {
        dbg!();
        let (sender, ret) = oneshot::channel();
        dbg!();
        self.sender.send(AllocationMessage::HranaPipelineReq { req, ret: sender }).await.unwrap();
        dbg!();
        ret.await.unwrap()
    }
}
