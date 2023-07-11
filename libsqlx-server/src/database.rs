use tokio::sync::{mpsc, oneshot};

use crate::allocation::AllocationMessage;
use crate::hrana::http::proto::{PipelineRequestBody, PipelineResponseBody};

pub struct Database {
    pub sender: mpsc::Sender<AllocationMessage>,
}

impl Database {
    pub async fn hrana_pipeline(
        &self,
        req: PipelineRequestBody,
    ) -> crate::Result<PipelineResponseBody> {
        let (sender, ret) = oneshot::channel();
        self.sender
            .send(AllocationMessage::HranaPipelineReq { req, ret: sender })
            .await
            .unwrap();
        ret.await.unwrap()
    }
}
