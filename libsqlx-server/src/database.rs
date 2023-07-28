use tokio::sync::{mpsc, oneshot};

use crate::allocation::AllocationMessage;
use crate::error::Error;
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
        if self
            .sender
            .send(AllocationMessage::HranaPipelineReq { req, ret: sender })
            .await
            .is_err()
        {
            return Err(Error::AllocationClosed);
        }

        ret.await.map_err(|_| {
            Error::Internal(String::from("response builder dropped by connection"))
        })
    }
}
