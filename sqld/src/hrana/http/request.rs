use anyhow::Result;

use crate::auth::Authenticated;
use super::{proto, stream};

pub async fn execute(
    stream_guard: &stream::Guard<'_>,
    _auth: Authenticated,
    request: proto::StreamRequest,
) -> Result<proto::StreamResult> {
    let _db = stream_guard.get_db()?;
    let error = proto::Error {
        message: format!("Request not implemented: {:?}", request),
        code: "UNKNOWN".into(),
    };
    Ok(proto::StreamResult::Error { error })
}
