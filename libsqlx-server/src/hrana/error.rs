use super::http::request::StreamResponseError;
use super::http::StreamError;
use super::stmt::StmtError;
use super::ProtocolError;

#[derive(Debug, thiserror::Error)]
pub enum HranaError {
    #[error(transparent)]
    Stmt(#[from] StmtError),
    #[error(transparent)]
    Proto(#[from] ProtocolError),
    #[error(transparent)]
    Stream(#[from] StreamError),
    #[error(transparent)]
    StreamResponse(#[from] StreamResponseError),
    #[error(transparent)]
    Libsqlx(crate::error::Error),
}

impl HranaError {
    pub fn code(&self) -> Option<&str>{
        match self {
            HranaError::Stmt(e) => Some(e.code()),
            HranaError::StreamResponse(e) => Some(e.code()),
            HranaError::Stream(_)
            | HranaError::Libsqlx(_)
            | HranaError::Proto(_) => None,
        }
    }
}
