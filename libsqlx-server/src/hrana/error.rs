use super::ProtocolError;
use super::stmt::StmtError;
use super::http::StreamError;
use super::http::request::StreamResponseError;

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
