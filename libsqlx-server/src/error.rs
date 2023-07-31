use crate::meta::AllocationError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Libsqlx(#[from] libsqlx::error::Error),
    #[error("replica injector loop exited")]
    InjectorExited,
    #[error("connection closed")]
    ConnectionClosed,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("allocation closed")]
    AllocationClosed,
    #[error("internal error: {0}")]
    Internal(color_eyre::eyre::Error),
    #[error(transparent)]
    Heed(#[from] heed::Error),
    #[error(transparent)]
    Allocation(#[from] AllocationError),
}
