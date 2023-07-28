#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Libsqlx(#[from] libsqlx::error::Error),
    #[error("replica injector loop exited")]
    InjectorExited,
    #[error("connection closed")]
    ConnectionClosed,
}
