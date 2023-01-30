use std::collections::HashMap;
use std::fmt;

use futures::Sink;
use pgwire::api::auth::finish_authentication;
use pgwire::api::auth::save_startup_parameters_to_metadata;
use pgwire::api::auth::ServerParameterProvider;
use pgwire::api::auth::StartupHandler;
use pgwire::api::ClientInfo;
use pgwire::error::PgWireError;
use pgwire::error::PgWireResult;
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::PgWireFrontendMessage;
use pgwire::tokio::PgWireMessageServerCodec;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;

pub struct DefaultServerParameterProvider;

impl ServerParameterProvider for DefaultServerParameterProvider {
    fn server_parameters<C>(&self, _client: &C) -> Option<HashMap<String, String>>
    where
        C: ClientInfo,
    {
        let mut params = HashMap::with_capacity(4);
        params.insert("server_version".to_owned(), "0.1".to_owned());
        params.insert("server_encoding".to_owned(), "UTF8".to_owned());
        params.insert("client_encoding".to_owned(), "UTF8".to_owned());
        params.insert("DateStyle".to_owned(), "ISO YMD".to_owned());

        Some(params)
    }
}

pub struct NoopStartupHandler;

#[async_trait::async_trait]
impl StartupHandler for NoopStartupHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if let PgWireFrontendMessage::Startup(ref startup) = message {
            save_startup_parameters_to_metadata(client, startup);
            finish_authentication(client, &DefaultServerParameterProvider).await;
        }
        Ok(())
    }
}

pub struct PgAuthenticator;

impl PgAuthenticator {
    pub async fn authenticate<T>(
        &self,
        client: &mut Framed<T, PgWireMessageServerCodec>,
        msg: PgWireFrontendMessage,
    ) -> Result<(), PgWireError>
    where
        T: AsyncRead + AsyncWrite + Unpin + Send,
    {
        NoopStartupHandler.on_startup(client, msg).await?;

        Ok(())
    }
}
