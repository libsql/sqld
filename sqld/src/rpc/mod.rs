use anyhow::Context;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::watch;
use tonic::transport::Server;
use tower::util::option_layer;

use crate::database::factory::DbFactory;
use crate::replication::primary::logger::FileReplicationLogger;
use crate::replication::FrameNo;
use crate::rpc::proxy::rpc::proxy_server::ProxyServer;
use crate::rpc::proxy::ProxyService;
use crate::rpc::replication_log::rpc::replication_log_server::ReplicationLogServer;
use crate::rpc::replication_log::ReplicationLogService;
use crate::utils::services::idle_shutdown::IdleShutdownLayer;

pub mod proxy;
pub mod replication_log;

pub struct TlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub ca_cert_path: PathBuf,
}

pub struct ProxyServiceConfig {
    pub factory: Arc<dyn DbFactory>,
    pub frame_notifier: watch::Receiver<FrameNo>,
}

pub struct LoggerServiceConfig {
    pub logger: Arc<FileReplicationLogger>,
}

pub struct RpcServerBuilder {
    addr: SocketAddr,
    tls_config: Option<TlsConfig>,
    proxy_config: Option<ProxyServiceConfig>,
    logger_config: Option<LoggerServiceConfig>,
    idle_shutdown: Option<IdleShutdownLayer>,
}

impl RpcServerBuilder {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            tls_config: None,
            proxy_config: None,
            logger_config: None,
            idle_shutdown: None,
        }
    }

    pub fn with_tls(&mut self, tls_config: TlsConfig) -> &mut Self {
        self.tls_config.replace(tls_config);
        self
    }

    pub fn with_replication_logger_service(&mut self, config: LoggerServiceConfig) -> &mut Self {
        self.logger_config.replace(config);
        self
    }

    pub fn with_proxy_service(&mut self, config: ProxyServiceConfig) -> &mut Self {
        self.proxy_config.replace(config);
        self
    }

    pub fn with_idle_shutdown(&mut self, layer: IdleShutdownLayer) -> &mut Self {
        self.idle_shutdown.replace(layer);
        self
    }

    fn apply_tls(&self, mut builder: Server) -> anyhow::Result<Server> {
        if let Some(tls_config) = &self.tls_config {
            let cert_pem = std::fs::read_to_string(&tls_config.cert_path)?;
            let key_pem = std::fs::read_to_string(&tls_config.key_path)?;
            let identity = tonic::transport::Identity::from_pem(cert_pem, key_pem);

            let ca_cert_pem = std::fs::read_to_string(&tls_config.ca_cert_path)?;
            let ca_cert = tonic::transport::Certificate::from_pem(ca_cert_pem);

            let tls_config = tonic::transport::ServerTlsConfig::new()
                .identity(identity)
                .client_ca_root(ca_cert);
            builder = builder
                .tls_config(tls_config)
                .context("Failed to read the TSL config of RPC server")?;
        }

        Ok(builder)
    }

    pub async fn serve(self) -> anyhow::Result<()> {
        tracing::info!("serving write proxy server at {}", self.addr);
        let mut builder = tonic::transport::Server::builder();
        builder = self.apply_tls(builder)?;
        let proxy_service = self
            .proxy_config
            .map(|c| ProxyServer::new(ProxyService::new(c.factory, c.frame_notifier)));
        let logger_service = self
            .logger_config
            .map(|c| ReplicationLogServer::new(ReplicationLogService::new(c.logger)));
        builder
            .layer(&option_layer(self.idle_shutdown))
            .add_optional_service(proxy_service)
            .add_optional_service(logger_service)
            .serve(self.addr)
            .await?;

        Ok(())
    }
}
