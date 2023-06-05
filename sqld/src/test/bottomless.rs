use std::net::ToSocketAddrs;
use std::path::PathBuf;
use std::time::Duration;
use crate::{Config, run_server};
use libsql_client::{Connection, QueryResult, Value};
use reqwest::StatusCode;
use tokio::time::sleep;
use tracing_test::traced_test;

struct Cleaner(PathBuf);

impl Cleaner {
    fn new<P: Into<PathBuf>>(path: P) -> Self {
        let path = path.into();
        Self::cleanup(&path);
        Cleaner(path)
    }

    fn cleanup(path: &PathBuf) {
        let _ = std::fs::remove_dir_all(path);
    }

    fn path(&self) -> &PathBuf {
        &self.0
    }
}

impl Drop for Cleaner {
    fn drop(&mut self) {
        Self::cleanup(&self.0)
    }
}

#[tokio::test]
#[traced_test]
async fn backup_restore() {
    // we assume here that MinIO service is up and running
    assert_minio_ready().await;
    const PATH: &str = "backup_restore.sqld";
    const PORT: u16 = 15001;
    let listener_addr = format!("0.0.0.0:{}", PORT).to_socket_addrs().unwrap().next().unwrap();
    let connection_addr = url::Url::parse(&format!("http://127.0.0.1:{}", PORT)).unwrap();
    let db_config = Config {
        enable_bottomless_replication: true,
        db_path: PATH.into(),
        rpc_server_addr: Some(listener_addr.clone()),
        ..Config::default()
    };

    {
        // 1: create a local database, fill it with data, wait for WAL backup
        let cleaner = Cleaner::new(PATH);
        let db_job = tokio::spawn(run_server(db_config.clone()));
        db_job.await.unwrap().unwrap();

        sleep(Duration::from_secs(5)).await;

        let db = libsql_client::reqwest::Connection::connect_from_url(&connection_addr).unwrap();
        let result = db.batch([
            "CREATE TABLE t(id)",
            "INSERT INTO t VALUES (42)"
        ]).await.unwrap();

        sleep(Duration::from_millis(100)).await;

        //db_job.abort();
        drop(cleaner); // drop database files
    }
    // make sure that db file doesn't exist
    assert!(!std::path::Path::new(PATH).exists());

    {
        // 2: recreate the database, wait for restore from backup
        let cleaner = Cleaner::new(PATH);
        let db_job = tokio::spawn(run_server(db_config));

        sleep(Duration::from_secs(5)).await;

        let db = libsql_client::reqwest::Connection::connect_from_url(&connection_addr).unwrap();
        let result = db.execute("SELECT id FROM t;").await.unwrap();
        let rs = result.into_result_set().unwrap();
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(rs.rows[0].cells["id"], Value::Integer(42));

        db_job.abort();
    }
}

async fn assert_minio_ready() {
    const MINIO_URL: &str = "http://127.0.0.1:9090/minio/health/ready";
    let status = reqwest::get(MINIO_URL)
        .await.unwrap()
        .status();
    assert_eq!(status, StatusCode::OK);
}