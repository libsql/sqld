use std::net::ToSocketAddrs;
use std::path::PathBuf;
use std::time::Duration;
use crate::{Config, run_server};
use libsql_client::{Connection, QueryResult, Value};
use reqwest::StatusCode;
use tokio::time::sleep;
use tracing_test::traced_test;
use anyhow::Result;
use aws_config::SdkConfig;
use futures::executor::block_on;
use url::Url;

struct DbFileCleaner(PathBuf);

impl DbFileCleaner {
    fn new<P: Into<PathBuf>>(path: P) -> Self {
        let path = path.into();
        Self::cleanup(&path);
        DbFileCleaner(path)
    }

    fn cleanup(path: &PathBuf) {
        let _ = std::fs::remove_dir_all(path);
    }

    fn path(&self) -> &PathBuf {
        &self.0
    }
}

impl Drop for DbFileCleaner {
    fn drop(&mut self) {
        Self::cleanup(&self.0)
    }
}

struct S3BucketCleaner(&'static str);

impl S3BucketCleaner {
    async fn new(bucket: &'static str) -> Self {
        let _ = Self::cleanup(bucket).await; // ignore if doesn't exist
        S3BucketCleaner(bucket)
    }

    async fn cleanup(bucket: &str) -> Result<()> {
        use aws_sdk_s3::Client;

        let mut loader = aws_config::from_env().endpoint_url("http://127.0.0.1:9000/");
        let conf = aws_sdk_s3::config::Builder::from(&loader.load().await)
            .force_path_style(true)
            .build();
        let client = Client::from_conf(conf);
        client.delete_bucket().bucket(bucket).send().await?;
        Ok(())
    }
}

impl Drop for S3BucketCleaner {
    fn drop(&mut self) {
        let _ = block_on(Self::cleanup(self.0));
    }
}

#[tokio::test]
#[traced_test]
async fn backup_restore() {
    // we assume here that MinIO service is up and running
    assert_minio_ready().await;
    const BUCKET: &str = "test_backup_restore";
    const PATH: &str = "backup_restore.sqld";
    const PORT: u16 = 15001;
    //let s3_cleaner = S3BucketCleaner::new(BUCKET).await;
    let listener_addr = format!("0.0.0.0:{}", PORT).to_socket_addrs().unwrap().next().unwrap();
    let connection_addr = url::Url::parse(&format!("http://localhost:{}", PORT)).unwrap();
    let db_config = Config {
        enable_bottomless_replication: true,
        db_path: PATH.into(),
        http_addr: Some(listener_addr),
        ..Config::default()
    };

    {
        // 1: create a local database, fill it with data, wait for WAL backup
        let cleaner = DbFileCleaner::new(PATH);
        let db_job = tokio::spawn(run_server(db_config.clone()));

        sleep(Duration::from_secs(2)).await;

        let _ = sql(&connection_addr, [
            "CREATE TABLE t(id)",
            "INSERT INTO t VALUES (42)"
        ]).await.unwrap();

        sleep(Duration::from_millis(100)).await;

        db_job.abort();
        drop(cleaner); // drop database files
    }
    // make sure that db file doesn't exist
    assert!(!std::path::Path::new(PATH).exists());
    assert_bucket_non_empty(BUCKET).await;

    {
        // 2: recreate the database, wait for restore from backup
        let cleaner = DbFileCleaner::new(PATH);
        let db_job = tokio::spawn(run_server(db_config));

        sleep(Duration::from_secs(2)).await;

        let result = sql(&connection_addr, ["SELECT id FROM t"]).await.unwrap();
        let rs = result.into_iter().next().unwrap().into_result_set().unwrap();
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(rs.rows[0].cells["id"], Value::Integer(42));

        db_job.abort();
    }
}

async fn sql<I: IntoIterator<Item=&'static str>>(url: &Url, stmts: I) -> Result<Vec<QueryResult>> {
    let db = libsql_client::reqwest::Connection::connect_from_url(url)?;
    Ok(db.batch(stmts).await?)
}

const MINIO_URL: &str = "http://127.0.0.1:9090/";

async fn assert_minio_ready() {
    let status = reqwest::get(format!("{}/minio/health/ready", MINIO_URL))
        .await.expect(&format!("couldn't reach minio health check: '{}'", MINIO_URL))
        .status();
    assert_eq!(status, StatusCode::OK);
}

async fn assert_bucket_non_empty(bucket: &str) {
    use aws_sdk_s3::Client;

    let mut loader = aws_config::from_env().endpoint_url("http://127.0.0.1:9000/");
    let conf = aws_sdk_s3::config::Builder::from(&loader.load().await)
        .force_path_style(true)
        .build();
    let client = Client::from_conf(conf);
    let out = client.list_objects().bucket(bucket)
        .send().await.expect(&format!("bucket '{}': cannot list objects", bucket));
    let contents = out.contents().expect(&format!("bucket '{}' contents unavailable", bucket));
    assert!(!contents.is_empty());
}