use anyhow::Result;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use aws_sdk_s3::Client;
use bottomless::s3::S3Client;
use chrono::{DateTime, SecondsFormat, Utc};
use futures_core::Future;
use itertools::Itertools;
use libsql_client::{Connection, QueryResult, Statement, Value};
use serde_json::json;
use std::collections::BTreeSet;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use tokio::time::sleep;
use tokio::time::Duration;
use url::Url;
use uuid::Uuid;

use crate::config::{AdminApiConfig, DbConfig, UserApiConfig};
use crate::net::AddrIncoming;
use crate::Server;

const S3_URL: &str = "http://localhost:9000/";

/// returns a future that once polled will shutdown the server and wait for cleanup
fn start_db(step: u32, server: Server) -> impl Future<Output = ()> {
    let notify = server.shutdown.clone();
    let handle = tokio::spawn(async move {
        if let Err(e) = server.start().await {
            panic!("Failed step {}: {}", step, e);
        }
    });

    async move {
        notify.notify_waiters();
        handle.await.unwrap();
    }
}

async fn configure_server(
    options: &bottomless::replicator::Options,
    addr: SocketAddr,
    admin_addr: Option<SocketAddr>,
    path: impl Into<PathBuf>,
    checkpoint_interval: Option<Duration>,
) -> Server {
    let http_acceptor = AddrIncoming::new(tokio::net::TcpListener::bind(addr).await.unwrap());
    Server {
        db_config: DbConfig {
            extensions_path: None,
            bottomless_replication: Some(options.clone()),
            max_log_size: 200 * 4046,
            max_log_duration: None,
            soft_heap_limit_mb: None,
            hard_heap_limit_mb: None,
            max_response_size: 10000000 * 4096,
            max_total_response_size: 10000000 * 4096,
            snapshot_exec: None,
            checkpoint_interval,
            auto_checkpoint: 1000,
        },
        admin_api_config: if let Some(addr) = admin_addr {
            Some(AdminApiConfig {
                acceptor: AddrIncoming::new(tokio::net::TcpListener::bind(addr).await.unwrap()),
            })
        } else {
            None
        },
        disable_namespaces: true,
        user_api_config: UserApiConfig {
            hrana_ws_acceptor: None,
            http_acceptor: Some(http_acceptor),
            enable_http_console: false,
            self_url: None,
            http_auth: None,
            auth_jwt_key: None,
        },
        path: path.into().into(),
        disable_default_namespace: false,
        heartbeat_config: None,
        idle_shutdown_timeout: None,
        initial_idle_shutdown_timeout: None,
        rpc_server_config: None,
        rpc_client_config: None,
        shutdown: Default::default(),
    }
}

#[tokio::test]
async fn backup_restore() {
    let _ = env_logger::builder().is_test(true).try_init();
    const DB_ID: &str = "testbackuprestore";
    const BUCKET: &str = "testbackuprestore";
    const PATH: &str = "backup_restore.sqld";
    const PORT: u16 = 15001;
    const ADMIN_PORT: u16 = 15701;
    const OPS: usize = 2000;
    const ROWS: usize = 10;

    let _ = S3BucketCleaner::new(BUCKET).await;
    assert_bucket_occupancy(BUCKET, true).await;

    let options = bottomless::replicator::Options {
        db_id: Some(DB_ID.to_string()),
        create_bucket_if_not_exists: true,
        verify_crc: true,
        use_compression: bottomless::replicator::CompressionKind::Gzip,
        bucket_name: BUCKET.to_string(),
        max_batch_interval: Duration::from_millis(250),
        restore_transaction_page_swap_after: 1, // in this test swap should happen at least once
        snapshot_interval: Some(Duration::from_millis(500)),
        ..bottomless::replicator::Options::from_env().unwrap()
    };
    let connection_addr = Url::parse(&format!("http://localhost:{}", PORT)).unwrap();
    let listener_addr = format!("0.0.0.0:{}", PORT)
        .to_socket_addrs()
        .unwrap()
        .next()
        .unwrap();
    let admin_addr = format!("0.0.0.0:{}", ADMIN_PORT)
        .to_socket_addrs()
        .unwrap()
        .next()
        .unwrap();

    let make_server =
        || async { configure_server(&options, listener_addr, Some(admin_addr), PATH, None).await };

    let recover_time = {
        tracing::info!(
            "---STEP 1: create a local database, fill it with data, wait for WAL backup---"
        );
        let cleaner = DbFileCleaner::new(PATH);
        let db_job = start_db(1, make_server().await);

        sleep(Duration::from_secs(2)).await;

        let _ = sql(
            &connection_addr,
            ["CREATE TABLE IF NOT EXISTS t(id INT PRIMARY KEY, name TEXT);"],
        )
        .await
        .unwrap();

        perform_updates(&connection_addr, ROWS, OPS, "A").await;

        sleep(Duration::from_secs(2)).await;

        db_job.await;
        drop(cleaner);
        let time = Utc::now(); // remember this timestamp for point-in-time recovery
        tracing::info!("restoration point: {}", time.to_rfc3339());
        time
    };

    // make sure that db file doesn't exist, and that the bucket contains backup
    assert!(!std::path::Path::new(PATH).exists());
    assert_bucket_occupancy(BUCKET, false).await;

    {
        tracing::info!(
            "---STEP 2: recreate the database from WAL - create a snapshot at the end---"
        );
        let snapshots = snapshot_count(BUCKET, DB_ID, "default").await;
        assert_eq!(snapshots, 0, "no generation should have snapshot yet");
        let cleaner = DbFileCleaner::new(PATH);
        let db_job = start_db(2, make_server().await);

        sleep(Duration::from_secs(2)).await;

        assert_updates(&connection_addr, ROWS, OPS, "A").await;

        db_job.await;
        drop(cleaner);
    }

    assert!(!std::path::Path::new(PATH).exists());

    {
        tracing::info!("---STEP 3: recreate database from snapshot alone---");
        let snapshots = snapshot_count(BUCKET, DB_ID, "default").await;
        assert!(
            snapshots > 0,
            "at least one generation should have snapshot"
        );
        let cleaner = DbFileCleaner::new(PATH);
        let db_job = start_db(3, make_server().await);

        sleep(Duration::from_secs(2)).await;

        // override existing entries, this will generate WAL
        perform_updates(&connection_addr, ROWS, OPS, "B").await;

        // wait for WAL to backup
        sleep(Duration::from_secs(2)).await;
        db_job.await;
        drop(cleaner);
    }

    assert!(!std::path::Path::new(PATH).exists());

    {
        tracing::info!("---STEP 4: recreate the database from snapshot + WAL---");
        let snapshots = snapshot_count(BUCKET, DB_ID, "default").await;
        assert!(
            snapshots > 0,
            "at least one generation should have snapshot"
        );
        let cleaner = DbFileCleaner::new(PATH);
        let db_job = start_db(4, make_server().await);

        sleep(Duration::from_secs(2)).await;

        assert_updates(&connection_addr, ROWS, OPS, "B").await;

        db_job.await;
        drop(cleaner);
    }

    {
        // make sure that we can follow back until the generation from which snapshot could be possible
        tracing::info!("---STEP 5: recreate database from generation missing snapshot ---");

        // manually remove snapshots from all generations, this will force restore across generations
        // from the very beginning
        remove_snapshots(BUCKET).await;
        let snapshots = snapshot_count(BUCKET, DB_ID, "default").await;
        assert_eq!(snapshots, 0, "all snapshots should be removed");

        let cleaner = DbFileCleaner::new(PATH);
        let db_job = start_db(4, make_server().await);

        sleep(Duration::from_secs(3)).await;

        assert_updates(&connection_addr, ROWS, OPS, "B").await;

        db_job.await;
        drop(cleaner);
    }

    {
        tracing::info!("---STEP 6: point in time recovery ({}) ---", recover_time);
        let cleaner = DbFileCleaner::new(PATH);
        let db_job = start_db(4, make_server().await);

        sleep(Duration::from_secs(2)).await;

        let admin_api = Url::parse(&format!("http://localhost:{}", ADMIN_PORT)).unwrap();
        request_restore(&admin_api, recover_time).await.unwrap();

        sleep(Duration::from_secs(2)).await;

        assert_updates(&connection_addr, ROWS, OPS, "A").await;

        let snapshots = snapshot_count(BUCKET, DB_ID, "default").await;
        assert_eq!(snapshots, 1, "new fork should have a snapshot");

        db_job.await;
        drop(cleaner);
    }
}

#[tokio::test]
async fn rollback_restore() {
    let _ = env_logger::builder().is_test(true).try_init();
    const DB_ID: &str = "testrollbackrestore";
    const BUCKET: &str = "testrollbackrestore";
    const PATH: &str = "rollback_restore.sqld";
    const PORT: u16 = 15002;

    async fn get_data(conn: &Url) -> Result<Vec<(Value, Value)>> {
        let result = sql(conn, ["SELECT * FROM t"]).await?;
        let rows = result
            .into_iter()
            .next()
            .unwrap()
            .into_result_set()?
            .rows
            .into_iter()
            .map(|row| (row.cells["id"].clone(), row.cells["name"].clone()))
            .collect();
        Ok(rows)
    }

    let _ = S3BucketCleaner::new(BUCKET).await;
    assert_bucket_occupancy(BUCKET, true).await;

    let listener_addr = format!("0.0.0.0:{}", PORT)
        .to_socket_addrs()
        .unwrap()
        .next()
        .unwrap();
    let conn = Url::parse(&format!("http://localhost:{}", PORT)).unwrap();
    let options = bottomless::replicator::Options {
        db_id: Some(DB_ID.to_string()),
        create_bucket_if_not_exists: true,
        verify_crc: true,
        use_compression: bottomless::replicator::CompressionKind::Gzip,
        bucket_name: BUCKET.to_string(),
        max_batch_interval: Duration::from_millis(250),
        restore_transaction_page_swap_after: 1, // in this test swap should happen at least once
        ..bottomless::replicator::Options::from_env().unwrap()
    };
    let make_server = || async {
        configure_server(
            &options,
            listener_addr,
            None,
            PATH,
            Some(Duration::from_secs(1)),
        )
        .await
    };

    {
        tracing::info!("---STEP 1: create db, write row, rollback---");
        let cleaner = DbFileCleaner::new(PATH);
        let db_job = start_db(1, make_server().await);

        sleep(Duration::from_secs(2)).await;

        let _ = sql(
            &conn,
            [
                "CREATE TABLE IF NOT EXISTS t(id INT PRIMARY KEY, name TEXT);",
                "INSERT INTO t(id, name) VALUES(1, 'A')",
            ],
        )
        .await
        .unwrap();

        let _ = sql(
            &conn,
            [
                "BEGIN",
                "UPDATE t SET name = 'B' WHERE id = 1",
                "ROLLBACK",
                "INSERT INTO t(id, name) VALUES(2, 'B')",
            ],
        )
        .await
        .unwrap();

        // wait for backup
        sleep(Duration::from_secs(2)).await;
        assert_bucket_occupancy(BUCKET, false).await;

        let rs = get_data(&conn).await.unwrap();
        assert_eq!(
            rs,
            vec![
                (Value::Integer(1), Value::Text("A".into())),
                (Value::Integer(2), Value::Text("B".into()))
            ],
            "rollback value should not be updated"
        );

        db_job.await;
        drop(cleaner);
    }

    {
        tracing::info!("---STEP 2: recreate database, read modify, read again ---");
        let cleaner = DbFileCleaner::new(PATH);
        let db_job = start_db(2, make_server().await);
        sleep(Duration::from_secs(2)).await;

        let rs = get_data(&conn).await.unwrap();
        assert_eq!(
            rs,
            vec![
                (Value::Integer(1), Value::Text("A".into())),
                (Value::Integer(2), Value::Text("B".into()))
            ],
            "restored value should not contain rollbacked update"
        );
        let _ = sql(&conn, ["UPDATE t SET name = 'C'"]).await.unwrap();
        let rs = get_data(&conn).await.unwrap();
        assert_eq!(
            rs,
            vec![
                (Value::Integer(1), Value::Text("C".into())),
                (Value::Integer(2), Value::Text("C".into()))
            ]
        );

        db_job.await;
        drop(cleaner);
    }
}

#[tokio::test]
async fn periodic_snapshot() {
    let _ = env_logger::builder().is_test(true).try_init();
    const DB_ID: &str = "periodicsnapshot";
    const BUCKET: &str = "periodicsnapshot";
    const PATH: &str = "periodic_snapshot.sqld";
    const PORT: u16 = 15003;

    let _ = S3BucketCleaner::new(BUCKET).await;
    assert_bucket_occupancy(BUCKET, true).await;

    let snapshot_interval = Duration::from_secs(3);
    let options = bottomless::replicator::Options {
        db_id: Some(DB_ID.to_string()),
        create_bucket_if_not_exists: true,
        verify_crc: true,
        use_compression: bottomless::replicator::CompressionKind::Gzip,
        bucket_name: BUCKET.to_string(),
        max_batch_interval: Duration::from_millis(250),
        restore_transaction_page_swap_after: 100,
        max_frames_per_batch: 100,
        snapshot_interval: Some(snapshot_interval),
        ..bottomless::replicator::Options::from_env().unwrap()
    };
    let connection_addr = Url::parse(&format!("http://localhost:{}", PORT)).unwrap();
    let listener_addr = format!("0.0.0.0:{}", PORT)
        .to_socket_addrs()
        .unwrap()
        .next()
        .unwrap();

    let make_server = || async {
        configure_server(
            &options,
            listener_addr,
            None,
            PATH,
            Some(Duration::from_secs(1)),
        )
        .await
    };
    {
        let cleaner = DbFileCleaner::new(PATH);
        let db_job = start_db(1, make_server().await);

        sleep(Duration::from_secs(2)).await;

        let _ = sql(
            &connection_addr,
            ["CREATE TABLE IF NOT EXISTS t(id INT PRIMARY KEY, name TEXT);"],
        )
        .await
        .unwrap();

        // at first there's no snapshot cache file info, so we need to wait for first snasphot to occur
        perform_updates(&connection_addr, 10, 100, "A").await;
        sleep(snapshot_interval + Duration::from_secs(1)).await;

        remove_snapshots(BUCKET).await;

        // generate enough updates for 3 generations to occur
        // wait long enough to make sure that updates got to the bottomless but not long enough
        // for a snapshot interval to trigger
        perform_updates(&connection_addr, 10, 100, "B").await;
        sleep(Duration::from_secs(1)).await;

        let generations = list_generations(BUCKET, DB_ID, "default").await.unwrap();
        assert_ne!(generations.len(), 0, "some generations should be created");
        for gen in generations {
            let snapshotted = has_snapshot(BUCKET, DB_ID, &gen, "default").await.unwrap();
            assert!(!snapshotted, "no generation should be snapshotted yet");
        }

        // make sure that snapshot interval passed
        sleep(snapshot_interval + Duration::from_secs(1)).await;

        let snapshots = snapshot_count(BUCKET, DB_ID, "default").await;
        assert_eq!(snapshots, 1, "snapshot should be created after interval");

        db_job.await;
        drop(cleaner);
        let time = Utc::now(); // remember this timestamp for point-in-time recovery
        tracing::info!("restoration point: {}", time.to_rfc3339());
        time
    };
}

async fn perform_updates(connection_addr: &Url, row_count: usize, ops_count: usize, update: &str) {
    let stmts: Vec<_> = (0..ops_count)
        .map(|i| {
            format!(
                "INSERT INTO t(id, name) VALUES({}, '{}-{}') ON CONFLICT (id) DO UPDATE SET name = '{}-{}';",
                i % row_count,
                i,
                update,
                i,
                update
            )
        })
        .collect();
    let _ = sql(connection_addr, stmts).await.unwrap();
}

async fn assert_updates(connection_addr: &Url, row_count: usize, ops_count: usize, update: &str) {
    let result = sql(connection_addr, ["SELECT id, name FROM t ORDER BY id;"])
        .await
        .unwrap();
    let rs = result
        .into_iter()
        .next()
        .unwrap()
        .into_result_set()
        .unwrap();
    assert_eq!(rs.rows.len(), row_count, "unexpected number of rows");
    let base = if ops_count < 10 { 0 } else { ops_count - 10 } as i64;
    for (i, row) in rs.rows.iter().enumerate() {
        let i = i as i64;
        let id = row.cells["id"].clone();
        let name = row.cells["name"].clone();
        assert_eq!(
            (&id, &name),
            (
                &Value::Integer(i),
                &Value::Text(format!("{}-{}", base + i, update))
            ),
            "unexpected values for row {}: ({})",
            i,
            name
        );
    }
}

async fn request_restore(url: &Url, timestamp: DateTime<Utc>) -> Result<()> {
    let mut timestamp: String = timestamp.to_rfc3339_opts(SecondsFormat::Secs, true);
    timestamp.pop();
    // NaiveDateTime accepted format is `2018-01-26T18:30:09` (without Z)
    let json = json!({"timestamp": timestamp});
    let endpoint = url.join("/v1/namespaces/default/fork/temp")?;
    tracing::info!("calling {} - {}", endpoint, json);
    let client = reqwest::Client::new();
    let resp = client.post(endpoint.clone()).json(&json).send().await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    tracing::info!("forked `default` to `temp`");
    assert_eq!(resp.status().as_u16(), 200);
    // replace default with temp
    let resp = client
        .delete(url.join("/v1/namespaces/default")?)
        .send()
        .await?;
    assert_eq!(resp.status().as_u16(), 200);
    tracing::info!("deleted `default`");
    tokio::time::sleep(Duration::from_secs(1)).await;
    let resp = client
        .post(url.join("/v1/namespaces/temp/fork/default")?)
        .send()
        .await?;
    assert_eq!(resp.status().as_u16(), 200);
    tracing::info!("forked `temp` to `default`");
    Ok(())
}

async fn sql<I, S>(url: &Url, stmts: I) -> Result<Vec<QueryResult>>
where
    I: IntoIterator<Item = S>,
    S: Into<Statement>,
{
    let db = libsql_client::reqwest::Connection::connect_from_url(url)?;
    db.batch(stmts).await
}

async fn s3_config() -> aws_sdk_s3::config::Config {
    let loader = aws_config::from_env().endpoint_url(S3_URL);
    aws_sdk_s3::config::Builder::from(&loader.load().await)
        .force_path_style(true)
        .region(Region::new(
            std::env::var("LIBSQL_BOTTOMLESS_AWS_DEFAULT_REGION").unwrap(),
        ))
        .credentials_provider(Credentials::new(
            std::env::var("LIBSQL_BOTTOMLESS_AWS_ACCESS_KEY_ID").unwrap(),
            std::env::var("LIBSQL_BOTTOMLESS_AWS_SECRET_ACCESS_KEY").unwrap(),
            None,
            None,
            "Static",
        ))
        .build()
}

async fn s3_client(bucket: &str, db_name: &str) -> S3Client {
    let conf = s3_config().await;
    let client = Client::from_conf(conf);
    S3Client::new(client, bucket.into(), db_name.into())
}

async fn list_generations(
    bucket: &str,
    db_id: &str,
    namespace: &str,
) -> Result<BTreeSet<uuid::Uuid>> {
    let client = s3_client(bucket, &db_name(db_id, namespace)).await;
    let mut set = BTreeSet::new();
    let mut generations = client.list_generations();
    while let Some(gen) = generations.next().await? {
        set.insert(gen);
    }
    Ok(set)
}

fn db_name(db_id: &str, namespace: &str) -> String {
    format!("ns-{db_id}:{namespace}")
}

async fn has_snapshot(
    bucket: &str,
    db_id: &str,
    generation: &Uuid,
    namespace: &str,
) -> Result<bool> {
    let client = s3_client(bucket, &db_name(db_id, namespace)).await;
    let res = client.has_snapshot(generation).await?;
    tracing::info!("Generation '{}' has snapshot: {}", generation, res);
    Ok(res)
}

async fn snapshot_count(bucket: &str, db_id: &str, namespace: &str) -> usize {
    let mut snapshots = 0;
    for gen in list_generations(bucket, db_id, namespace).await.unwrap() {
        if has_snapshot(bucket, db_id, &gen, namespace).await.unwrap() {
            snapshots += 1;
        }
    }
    snapshots
}

/// Remove a snapshot objects from all generation. This may trigger bottomless to do rollup restore
/// across all generations.
async fn remove_snapshots(bucket: &str) {
    tracing::info!("removing all snapshots");
    let client = Client::from_conf(s3_config().await);
    if let Ok(out) = client.list_objects().bucket(bucket).send().await {
        let keys = out
            .contents()
            .unwrap()
            .iter()
            .map(|o| {
                let key = o.key().unwrap();
                let prefix = key.split('/').next().unwrap();
                format!("{}/db.gz", prefix)
            })
            .unique()
            .map(|key| ObjectIdentifier::builder().key(key).build())
            .collect();

        client
            .delete_objects()
            .bucket(bucket)
            .delete(
                Delete::builder()
                    .set_objects(Some(keys))
                    .quiet(true)
                    .build(),
            )
            .send()
            .await
            .unwrap();
    }
}

/// Checks if the corresponding bucket is empty (has any elements) or not.
/// If bucket was not found, it's equivalent of an empty one.
async fn assert_bucket_occupancy(bucket: &str, expect_empty: bool) {
    let client = Client::from_conf(s3_config().await);
    if let Ok(out) = client.list_objects().bucket(bucket).send().await {
        let contents = out.contents().unwrap_or_default();
        if expect_empty {
            assert!(
                contents.is_empty(),
                "expected S3 bucket to be empty but {} were found",
                contents.len()
            );
        } else {
            assert!(
                !contents.is_empty(),
                "expected S3 bucket to be filled with backup data but it was empty"
            );
        }
    } else if !expect_empty {
        panic!("bucket '{}' doesn't exist", bucket);
    }
}

/// Guardian struct used for cleaning up the test data from
/// database file dir at the beginning and end of a test.
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
}

impl Drop for DbFileCleaner {
    fn drop(&mut self) {
        Self::cleanup(&self.0)
    }
}

/// Guardian struct used for cleaning up the test data from
/// S3 bucket dir at the beginning and end of a test.
struct S3BucketCleaner(&'static str);

impl S3BucketCleaner {
    async fn new(bucket: &'static str) -> Self {
        let _ = Self::cleanup(bucket).await; // cleanup the bucket before test
        S3BucketCleaner(bucket)
    }

    /// Delete all objects from S3 bucket with provided name (doesn't delete bucket itself).
    async fn cleanup(bucket: &str) -> Result<()> {
        let client = Client::from_conf(s3_config().await);
        let objects = client.list_objects().bucket(bucket).send().await?;
        let mut delete_keys = Vec::new();
        for o in objects.contents().unwrap_or_default() {
            let id = ObjectIdentifier::builder()
                .set_key(o.key().map(String::from))
                .build();
            delete_keys.push(id);
        }

        let _ = client
            .delete_objects()
            .bucket(bucket)
            .delete(Delete::builder().set_objects(Some(delete_keys)).build())
            .send()
            .await?;

        Ok(())
    }
}

impl Drop for S3BucketCleaner {
    fn drop(&mut self) {
        //FIXME: running line below on tokio::test runtime will hang.
        //let _ = block_on(Self::cleanup(self.0));
    }
}
