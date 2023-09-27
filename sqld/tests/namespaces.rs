mod common;

#[cfg(feature = "sim-tests")]
mod test {
    use crate::common::net::{init_tracing, TurmoilConnector};

    use super::common;

    use std::{convert::Infallible, path::PathBuf};

    use common::{net::{TestServer, TurmoilAcceptor}, http::Client};
    use hyper::{service::make_service_fn, Response, Body, StatusCode};
    use libsql::{Database, Value};
    use serde_json::json;
    use sqld::config::{AdminApiConfig, RpcServerConfig, UserApiConfig};
    use tempfile::tempdir;
    use tower::service_fn;
    use turmoil::{Builder, Sim};

    fn make_primary(sim: &mut Sim, path: PathBuf) {
        init_tracing();
        sim.host("primary", move || {
            let path = path.clone();
            async move {
                let server = TestServer {
                    path: path.into(),
                    user_api_config: UserApiConfig {
                        http_acceptor: Some(TurmoilAcceptor::bind(([0, 0, 0, 0], 8080)).await?),
                        ..Default::default()
                    },
                    admin_api_config: Some(AdminApiConfig {
                        acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 9090)).await?,
                        connector: TurmoilConnector,
                    }),
                    rpc_server_config: Some(RpcServerConfig {
                        acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 4567)).await?,
                        tls_config: None,
                    }),
                    disable_namespaces: false,
                    disable_default_namespace: true,
                    ..Default::default()
                };

                server.start().await?;

                Ok(())
            }
        });
    }

    #[test]
    fn fork_namespace() {
        let mut sim = Builder::new().build();
        let tmp = tempdir().unwrap();
        make_primary(&mut sim, tmp.path().to_path_buf());

        sim.client("client", async {
            let client = Client::new();
            client
                .post("http://primary:9090/v1/namespaces/foo/create", json!({}))
                .await?;

            let foo = Database::open_remote_with_connector(
                "http://foo.primary:8080",
                "",
                TurmoilConnector,
            )?;
            let foo_conn = foo.connect()?;

            foo_conn.execute("create table test (c)", ()).await?;
            foo_conn.execute("insert into test values (42)", ()).await?;

            client
                .post("http://primary:9090/v1/namespaces/foo/fork/bar", ())
                .await?;

            let bar = Database::open_remote_with_connector(
                "http://bar.primary:8080",
                "",
                TurmoilConnector,
            )?;
            let bar_conn = bar.connect()?;

            // what's in foo is in bar as well
            let mut rows = bar_conn.query("select count(*) from test", ()).await?;
            assert!(matches!(
                rows.next().unwrap().unwrap().get_value(0).unwrap(),
                Value::Integer(1)
            ));

            bar_conn.execute("insert into test values (42)", ()).await?;

            // add something to bar
            let mut rows = bar_conn.query("select count(*) from test", ()).await?;
            assert!(matches!(
                rows.next().unwrap().unwrap().get_value(0)?,
                Value::Integer(2)
            ));

            // ... and make sure it doesn't exist in foo
            let mut rows = foo_conn.query("select count(*) from test", ()).await?;
            assert!(matches!(
                rows.next().unwrap().unwrap().get_value(0)?,
                Value::Integer(1)
            ));

            Ok(())
        });

        sim.run().unwrap();
    }

    #[test]
    fn delete_namespace() {
        let mut sim = Builder::new().build();
        let tmp = tempdir().unwrap();
        make_primary(&mut sim, tmp.path().to_path_buf());

        sim.client("client", async {
            let client = Client::new();
            client
                .post("http://primary:9090/v1/namespaces/foo/create", json!({}))
                .await?;

            let foo = Database::open_remote_with_connector(
                "http://foo.primary:8080",
                "",
                TurmoilConnector,
            )?;
            let foo_conn = foo.connect()?;
            foo_conn.execute("create table test (c)", ()).await?;

            client
                .post("http://primary:9090/v1/namespaces/foo/destroy", json!({}))
                .await
                .unwrap();
            // namespace doesn't exist anymore
            assert!(foo_conn.execute("create table test (c)", ()).await.is_err());

            Ok(())
        });

        sim.run().unwrap();
    }

    #[test]
    #[ignore = "need to remove block in place in load dump, wait for #693"]
    fn load_namespace_from_dump_from_url() {
        const DUMP: &str = r#"
PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE test (x);
INSERT INTO test VALUES(42);
COMMIT;"#;

        let mut sim = Builder::new().build();
        let tmp = tempdir().unwrap();
        make_primary(&mut sim, tmp.path().to_path_buf());

        sim.host("dump-store", || async {
            let incoming = TurmoilAcceptor::bind(([0, 0, 0, 0], 8080)).await?;
            let server = hyper::server::Server::builder(incoming).serve(make_service_fn(|_conn| async {
                Ok::<_, Infallible>(service_fn(|_req| async {
                    Ok::<_, Infallible>(Response::new(Body::from(DUMP)))
                }))
            }));

            server.await.unwrap();

            Ok(())
        });

        sim.client("client", async {
            let client = Client::new();
            client.post("http://primary:9090/v1/namespaces/foo/create", json!({ "dump_url": "http://dump-store:8080/"})).await.unwrap().json::<serde_json::Value>().await.unwrap();

            let foo = Database::open_remote_with_connector("http://foo.primary:8080", "", TurmoilConnector)?;
            let foo_conn = foo.connect()?;
            let mut rows = foo_conn.query("select count(*) from test", ()).await?;
            assert!(matches!(
                rows.next().unwrap().unwrap().get_value(0)?,
                Value::Integer(1)
            ));

            Ok(())
        });

        sim.run().unwrap();
    }

    #[test]
    #[ignore = "need to remove block in place in load dump, wait for #693"]
    fn load_namespace_from_dump_from_file() {
        const DUMP: &str = r#"
PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE test (x);
INSERT INTO test VALUES(42);
COMMIT;"#;

        let mut sim = Builder::new().build();
        let tmp = tempdir().unwrap();
        let tmp_path = tmp.path().to_path_buf();

        std::fs::write(tmp_path.join("dump.sql"), &DUMP).unwrap();

        make_primary(&mut sim, tmp.path().to_path_buf());

        sim.client("client", async move {
            let client = Client::new();

            // path is not absolute is an error
            let resp = client.post("http://primary:9090/v1/namespaces/foo/create", json!({ "dump_url": "file:dump.sql"})).await.unwrap();
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

            // path doesn't exist is an error
            let resp = client.post("http://primary:9090/v1/namespaces/foo/create", json!({ "dump_url": "file:/dump.sql"})).await.unwrap();
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

            let resp = client.post("http://primary:9090/v1/namespaces/foo/create", json!({ "dump_url": format!("file:{}", tmp_path.join("dump.sql").display())})).await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK, "{}", resp.json::<serde_json::Value>().await.unwrap_or_default());

            let foo = Database::open_remote_with_connector("http://foo.primary:8080", "", TurmoilConnector)?;
            let foo_conn = foo.connect()?;
            let mut rows = foo_conn.query("select count(*) from test", ()).await?;
            assert!(matches!(
                rows.next().unwrap().unwrap().get_value(0)?,
                Value::Integer(1)
            ));

            Ok(())
        });

        sim.run().unwrap();
    }
}
