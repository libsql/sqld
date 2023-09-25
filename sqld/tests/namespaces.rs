mod common;

#[cfg(feature = "sim-tests")]
mod test {
    use crate::common::net::TurmoilConnector;

    use super::common;

    use std::path::PathBuf;

    use common::{net::{TestServer, TurmoilAcceptor}, http::Client};
    use libsql::{Database, Value};
    use serde_json::json;
    use sqld::config::{UserApiConfig, AdminApiConfig, RpcServerConfig};
    use tempfile::tempdir;
    use turmoil::{Builder, Sim};

    fn make_primary(sim: &mut Sim, path: PathBuf) {
        sim.host("primary", move || {
            let path = path.clone();
            async move  {
                let server = TestServer {
                    path: path.into(),
                    user_api_config: UserApiConfig {
                        http_acceptor: Some(TurmoilAcceptor::bind(([0, 0, 0, 0], 8080)).await?),
                        ..Default::default()
                    },
                    admin_api_config: Some(AdminApiConfig {
                        acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 9090)).await?,
                    }),
                    rpc_server_config: Some(RpcServerConfig {
                        acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 4567)).await?,
                        tls_config: None,
                    }),
                    disable_namespaces: false,
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
            client.post("http://primary:9090/v1/namespaces/foo/create", json!({})).await.unwrap();

            let foo = Database::open_remote_with_connector("http://foo.primary:8080", "", TurmoilConnector)?;
            let foo_conn = foo.connect().unwrap();

            foo_conn.execute("create table test (c)", ()).await.unwrap();
            foo_conn.execute("insert into test values (42)", ()).await.unwrap();

            client.post("http://primary:9090/v1/namespaces/foo/fork/bar", ()).await.unwrap();

            let bar = Database::open_remote_with_connector("http://bar.primary:8080", "", TurmoilConnector)?;
            let bar_conn = bar.connect().unwrap();

            // what's in foo is in bar as well
            let mut rows = bar_conn.query("select count(*) from test", ()).await.unwrap();
            assert!(matches!(
                rows.next().unwrap().unwrap().get_value(0).unwrap(),
                Value::Integer(1)
            ));

            bar_conn.execute("insert into test values (42)", ()).await.unwrap();

            // add something to bar
            let mut rows = bar_conn.query("select count(*) from test", ()).await.unwrap();
            assert!(matches!(
                rows.next().unwrap().unwrap().get_value(0).unwrap(),
                Value::Integer(2)
            ));

            // ... and make sure it doesn't exist in foo
            let mut rows = foo_conn.query("select count(*) from test", ()).await.unwrap();
            assert!(matches!(
                rows.next().unwrap().unwrap().get_value(0).unwrap(),
                Value::Integer(1)
            ));

            Ok(())
        });

        sim.run().unwrap();
    }
}
