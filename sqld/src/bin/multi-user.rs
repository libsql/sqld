use std::io::Read;

use anyhow::Result;
use bytes::Buf;
use hyper::{client::HttpConnector, Body, Request, StatusCode};

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::default();

    client.create_namespace("foo2").await?;
    client.do_inserts("foo2").await?;

    client.create_namespace("foo3").await?;
    client.do_inserts("foo3").await?;

    client.create_namespace("foo4").await?;
    client.do_inserts("foo4").await?;

    Ok(())
}

#[derive(Default)]
struct Client {
    client: hyper::Client<HttpConnector>,
}

impl Client {
    async fn create_namespace(&self, name: &str) -> Result<()> {
        let req = Request::post(format!(
            "http://localhost:8080/v1/namespace/{}/create",
            name
        ))
        .body(Body::empty())
        .unwrap();

        self.send(req).await?;

        Ok(())
    }

    async fn do_inserts(&self, name: &str) -> Result<()> {
        let json = serde_json::json!({
            "statements": [
                "CREATE TABLE IF NOT EXISTS foo (b BLOB)",
                "INSERT INTO foo VALUES (zeroblob(1024))",
                "INSERT INTO foo VALUES (zeroblob(1024))",
                "INSERT INTO foo VALUES (zeroblob(1024))",
                "INSERT INTO foo VALUES (zeroblob(1024))",
                "INSERT INTO foo VALUES (zeroblob(1024))"
            ]
        });

        let req = Request::post("http://localhost:8080")
            .header("namespace", name)
            .body(Body::from(serde_json::to_vec(&json)?))
            .unwrap();

        self.send(req).await?;

        Ok(())
    }

    async fn send(&self, req: Request<Body>) -> Result<()> {
        let res = self.client.request(req).await?;

        if res.status() != StatusCode::OK {
            let (res, body) = res.into_parts();
            let b = hyper::body::aggregate(body).await?;
            let mut s = String::new();
            b.reader().read_to_string(&mut s)?;
            println!("failed request: {:?}, {:?}", res, s);
        }

        Ok(())
    }
}
