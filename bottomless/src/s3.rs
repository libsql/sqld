use crate::replicator::Replicator;
use anyhow::Result;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::list_objects::ListObjectsOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use bytes::Bytes;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct S3Client {
    keyspace: Keyspace,
    client: Client,
}

impl S3Client {
    pub fn new(s3: Client, bucket: String, db_name: String) -> Self {
        S3Client {
            client: s3,
            keyspace: Keyspace::new(bucket, db_name),
        }
    }

    pub fn bucket(&self) -> &str {
        &self.keyspace.bucket
    }

    pub fn db_name(&self) -> &str {
        &self.keyspace.db_name
    }

    pub fn list_generations(&self) -> Generations {
        Generations::new(self.client.clone(), self.keyspace.clone())
    }

    pub async fn generation_summary(&self, generation: Uuid) -> Result<GenerationSummary> {
        todo!()
    }

    pub async fn get_dependency(&self, generation: &Uuid) -> Result<Option<Uuid>> {
        let key = format!("{}-{}/.dep", self.db_name(), generation);
        let resp = self
            .client
            .get_object()
            .bucket(self.bucket())
            .key(key)
            .send()
            .await;
        match resp {
            Ok(out) => {
                let bytes = out.body.collect().await?.into_bytes();
                let prev_generation = Uuid::from_bytes(bytes.as_ref().try_into()?);
                Ok(Some(prev_generation))
            }
            Err(SdkError::ServiceError(se)) => match se.into_err() {
                GetObjectError::NoSuchKey(_) => Ok(None),
                e => Err(e.into()),
            },
            Err(e) => Err(e.into()),
        }
    }

    /// Request to store dependency between current generation and its predecessor on S3 object.
    /// This works asynchronously on best-effort rules, as putting object to S3 introduces an
    /// extra undesired latency and this method may be called during SQLite checkpoint.
    fn store_dependency(&self, prev: Uuid, curr: Uuid) {
        let key = format!("{}-{}/.dep", self.db_name(), curr);
        let request = self
            .client
            .put_object()
            .bucket(self.bucket())
            .key(key)
            .body(ByteStream::from(Bytes::copy_from_slice(
                prev.into_bytes().as_slice(),
            )));
        tokio::spawn(async move {
            if let Err(e) = request.send().await {
                tracing::error!(
                    "Failed to store dependency between generations {} -> {}: {}",
                    prev,
                    curr,
                    e
                );
            } else {
                tracing::trace!(
                    "Stored dependency between parent ({}) and child ({})",
                    prev,
                    curr
                );
            }
        });
    }

    pub async fn get_last_consistent_frame(&self, generation: &Uuid) -> Result<u32> {
        let prefix = format!("{}-{}/", self.db_name(), generation);
        let mut marker: Option<String> = None;
        let mut last_frame = 0;
        while {
            let mut list_objects = self.client.list_objects().prefix(&prefix);
            if let Some(marker) = marker.take() {
                list_objects = list_objects.marker(marker);
            }
            let response = list_objects.send().await?;
            marker = Self::try_get_last_frame_no(response, &mut last_frame);
            marker.is_some()
        } {}
        Ok(last_frame)
    }

    fn try_get_last_frame_no(response: ListObjectsOutput, frame_no: &mut u32) -> Option<String> {
        let objs = response.contents()?;
        let mut last_key = None;
        for obj in objs.iter() {
            last_key = Some(obj.key()?);
            if let Some(key) = last_key {
                if let Some((_, last_frame_no, _, _)) = Replicator::parse_frame_range(key) {
                    *frame_no = last_frame_no;
                }
            }
        }
        last_key.map(String::from)
    }
}

#[derive(Debug)]
pub struct Generations {
    client: Client,
    keyspace: Keyspace,
    next_marker: Option<String>,
    latest_resp: Option<ListObjectsOutput>,
    offset: usize,
}

impl Generations {
    fn new(client: Client, keyspace: Keyspace) -> Self {
        Generations {
            client,
            keyspace,
            next_marker: None,
            latest_resp: None,
            offset: 0,
        }
    }

    pub async fn next(&mut self) -> Result<Option<Uuid>> {
        loop {
            let resp: &ListObjectsOutput = if let Some(resp) = &self.latest_resp {
                resp
            } else {
                let resp = self.request_next_batch().await?;
                self.latest_resp = Some(resp);
                self.latest_resp.as_ref().unwrap()
            };
            let prefixes = if let Some(prefixes) = resp.common_prefixes() {
                prefixes
            } else {
                return Ok(None);
            };
            while self.offset < prefixes.len() {
                let prefix = &prefixes[self.offset];
                self.offset += 1;
                if let Some(p) = prefix.prefix() {
                    let prefix = &p[self.keyspace.db_name.len() + 1..p.len() - 1];
                    let generation = Uuid::try_parse(prefix)?;
                    return Ok(Some(generation));
                }
            }
            self.next_marker = resp.next_marker().map(|s| s.to_owned());
            if self.next_marker.is_none() {
                return Ok(None);
            }
        }
    }

    async fn request_next_batch(&mut self) -> Result<ListObjectsOutput> {
        self.offset = 0;
        let mut list_request = self
            .client
            .list_objects()
            .bucket(&self.keyspace.bucket)
            .set_delimiter(Some("/".to_string()))
            .prefix(&self.keyspace.db_name);

        if let Some(marker) = self.next_marker.take() {
            list_request = list_request.marker(marker)
        }
        let resp = list_request.send().await?;
        Ok(resp)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Keyspace {
    pub bucket: String,
    pub db_name: String,
}
impl Keyspace {
    pub fn new(bucket: String, db_id: String) -> Self {
        Keyspace {
            bucket,
            db_name: db_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenerationSummary {
    pub id: Uuid,
    pub parent: Option<Uuid>,
    pub has_snapshot: bool,
    pub last_frame_no: u32,
}
