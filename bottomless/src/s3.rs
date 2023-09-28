use crate::replicator::CompressionKind;
use crate::uuid_utils::GenerationUuid;
use anyhow::Result;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::operation::list_objects::ListObjectsOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use bytes::Buf;
use chrono::NaiveDateTime;
use std::fmt::{Display, Formatter};
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

    /// Unique name of a database, this client refers to.
    pub fn db_name(&self) -> &str {
        &self.keyspace.db_name
    }

    /// Returns an async iterator over available generations for a current database
    /// identified by [Self::db_name].
    pub fn list_generations(&self) -> Generations {
        Generations::new(self.client.clone(), self.keyspace.clone())
    }

    /// Returns newest replicated generation, or None, if one is not found.
    pub async fn latest_generation_before(
        &self,
        threshold: Option<&NaiveDateTime>,
    ) -> Option<Uuid> {
        if let Some(t) = threshold {
            tracing::trace!("Looking for latest generation before {}", t);
        } else {
            tracing::trace!("Looking for latest generation");
        }
        let mut next_marker: Option<String> = None;
        let prefix = format!("{}-", self.db_name());
        loop {
            let mut request = self
                .client
                .list_objects()
                .bucket(self.bucket())
                .prefix(prefix.clone());
            if threshold.is_none() {
                request = request.max_keys(1);
            }
            if let Some(marker) = next_marker.take() {
                request = request.marker(marker);
            }
            let response = request.send().await.ok()?;
            let objs = response.contents()?;
            if objs.is_empty() {
                break;
            }
            let mut last_key = None;
            let mut last_gen = None;
            for obj in objs {
                let key = obj.key();
                last_key = key;
                if let Some(key) = last_key {
                    let key = match key.find('/') {
                        Some(index) => &key[self.db_name().len() + 1..index],
                        None => key,
                    };
                    if Some(key) != last_gen {
                        last_gen = Some(key);
                        if let Ok(generation) = Uuid::parse_str(key) {
                            match threshold {
                                None => return Some(generation),
                                Some(threshold) => match generation.date_time() {
                                    None => {
                                        tracing::warn!(
                                            "Generation {} is not valid UUID v7",
                                            generation
                                        );
                                    }
                                    Some(ts) => {
                                        if tracing::enabled!(tracing::Level::DEBUG) {
                                            tracing::debug!(
                                                "Generation candidate: {} - timestamp: {}",
                                                generation,
                                                ts
                                            );
                                        }
                                        if ts <= *threshold {
                                            return Some(generation);
                                        }
                                    }
                                },
                            }
                        }
                    }
                }
            }
            next_marker = last_key.map(String::from);
        }
        None
    }

    /// Total summary of all backup-related properties in scope of a provided generation.
    pub async fn generation_summary(&self, generation: Uuid) -> Result<GenerationSummary> {
        let parent = self.get_dependency(&generation).await?;
        let last_wal_segment = self.get_last_wal_segment(&generation).await?;
        let change_counter = self.get_change_counter(&generation).await?;
        let m = self.get_metadata(&generation).await?;
        let has_snapshot = self.has_snapshot(&generation).await?;
        Ok(GenerationSummary {
            generation,
            parent,
            has_snapshot,
            last_wal_segment,
            change_counter,
            page_size: m.map(|(page_size, _)| page_size),
            init_crc: m.map(|(_, crc)| crc).unwrap_or(0),
        })
    }

    /// Confirms that provided generation has been snapshotted.
    pub async fn has_snapshot(&self, generation: &Uuid) -> Result<bool> {
        let key = format!("{}-{}/db.gz", self.db_name(), generation);
        self.key_exists(key).await
    }

    async fn key_exists<S: Into<String>>(&self, key: S) -> Result<bool> {
        let resp = self
            .client
            .head_object()
            .bucket(self.bucket())
            .key(key)
            .send()
            .await;
        let found = resp.exists()?.is_some();
        Ok(found)
    }

    pub async fn try_get<S: Display>(
        &self,
        generation: &Uuid,
        suffix: S,
    ) -> Result<Option<ByteStream>> {
        let key = format!("{}-{}/{}", self.db_name(), generation, suffix);
        let resp = self
            .client
            .get_object()
            .bucket(self.bucket())
            .key(key)
            .send()
            .await;
        if let Some(out) = resp.exists()? {
            Ok(Some(out.body))
        } else {
            Ok(None)
        }
    }

    async fn put<S: Display>(&self, generation: &Uuid, suffix: S, data: &[u8]) -> Result<()> {
        let key = format!("{}-{}/{}", self.db_name(), generation, suffix);
        let body = Vec::from(data);
        let _ = self
            .client
            .put_object()
            .bucket(self.bucket())
            .key(key)
            .body(ByteStream::from(body))
            .send()
            .await?;
        Ok(())
    }

    /// Get ID of a generation preceding a provided one.
    pub async fn get_dependency(&self, generation: &Uuid) -> Result<Option<Uuid>> {
        let bytes = self.try_get(generation, ".dep").await?;
        if let Some(bytes) = bytes {
            let data = bytes.collect().await?.into_bytes();
            let prev_generation = Uuid::from_bytes(data.as_ref().try_into()?);
            Ok(Some(prev_generation))
        } else {
            Ok(None)
        }
    }

    /// Request to store dependency between current generation and its predecessor on S3 object.
    /// This works asynchronously on best-effort rules, as putting object to S3 introduces an
    /// extra undesired latency and this method may be called during SQLite checkpoint.
    pub async fn store_dependency(&self, prev: &Uuid, curr: &Uuid) -> Result<()> {
        self.put(curr, ".dep", (*prev).into_bytes().as_slice())
            .await
    }

    pub async fn get_metadata(&self, generation: &Uuid) -> Result<Option<(u32, u64)>> {
        let data = self.try_get(generation, ".meta").await?;
        if let Some(data) = data {
            let mut d = data.collect().await?.into_bytes();
            let page_size = d.get_u32();
            let crc = d.get_u64();
            Ok(Some((page_size, crc)))
        } else {
            Ok(None)
        }
    }

    pub async fn store_metadata(&self, generation: &Uuid, page_size: u32, crc: u64) -> Result<()> {
        let mut body = Vec::with_capacity(12);
        body.extend_from_slice(page_size.to_be_bytes().as_slice());
        body.extend_from_slice(crc.to_be_bytes().as_slice());
        self.put(generation, ".meta", &body).await
    }

    /// Tries to fetch the remote database change counter from given generation
    pub async fn get_change_counter(&self, generation: &Uuid) -> Result<[u8; 4]> {
        let mut change_counter = [0u8; 4];
        if let Some(data) = self.try_get(generation, ".changecounter").await? {
            let mut data = data.collect().await?.into_bytes();
            data.copy_to_slice(&mut change_counter)
        }
        Ok(change_counter)
    }

    pub async fn get_last_wal_segment(
        &self,
        generation: &Uuid,
    ) -> Result<Option<WalSegmentSummary>> {
        let prefix = format!("{}-{}/", self.db_name(), generation);
        let mut marker: Option<String> = None;
        let mut last_frame = None;
        while {
            let mut list_objects = self
                .client
                .list_objects()
                .bucket(self.bucket())
                .prefix(&prefix);
            if let Some(marker) = marker.take() {
                list_objects = list_objects.marker(marker);
            }
            let response = list_objects.send().await?;
            marker = Self::last_wal_segment_in(response, &mut last_frame);
            marker.is_some()
        } {}
        Ok(last_frame)
    }

    fn last_wal_segment_in(
        response: ListObjectsOutput,
        frame_no: &mut Option<WalSegmentSummary>,
    ) -> Option<String> {
        let objs = response.contents()?;
        let mut last_key = None;
        for obj in objs.iter() {
            last_key = Some(obj.key()?);
            if let Some(key) = last_key {
                if let Some(summary) = WalSegmentSummary::parse(key) {
                    *frame_no = Some(summary);
                }
            }
        }
        last_key.map(String::from)
    }

    /// Marks current database as deleted, invalidating all generations.
    pub async fn delete_all(&self, older_than: Option<NaiveDateTime>) -> Result<DeleteAll> {
        tracing::info!(
            "Called for tombstoning of all contents of the '{}' database",
            self.db_name()
        );
        let db_name = self.keyspace.db_name.clone();
        let bucket = self.keyspace.bucket.clone();
        let key = format!("{}.tombstone", db_name);
        let threshold = older_than.unwrap_or(NaiveDateTime::MAX);
        self.client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(
                threshold.timestamp().to_be_bytes().to_vec(),
            ))
            .send()
            .await?;
        let delete_task = DeleteAll::new(self.client.clone(), bucket, db_name, threshold);
        Ok(delete_task)
    }

    /// Checks if current replicator database has been marked as deleted.
    pub async fn get_tombstone(&self) -> Result<Option<NaiveDateTime>> {
        let key = format!("{}.tombstone", self.db_name());
        let resp = self
            .client
            .get_object()
            .bucket(self.bucket())
            .key(key)
            .send()
            .await;
        if let Some(out) = resp.exists()? {
            let mut buf = [0u8; 8];
            out.body.collect().await?.copy_to_slice(&mut buf);
            let timestamp = i64::from_be_bytes(buf);
            let tombstone = NaiveDateTime::from_timestamp_opt(timestamp, 0);
            Ok(tombstone)
        } else {
            Ok(None)
        }
    }
}

impl AsRef<Client> for S3Client {
    fn as_ref(&self) -> &Client {
        &self.client
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WalSegmentSummary {
    /// Number (in current generation) of the first frame included by corresponding WAL segment.
    pub first_frame_no: u32,
    /// Number (in current generation) of the last frame included by corresponding WAL segment.
    pub last_frame_no: u32,
    /// Timestamp when current WAL segment was created (with seconds resolution).
    pub timestamp: NaiveDateTime,
    /// Type of compression used on frames stored in corresponding WAL segment.
    pub compression_kind: CompressionKind,
}

impl WalSegmentSummary {
    /// Parses the frame and page number from given key.
    /// Format: <db-name>-<generation>/<first-frame-no>-<last-frame-no>-<timestamp>.<compression-kind>
    pub fn parse(key: &str) -> Option<Self> {
        let frame_delim = key.rfind('/')?;
        let frame_suffix = &key[(frame_delim + 1)..];
        let timestamp_delim = frame_suffix.rfind('-')?;
        let last_frame_delim = frame_suffix[..timestamp_delim].rfind('-')?;
        let compression_delim = frame_suffix.rfind('.')?;
        let first_frame_no = frame_suffix[0..last_frame_delim].parse::<u32>().ok()?;
        let last_frame_no = frame_suffix[(last_frame_delim + 1)..timestamp_delim]
            .parse::<u32>()
            .ok()?;
        let timestamp = frame_suffix[(timestamp_delim + 1)..compression_delim]
            .parse::<i64>()
            .ok()?;
        let timestamp = NaiveDateTime::from_timestamp_opt(timestamp, 0)?;
        let compression_kind =
            CompressionKind::parse(&frame_suffix[(compression_delim + 1)..]).ok()?;
        Some(WalSegmentSummary {
            first_frame_no,
            last_frame_no,
            timestamp,
            compression_kind,
        })
    }
}

impl Display for WalSegmentSummary {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:012}-{:012}-{}.{}",
            self.first_frame_no,
            self.last_frame_no,
            self.timestamp.timestamp() as u64,
            self.compression_kind
        )
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
    /// Unique ID of current generation.
    pub generation: Uuid,
    /// ID of a parent, this generation was created from.
    pub parent: Option<Uuid>,
    /// Does current generation contains a snapshot?
    pub has_snapshot: bool,
    /// Summary of the last WAL segment found in current generation. It contains useful information
    /// like the latest WAL frame number or the last timestamp.
    pub last_wal_segment: Option<WalSegmentSummary>,
    /// Change counter used to compare current local DB version against the one found in a snapshot.
    pub change_counter: [u8; 4],
    /// SQLite page size.
    pub page_size: Option<u32>,
    /// Init checksum used to compute subsequent CRC validation chains for applied WAL segments.
    pub init_crc: u64,
}

impl GenerationSummary {
    pub fn created_at(&self) -> Option<NaiveDateTime> {
        self.generation.date_time()
    }
}

/// This structure is returned by [Replicator::delete_all] after tombstoning (soft deletion) has
/// been confirmed. It may be called using [DeleteAll::commit] to trigger a follow up procedure that
/// performs hard deletion of corresponding S3 objects.
#[derive(Debug)]
pub struct DeleteAll {
    client: Client,
    bucket: String,
    db_name: String,
    threshold: NaiveDateTime,
}

impl DeleteAll {
    fn new(client: Client, bucket: String, db_name: String, threshold: NaiveDateTime) -> Self {
        DeleteAll {
            client,
            bucket,
            db_name,
            threshold,
        }
    }

    pub fn threshold(&self) -> &NaiveDateTime {
        &self.threshold
    }

    /// Performs hard deletion of all bottomless generations older than timestamp provided in
    /// current request.
    pub async fn commit(self) -> Result<u32> {
        let mut next_marker = None;
        let mut removed_count = 0;
        loop {
            let mut list_request = self
                .client
                .list_objects()
                .bucket(&self.bucket)
                .set_delimiter(Some("/".to_string()))
                .prefix(&self.db_name);

            if let Some(marker) = next_marker {
                list_request = list_request.marker(marker)
            }

            let response = list_request.send().await?;
            let prefixes = match response.common_prefixes() {
                Some(prefixes) => prefixes,
                None => {
                    tracing::debug!("no generations found to delete");
                    return Ok(0);
                }
            };

            for prefix in prefixes {
                if let Some(prefix) = &prefix.prefix {
                    let prefix = &prefix[self.db_name.len() + 1..prefix.len() - 1];
                    let uuid = Uuid::try_parse(prefix)?;
                    if let Some(datetime) = uuid.date_time() {
                        if datetime >= self.threshold {
                            continue;
                        }
                        tracing::debug!("Removing generation {}", uuid);
                        self.remove(uuid).await?;
                        removed_count += 1;
                    }
                }
            }

            next_marker = response.next_marker().map(|s| s.to_owned());
            if next_marker.is_none() {
                break;
            }
        }
        tracing::debug!("Removed {} generations", removed_count);
        self.remove_tombstone().await?;
        Ok(removed_count)
    }

    pub async fn remove_tombstone(&self) -> Result<()> {
        let key = format!("{}.tombstone", self.db_name);
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;
        Ok(())
    }

    async fn remove(&self, generation: Uuid) -> Result<()> {
        let mut removed = 0;
        let mut next_marker = None;
        loop {
            let mut list_request = self
                .client
                .list_objects()
                .bucket(&self.bucket)
                .prefix(format!("{}-{}/", &self.db_name, generation));

            if let Some(marker) = next_marker {
                list_request = list_request.marker(marker)
            }

            let response = list_request.send().await?;
            let objs = match response.contents() {
                Some(prefixes) => prefixes,
                None => {
                    return Ok(());
                }
            };

            for obj in objs {
                if let Some(key) = obj.key() {
                    tracing::trace!("Removing {}", key);
                    self.client
                        .delete_object()
                        .bucket(&self.bucket)
                        .key(key)
                        .send()
                        .await?;
                    removed += 1;
                }
            }

            next_marker = response.next_marker().map(|s| s.to_owned());
            if next_marker.is_none() {
                tracing::trace!("Removed {} snapshot generations", removed);
                return Ok(());
            }
        }
    }
}

trait S3ObjectOptional {
    type Result;
    fn exists(self) -> Result<Option<Self::Result>>;
}

impl<A> S3ObjectOptional for std::result::Result<A, SdkError<GetObjectError>> {
    type Result = A;

    fn exists(self) -> Result<Option<Self::Result>> {
        match self {
            Ok(out) => Ok(Some(out)),
            Err(SdkError::ServiceError(se)) => match se.into_err() {
                GetObjectError::NoSuchKey(_) => Ok(None),
                e => Err(e.into()),
            },
            Err(e) => Err(e.into()),
        }
    }
}

impl<A> S3ObjectOptional for std::result::Result<A, SdkError<HeadObjectError>> {
    type Result = A;

    fn exists(self) -> Result<Option<Self::Result>> {
        match self {
            Ok(out) => Ok(Some(out)),
            Err(SdkError::ServiceError(se)) => match se.into_err() {
                HeadObjectError::NotFound(_) => Ok(None),
                e => Err(e.into()),
            },
            Err(e) => Err(e.into()),
        }
    }
}
