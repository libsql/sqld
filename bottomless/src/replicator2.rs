use arc_swap::ArcSwap;
use async_compression::tokio::bufread::GzipEncoder;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::builders::GetObjectFluentBuilder;
use aws_sdk_s3::operation::list_objects::builders::ListObjectsFluentBuilder;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use bytes::{Bytes, BytesMut};
use std::io::{Cursor, SeekFrom};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::time::{sleep, timeout};
use tokio::{select, spawn};

pub type Result<T> = anyhow::Result<T>;

const CRC_64: crc::Crc<u64> = crc::Crc::<u64>::new(&crc::CRC_64_ECMA_182);

#[derive(Debug)]
struct Frame {
    frame_no: u32,
    pgno: u32,
    wal_offset: u64,
    commit_frame: bool,
}

#[derive(Debug)]
pub struct Replicator {
    client: Client,
    write_buffer: UnboundedSender<Frame>,

    page_size: AtomicUsize,
    generation: ArcSwap<uuid::Uuid>,
    commits_in_current_generation: AtomicU32,
    next_frame: AtomicU32,
    verify_crc: bool,
    last_frame_crc: AtomicU64,
    bucket: String,
    db_path: String,
    db_name: String,
    use_compression: bool,
}

impl Replicator {
    pub const UNSET_PAGE_SIZE: usize = usize::MAX;

    pub async fn new() -> Result<Arc<Self>> {
        Self::create(Options::default()).await
    }

    pub async fn create(options: Options) -> Result<Arc<Self>> {
        let (write_buffer, mut write_receiver) = unbounded_channel();
        let mut loader = aws_config::from_env();
        if let Some(endpoint) = options.aws_endpoint.as_deref() {
            loader = loader.endpoint_url(endpoint);
        }
        let bucket = options.bucket_name.clone();
        let conf = aws_sdk_s3::config::Builder::from(&loader.load().await)
            .force_path_style(true)
            .build();
        let client = Client::from_conf(conf);
        let generation = Self::generate_generation();
        tracing::debug!("Generation {}", generation);

        match client.head_bucket().bucket(&bucket).send().await {
            Ok(_) => tracing::info!("Bucket {} exists and is accessible", bucket),
            Err(SdkError::ServiceError(err)) if err.err().is_not_found() => {
                if options.create_bucket_if_not_exists {
                    tracing::info!("Bucket {} not found, recreating", bucket);
                    client.create_bucket().bucket(&bucket).send().await?;
                } else {
                    tracing::error!("Bucket {} does not exist", bucket);
                    return Err(SdkError::ServiceError(err).into());
                }
            }
            Err(e) => {
                tracing::error!("Bucket checking error: {}", e);
                return Err(e.into());
            }
        }

        let replicator = Arc::new(Self {
            client,
            write_buffer,
            bucket,
            page_size: AtomicUsize::new(Self::UNSET_PAGE_SIZE),
            generation: ArcSwap::new(Arc::new(generation)),
            commits_in_current_generation: AtomicU32::new(0),
            next_frame: AtomicU32::new(0),
            verify_crc: options.verify_crc,
            last_frame_crc: AtomicU64::new(0),
            db_path: String::new(),
            db_name: String::new(),
            use_compression: options.use_compression,
        });

        {
            let batch_interval = options.frame_batch_interval;
            let batch_size = options.frame_batch_max_size;
            let replicator = Arc::downgrade(&replicator);
            spawn(async move {
                while let Some(head) = write_receiver.recv().await {
                    if let Some(replicator) = replicator.upgrade() {
                        let interval = sleep(batch_interval);
                        let frames_per_batch =
                            batch_size / (replicator.page_size() + WAL_HEADER_SIZE as usize);
                        let mut frames = Vec::with_capacity(frames_per_batch);
                        frames.push(head);
                        // after receiving first frame in a new batch, try to continue receiving
                        // frames until a batch is filled or interval is reached
                        let _ = timeout(batch_interval, async {
                            while let Some(frame) = write_receiver.recv().await {
                                let last_frame_no = frames[frames.len() - 1].frame_no;
                                debug_assert!(frame.frame_no == last_frame_no + 1);

                                frames.push(frame);
                                if frames.len() >= frames_per_batch {
                                    break;
                                }
                            }
                        })
                        .await;
                        if let Err(e) = replicator.flush(frames).await {
                            replicator.abort(e);
                            return;
                        }
                    } else {
                        break;
                    }
                }
                tracing::info!("closing replicator due to closed frame receiver");
            });
        }

        Ok(replicator)
    }

    fn page_size(&self) -> usize {
        self.page_size.load(Ordering::Acquire)
    }

    // The database can use different page size - as soon as it's known,
    // it should be communicated to the replicator via this call.
    // NOTICE: in practice, WAL journaling mode does not allow changing page sizes,
    // so verifying that it hasn't changed is a panic check. Perhaps in the future
    // it will be useful, if WAL ever allows changing the page size.
    pub fn set_page_size(&self, page_size: usize) -> Result<()> {
        let curr_page_size = self.page_size();
        tracing::trace!("Setting page size from {} to {}", curr_page_size, page_size);
        if curr_page_size != Self::UNSET_PAGE_SIZE && curr_page_size != page_size {
            return Err(anyhow::anyhow!(
                "Cannot set page size to {}, it was already set to {}",
                page_size,
                curr_page_size
            ));
        }
        self.page_size.store(page_size, Ordering::AcqRel);
        Ok(())
    }

    // Gets an object from the current bucket
    fn get_object(&self, key: String) -> GetObjectFluentBuilder {
        self.client.get_object().bucket(&self.bucket).key(key)
    }

    // Lists objects from the current bucket
    fn list_objects(&self) -> ListObjectsFluentBuilder {
        self.client.list_objects().bucket(&self.bucket)
    }

    // Generates a new generation UUID v7, which contains a timestamp and is binary-sortable.
    // This timestamp goes back in time - that allows us to list newest generations
    // first in the S3-compatible bucket, under the assumption that fetching newest generations
    // is the most common operation.
    // NOTICE: at the time of writing, uuid v7 is an unstable feature of the uuid crate
    fn generate_generation() -> uuid::Uuid {
        let (seconds, nanos) = uuid::timestamp::Timestamp::now(uuid::NoContext).to_unix();
        let (seconds, nanos) = (253370761200 - seconds, 999999999 - nanos);
        let synthetic_ts = uuid::Timestamp::from_unix(uuid::NoContext, seconds, nanos);
        uuid::Uuid::new_v7(synthetic_ts)
    }

    // Starts a new generation for this replicator instance
    pub fn new_generation(&self) {
        tracing::debug!("New generation started: {}", self.generation);
        self.set_generation(Self::generate_generation());
    }

    // Sets a generation for this replicator instance. This function
    // should be called if a generation number from S3-compatible storage
    // is reused in this session.
    pub fn set_generation(&self, generation: uuid::Uuid) {
        self.generation.swap(Arc::new(generation));
        self.commits_in_current_generation
            .store(0, Ordering::AcqRel);
        self.next_frame.store(0, Ordering::AcqRel); // New generation marks a new WAL
        tracing::debug!("Generation set to {}", self.generation);
    }

    pub fn generation(&self) -> uuid::Uuid {
        **self.generation.load()
    }

    // Registers a database path for this replicator.
    pub fn register_db(&mut self, db_path: impl Into<String>) {
        let db_path = db_path.into();
        // An optional prefix to differentiate between databases with the same filename
        let db_id = std::env::var("LIBSQL_BOTTOMLESS_DATABASE_ID").unwrap_or_default();
        let name = match db_path.rfind('/') {
            Some(index) => &db_path[index + 1..],
            None => &db_path,
        };
        self.db_name = db_id + name;
        self.db_path = db_path;
        tracing::trace!("Registered {} (full path: {})", self.db_name, self.db_path);
    }

    // Returns the next free frame number for the replicated log
    fn next_frame(&self) -> u32 {
        self.next_frame.fetch_add(1, Ordering::SeqCst)
    }

    fn last_frame_crc(&self) -> u64 {
        self.last_frame_crc.load(Ordering::Acquire)
    }

    // Returns the current last valid frame in the replicated log
    pub fn peek_last_valid_frame(&self) -> u32 {
        self.next_frame.saturating_sub(1)
    }

    // Sets the last valid frame in the replicated log.
    pub fn register_last_valid_frame(&mut self, frame: u32) {
        if frame != self.peek_last_valid_frame() {
            if self.next_frame.load(Ordering::Acquire) != 1 {
                tracing::error!(
                    "[BUG] Local max valid frame is {}, while replicator thinks it's {}",
                    frame,
                    self.peek_last_valid_frame()
                );
            }
            self.next_frame.store(frame + 1, Ordering::AcqRel)
        }
    }

    // Writes pages to a local in-memory buffer
    pub fn write(&self, pgno: u32, wal_offset: u64, commit_frame: bool) -> Result<u32> {
        let frame_no = self.next_frame();
        tracing::trace!("Writing page {}:{} at frame {}", pgno, wal_offset, frame_no);
        self.write_buffer.send(Frame {
            frame_no,
            pgno,
            wal_offset,
        })?;
        Ok(frame_no)
    }

    // Sends pages participating in current transaction to S3.
    // Returns the frame number holding the last flushed page.
    pub async fn flush(&self, frames: Vec<Frame>) -> Result<()> {
        if frames.is_empty() {
            tracing::trace!("Attempting to flush an empty buffer");
            return Ok(());
        }
        tracing::trace!("Flushing {} frames", frames.len());
        let mut wal_file = tokio::fs::File::open(&format!("{}-wal", &self.db_path)).await?;
        self.commits_in_current_generation
            .fetch_add(1, Ordering::SeqCst);

        let mut last_crc = self.last_frame_crc();
        let first_frame_no = frames[0].frame_no;
        let first_pgno = frames[0].pgno;
        let page_size = self.page_size();
        let mut body = vec![0u8; frames.len() * page_size];
        let mut i = 0;
        for frame in frames {
            wal_file
                .seek(SeekFrom::Start(
                    self.offset_in_wal(frame.frame_no) + WAL_FRAME_HEADER_SIZE,
                ))
                .await?;
            if self.use_compression {
                let mut page = vec![0u8; page_size];
                wal_file.read_exact(&mut page[..]).await?;
                last_crc = Self::crc64(last_crc, &page);
                let mut gzip = GzipEncoder::new(page.as_ref());
                let written =
                    tokio::io::copy(&mut gzip, &mut Cursor::new(&mut body[i..(i + page_size)]))
                        .await?;
                i += written as usize;
            } else {
                let page = &mut body[i..(i + page_size)];
                wal_file.read_exact(page).await?;
                last_crc = Self::crc64(last_crc, page);
                i += page_size;
            }
        }

        let key = format!(
            "{}-{}/{:012}-{:012}-{:016x}",
            self.db_name,
            self.generation(),
            first_frame_no,
            first_pgno,
            last_crc
        );

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(body.into())
            .send()
            .await?;
        Ok(())
    }

    fn crc64(init: u64, data: &[u8]) -> u64 {
        let mut crc = CRC_64.digest_with_initial(init);
        crc.update(data);
        crc.finalize()
    }

    fn offset_in_wal(&self, frame_no: u32) -> u64 {
        WAL_HEADER_SIZE + ((frame_no - 1) as u64) * (WAL_FRAME_HEADER_SIZE + self.page_size())
    }

    pub fn abort(&self, cause: anyhow::Error) {
        tracing::error!("aborting replicator due to: {}", cause);
        todo!()
    }

    // Marks all recently flushed pages as committed and updates the frame number
    // holding the newest consistent committed transaction.
    pub async fn finalize_commit(&mut self, last_frame: u32, checksum: [u32; 2]) -> Result<()> {
        // Last consistent frame is persisted in S3 in order to be able to recover
        // from failured that happen in the middle of a commit, when only some
        // of the pages that belong to a transaction are replicated.
        let last_consistent_frame_key = format!("{}-{}/.consistent", self.db_name, self.generation);
        tracing::trace!("Finalizing frame: {}, checksum: {:?}", last_frame, checksum);
        // Information kept in this entry: [last consistent frame number: 4 bytes][last checksum: 8 bytes]
        let mut consistent_info = BytesMut::with_capacity(12);
        consistent_info.extend_from_slice(&last_frame.to_be_bytes());
        consistent_info.extend_from_slice(&checksum[0].to_be_bytes());
        consistent_info.extend_from_slice(&checksum[1].to_be_bytes());
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(last_consistent_frame_key)
            .body(ByteStream::from(Bytes::from(consistent_info)))
            .send()
            .await?;
        tracing::trace!("Commit successful");
        Ok(())
    }

    // Drops uncommitted frames newer than given last valid frame
    pub fn rollback_to_frame(&mut self, last_valid_frame: u32) {
        // NOTICE: O(size), can be optimized to O(removed) if ever needed
        self.write_buffer.retain(|&k, _| k <= last_valid_frame);
        self.next_frame = last_valid_frame + 1;
        self.last_frame_crc = self
            .write_buffer
            .iter()
            .next_back()
            .map(|entry| entry.1.crc)
            .unwrap_or(self.last_transaction_crc);
        tracing::debug!(
            "Rolled back to {}, crc {} (last transaction crc = {})",
            self.next_frame - 1,
            self.last_frame_crc,
            self.last_transaction_crc,
        );
    }

    // Tries to read the local change counter from the given database file
    async fn read_change_counter(reader: &mut tokio::fs::File) -> Result<[u8; 4]> {
        let mut counter = [0u8; 4];
        reader.seek(std::io::SeekFrom::Start(24)).await?;
        reader.read_exact(&mut counter).await?;
        Ok(counter)
    }

    // Tries to read the local page size from the given database file
    async fn read_page_size(reader: &mut tokio::fs::File) -> Result<usize> {
        reader.seek(std::io::SeekFrom::Start(16)).await?;
        let page_size = reader.read_u16().await?;
        if page_size == 1 {
            Ok(65536)
        } else {
            Ok(page_size as usize)
        }
    }

    // Returns the compressed database file path and its change counter, extracted
    // from the header of page1 at offset 24..27 (as per SQLite documentation).
    pub async fn compress_main_db_file(&self) -> Result<(&'static str, [u8; 4])> {
        use tokio::io::AsyncWriteExt;
        let compressed_db = "db.gz";
        let mut reader = tokio::fs::File::open(&self.db_path).await?;
        let mut writer = async_compression::tokio::write::GzipEncoder::new(
            tokio::fs::File::create(compressed_db).await?,
        );
        tokio::io::copy(&mut reader, &mut writer).await?;
        writer.shutdown().await?;
        let change_counter = Self::read_change_counter(&mut reader).await?;
        Ok((compressed_db, change_counter))
    }

    // Replicates local WAL pages to S3, if local WAL is present.
    // This function is called under the assumption that if local WAL
    // file is present, it was already detected to be newer than its
    // remote counterpart.
    pub async fn maybe_replicate_wal(&mut self) -> Result<()> {
        let mut wal_file = match tokio::fs::File::open(&format!("{}-wal", &self.db_path)).await {
            Ok(file) => file,
            Err(_) => {
                tracing::info!("Local WAL not present - not replicating");
                return Ok(());
            }
        };
        let len = match wal_file.metadata().await {
            Ok(metadata) => metadata.len(),
            Err(_) => 0,
        };
        if len < WAL_HEADER_SIZE {
            tracing::info!("Local WAL is empty, not replicating");
            return Ok(());
        }
        let page_size = self.page_size();
        if page_size == Self::UNSET_PAGE_SIZE {
            tracing::trace!("Page size not detected yet, not replicated");
            return Ok(());
        }

        tracing::trace!(
            "Local WAL pages: {}",
            (len - WAL_HEADER_SIZE) / page_size as u64
        );
        wal_file.seek(tokio::io::SeekFrom::Start(24)).await?;
        let checksum: [u32; 2] = [wal_file.read_u32().await?, wal_file.read_u32().await?];
        tracing::trace!("Local WAL checksum: {:?}", checksum);
        let mut last_written_frame = 0;
        for offset in (WAL_HEADER_SIZE..len).step_by(page_size + WAL_FRAME_HEADER_SIZE as usize) {
            wal_file.seek(SeekFrom::Start(offset)).await?;
            let pgno = wal_file.read_u32().await?;
            let size_after = wal_file.read_u32().await?;
            tracing::trace!("Size after transaction for {}: {}", pgno, size_after);
            // In multi-page transactions, only the last page in the transaction contains
            // the size_after_transaction field. If it's zero, it means it's an uncommited
            // page.
            let commit_frame = size_after != 0;
            self.write(pgno, offset + WAL_FRAME_HEADER_SIZE, commit_frame);
        }
        if last_written_frame > 0 {
            self.finalize_commit(last_written_frame, checksum).await?;
        }
        if !self.write_buffer.is_empty() {
            tracing::warn!("Uncommited WAL entries: {}", self.write_buffer.len());
        }
        self.write_buffer.clear();
        tracing::info!("Local WAL replicated");
        Ok(())
    }

    // Check if the local database file exists and contains data
    async fn main_db_exists_and_not_empty(&self) -> bool {
        let file = match tokio::fs::File::open(&self.db_path).await {
            Ok(file) => file,
            Err(_) => return false,
        };
        match file.metadata().await {
            Ok(metadata) => metadata.len() > 0,
            Err(_) => false,
        }
    }

    // Sends the main database file to S3 - if -wal file is present, it's replicated
    // too - it means that the local file was detected to be newer than its remote
    // counterpart.
    pub async fn snapshot_main_db_file(&mut self) -> Result<()> {
        if !self.main_db_exists_and_not_empty().await {
            tracing::debug!("Not snapshotting, the main db file does not exist or is empty");
            return Ok(());
        }
        tracing::debug!("Snapshotting {}", self.db_path);

        let change_counter = if self.use_compression {
            // TODO: find a way to compress ByteStream on the fly instead of creating
            // an intermediary file.
            let (compressed_db_path, change_counter) = self.compress_main_db_file().await?;
            let key = format!("{}-{}/db.gz", self.db_name, self.generation);
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .body(ByteStream::from_path(compressed_db_path).await?)
                .send()
                .await?;
            change_counter
        } else {
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(format!("{}-{}/db.db", self.db_name, self.generation))
                .body(ByteStream::from_path(&self.db_path).await?)
                .send()
                .await?;
            let mut reader = tokio::fs::File::open(&self.db_path).await?;
            Self::read_change_counter(&mut reader).await?
        };

        /* FIXME: we can't rely on the change counter in WAL mode:
         ** "In WAL mode, changes to the database are detected using the wal-index and
         ** so the change counter is not needed. Hence, the change counter might not be
         ** incremented on each transaction in WAL mode."
         ** Instead, we need to consult WAL checksums.
         */
        let change_counter_key = format!("{}-{}/.changecounter", self.db_name, self.generation);
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(change_counter_key)
            .body(ByteStream::from(Bytes::copy_from_slice(&change_counter)))
            .send()
            .await?;
        tracing::debug!("Main db snapshot complete");
        Ok(())
    }

    // Returns newest replicated generation, or None, if one is not found.
    // FIXME: assumes that this bucket stores *only* generations for databases,
    // it should be more robust and continue looking if the first item does not
    // match the <db-name>-<generation-uuid>/ pattern.
    pub async fn find_newest_generation(&self) -> Option<uuid::Uuid> {
        let prefix = format!("{}-", self.db_name);
        let response = self
            .list_objects()
            .prefix(prefix)
            .max_keys(1)
            .send()
            .await
            .ok()?;
        let objs = response.contents()?;
        let key = objs.first()?.key()?;
        let key = match key.find('/') {
            Some(index) => &key[self.db_name.len() + 1..index],
            None => key,
        };
        tracing::debug!("Generation candidate: {}", key);
        uuid::Uuid::parse_str(key).ok()
    }

    // Tries to fetch the remote database change counter from given generation
    pub async fn get_remote_change_counter(&self, generation: &uuid::Uuid) -> Result<[u8; 4]> {
        use bytes::Buf;
        let mut remote_change_counter = [0u8; 4];
        if let Ok(response) = self
            .get_object(format!("{}-{}/.changecounter", self.db_name, generation))
            .send()
            .await
        {
            response
                .body
                .collect()
                .await?
                .copy_to_slice(&mut remote_change_counter)
        }
        Ok(remote_change_counter)
    }

    // Tries to fetch the last consistent frame number stored in the remote generation
    pub async fn get_last_consistent_frame(&self, generation: &uuid::Uuid) -> Result<(u32, u64)> {
        use bytes::Buf;
        Ok(
            match self
                .get_object(format!("{}-{}/.consistent", self.db_name, generation))
                .send()
                .await
                .ok()
            {
                Some(response) => {
                    let mut collected = response.body.collect().await?;
                    (collected.get_u32(), collected.get_u64())
                }
                None => (0, 0),
            },
        )
    }

    // Returns the number of pages stored in the local WAL file, or 0, if there aren't any.
    async fn get_local_wal_page_count(&mut self) -> u32 {
        match tokio::fs::File::open(&format!("{}-wal", &self.db_path)).await {
            Ok(mut file) => {
                let metadata = match file.metadata().await {
                    Ok(metadata) => metadata,
                    Err(_) => return 0,
                };
                let len = metadata.len();
                if len >= 32 {
                    // Page size is stored in WAL file at offset [8-12)
                    if file.seek(tokio::io::SeekFrom::Start(8)).await.is_err() {
                        return 0;
                    };
                    let page_size = match file.read_u32().await {
                        Ok(size) => size,
                        Err(_) => return 0,
                    };
                    if self.set_page_size(page_size as usize).is_err() {
                        return 0;
                    }
                    // Each WAL file consists of a 32-byte WAL header and N entries of size (page size + 24)
                    (len / (self.page_size + 24) as u64) as u32
                } else {
                    0
                }
            }
            Err(_) => 0,
        }
    }

    // Parses the frame and page number from given key.
    // Format: <db-name>-<generation>/<frame-number>-<page-number>-<crc64>
    fn parse_frame_page_crc(key: &str) -> Option<(u32, i32, u64)> {
        let checksum_delim = key.rfind('-')?;
        let page_delim = key[0..checksum_delim].rfind('-')?;
        let frame_delim = key[0..page_delim].rfind('/')?;
        let frameno = key[frame_delim + 1..page_delim].parse::<u32>().ok()?;
        let pgno = key[page_delim + 1..checksum_delim].parse::<i32>().ok()?;
        let crc = u64::from_str_radix(&key[checksum_delim + 1..], 16).ok()?;
        tracing::debug!(frameno, pgno, crc);
        Some((frameno, pgno, crc))
    }

    async fn restore_frame(
        &mut self,
        pgno: i32,
        crc: u64,
        prev_crc: u64,
        page_buffer: &mut Vec<u8>,
        main_db_writer: &mut (impl tokio::io::AsyncWriteExt
                  + tokio::io::AsyncSeekExt
                  + std::marker::Unpin),
        reader: &mut (impl tokio::io::AsyncRead + std::marker::Unpin),
    ) -> Result<()> {
        // If page size is unknown *or* crc verification is performed,
        // a page needs to be loaded to memory first
        if self.verify_crc || self.page_size == Self::UNSET_PAGE_SIZE {
            let page_size = tokio::io::copy(reader, page_buffer).await?;
            if self.verify_crc {
                let mut expected_crc = CRC_64.digest_with_initial(prev_crc);
                expected_crc.update(page_buffer);
                let expected_crc = expected_crc.finalize();
                tracing::debug!(crc, expected_crc);
                if crc != expected_crc {
                    tracing::warn!(
                        "CRC check failed: {:016x} != {:016x} (expected)",
                        crc,
                        expected_crc
                    );
                }
            };
            self.set_page_size(page_size as usize)?;
            let offset = (pgno - 1) as u64 * page_size;
            main_db_writer
                .seek(tokio::io::SeekFrom::Start(offset))
                .await?;
            tokio::io::copy(&mut &page_buffer[..], main_db_writer).await?;
            page_buffer.clear();
        } else {
            let offset = (pgno - 1) as u64 * self.page_size as u64;
            main_db_writer
                .seek(tokio::io::SeekFrom::Start(offset))
                .await?;
            // FIXME: we only need to overwrite with the newest page,
            // no need to replay the whole WAL
            tokio::io::copy(reader, main_db_writer).await?;
        }
        main_db_writer.flush().await?;
        Ok(())
    }

    // Restores the database state from given remote generation
    pub async fn restore_from(&mut self, generation: uuid::Uuid) -> Result<RestoreAction> {
        use tokio::io::AsyncWriteExt;

        // Check if the database needs to be restored by inspecting the database
        // change counter and the WAL size.
        let local_counter = match tokio::fs::File::open(&self.db_path).await {
            Ok(mut db) => {
                // While reading the main database file for the first time,
                // page size from an existing database should be set.
                if let Ok(page_size) = Self::read_page_size(&mut db).await {
                    self.set_page_size(page_size)?;
                }
                Self::read_change_counter(&mut db).await.unwrap_or([0u8; 4])
            }
            Err(_) => [0u8; 4],
        };

        let remote_counter = self.get_remote_change_counter(&generation).await?;
        tracing::debug!("Counters: l={:?}, r={:?}", local_counter, remote_counter);

        let (last_consistent_frame, checksum) = self.get_last_consistent_frame(&generation).await?;
        tracing::debug!(
            "Last consistent remote frame: {}; checksum: {:x}",
            last_consistent_frame,
            checksum
        );

        let wal_pages = self.get_local_wal_page_count().await;
        match local_counter.cmp(&remote_counter) {
            Ordering::Equal => {
                tracing::debug!(
                    "Consistent: {}; wal pages: {}",
                    last_consistent_frame,
                    wal_pages
                );
                match wal_pages.cmp(&last_consistent_frame) {
                    Ordering::Equal => {
                        tracing::info!(
                            "Remote generation is up-to-date, reusing it in this session"
                        );
                        self.next_frame = wal_pages + 1;
                        return Ok(RestoreAction::ReuseGeneration(generation));
                    }
                    Ordering::Greater => {
                        tracing::info!("Local change counter matches the remote one, but local WAL contains newer data, which needs to be replicated");
                        return Ok(RestoreAction::SnapshotMainDbFile);
                    }
                    Ordering::Less => (),
                }
            }
            Ordering::Greater => {
                tracing::info!("Local change counter is larger than its remote counterpart - a new snapshot needs to be replicated");
                return Ok(RestoreAction::SnapshotMainDbFile);
            }
            Ordering::Less => (),
        }

        tokio::fs::rename(&self.db_path, format!("{}.bottomless.backup", self.db_path))
            .await
            .ok(); // Best effort
        let mut main_db_writer = tokio::fs::File::create(&self.db_path).await?;
        // If the db file is not present, the database could have been empty

        let main_db_path = if self.use_compression {
            format!("{}-{}/db.gz", self.db_name, generation)
        } else {
            format!("{}-{}/db.db", self.db_name, generation)
        };

        if let Ok(db_file) = self.get_object(main_db_path).send().await {
            let mut body_reader = db_file.body.into_async_read();
            if self.use_compression {
                let mut decompress_reader = async_compression::tokio::bufread::GzipDecoder::new(
                    tokio::io::BufReader::new(body_reader),
                );
                tokio::io::copy(&mut decompress_reader, &mut main_db_writer).await?;
            } else {
                tokio::io::copy(&mut body_reader, &mut main_db_writer).await?;
            }
            main_db_writer.flush().await?;
        }
        tracing::info!("Restored the main database file");

        let mut next_marker = None;
        let prefix = format!("{}-{}/", self.db_name, generation);
        tracing::debug!("Overwriting any existing WAL file: {}-wal", &self.db_path);
        tokio::fs::remove_file(&format!("{}-wal", &self.db_path))
            .await
            .ok();
        tokio::fs::remove_file(&format!("{}-shm", &self.db_path))
            .await
            .ok();

        let mut applied_wal_frame = false;
        loop {
            let mut list_request = self.list_objects().prefix(&prefix);
            if let Some(marker) = next_marker {
                list_request = list_request.marker(marker);
            }
            let response = list_request.send().await?;
            let objs = match response.contents() {
                Some(objs) => objs,
                None => {
                    tracing::debug!("No objects found in generation {}", generation);
                    break;
                }
            };
            let mut prev_crc = 0;
            let mut page_buffer = Vec::with_capacity(65536); // best guess for the page size - it will certainly not be more than 64KiB
            for obj in objs {
                let key = obj
                    .key()
                    .ok_or_else(|| anyhow::anyhow!("Failed to get key for an object"))?;
                tracing::debug!("Loading {}", key);
                let frame = self.get_object(key.into()).send().await?;

                let (frameno, pgno, crc) = match Self::parse_frame_page_crc(key) {
                    Some(result) => result,
                    None => {
                        if !key.ends_with(".gz")
                            && !key.ends_with(".db")
                            && !key.ends_with(".consistent")
                            && !key.ends_with(".changecounter")
                        {
                            tracing::warn!("Failed to parse frame/page from key {}", key);
                        }
                        continue;
                    }
                };
                if frameno > last_consistent_frame {
                    tracing::warn!("Remote log contains frame {} larger than last consistent frame ({}), stopping the restoration process",
                                frameno, last_consistent_frame);
                    break;
                }
                let mut body_reader = frame.body.into_async_read();
                if self.use_compression {
                    let mut compressed_reader = async_compression::tokio::bufread::GzipDecoder::new(
                        tokio::io::BufReader::new(body_reader),
                    );
                    self.restore_frame(
                        pgno,
                        crc,
                        prev_crc,
                        &mut page_buffer,
                        &mut main_db_writer,
                        &mut compressed_reader,
                    )
                    .await?;
                } else {
                    self.restore_frame(
                        pgno,
                        crc,
                        prev_crc,
                        &mut page_buffer,
                        &mut main_db_writer,
                        &mut body_reader,
                    )
                    .await?;
                };
                tracing::debug!("Written frame {} as main db page {}", frameno, pgno);

                prev_crc = crc;
                applied_wal_frame = true;
            }
            next_marker = response
                .is_truncated()
                .then(|| objs.last().map(|elem| elem.key().unwrap().to_string()))
                .flatten();
            if next_marker.is_none() {
                break;
            }
        }

        if applied_wal_frame {
            Ok::<_, anyhow::Error>(RestoreAction::SnapshotMainDbFile)
        } else {
            Ok::<_, anyhow::Error>(RestoreAction::None)
        }
    }

    // Restores the database state from newest remote generation
    pub async fn restore(&mut self) -> Result<RestoreAction> {
        let newest_generation = match self.find_newest_generation().await {
            Some(gen) => gen,
            None => {
                tracing::debug!("No generation found, nothing to restore");
                return Ok(RestoreAction::SnapshotMainDbFile);
            }
        };

        tracing::info!("Restoring from generation {}", newest_generation);
        self.restore_from(newest_generation).await
    }
}

pub struct Context {
    pub replicator: Replicator,
    pub runtime: tokio::runtime::Runtime,
}

#[derive(Debug)]
pub struct FetchedResults {
    pub pages: Vec<(i32, Bytes)>,
    pub next_marker: Option<String>,
}

#[derive(Debug)]
pub enum RestoreAction {
    None,
    SnapshotMainDbFile,
    ReuseGeneration(uuid::Uuid),
}

#[derive(Clone, Debug)]
pub struct Options {
    pub create_bucket_if_not_exists: bool,
    pub verify_crc: bool,
    pub use_compression: bool,
    pub aws_endpoint: Option<String>,
    pub bucket_name: String,
    pub frame_batch_interval: Duration,
    pub frame_batch_max_size: usize,
}

impl Default for Options {
    fn default() -> Self {
        let aws_endpoint = std::env::var("LIBSQL_BOTTOMLESS_ENDPOINT").ok();
        let bucket_name =
            std::env::var("LIBSQL_BOTTOMLESS_BUCKET").unwrap_or_else(|_| "bottomless".to_string());
        Options {
            create_bucket_if_not_exists: false,
            verify_crc: true,
            use_compression: false,
            frame_batch_interval: Duration::from_secs(15),
            frame_batch_max_size: 128 * 1024, // AWS S3 put_object payload cost are counted every 256KiB
            aws_endpoint,
            bucket_name,
        }
    }
}
