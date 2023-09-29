use anyhow::{bail, Result};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::types::ObjectAttributes;
use aws_sdk_s3::Client;
use aws_smithy_types::date_time::Format;
use bottomless::s3::GenerationSummary;
use bottomless::uuid_utils::GenerationUuid;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

pub(crate) struct Replicator {
    inner: bottomless::replicator::Replicator,
}

impl std::ops::Deref for Replicator {
    type Target = bottomless::replicator::Replicator;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::DerefMut for Replicator {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

pub(crate) async fn detect_db(client: &Client, bucket: &str, namespace: &str) -> Option<String> {
    let namespace = namespace.to_owned() + ":";
    let response = client
        .list_objects()
        .bucket(bucket)
        .set_delimiter(Some("/".to_string()))
        .prefix(namespace.clone())
        .send()
        .await
        .ok()?;

    let prefix = response.common_prefixes()?.first()?.prefix()?;
    // 38 is the length of the uuid part
    if let Some('-') = prefix.chars().nth(prefix.len().saturating_sub(38)) {
        let ns_db = &prefix[..prefix.len().saturating_sub(38)];
        Some(ns_db.strip_prefix(&namespace).unwrap_or(ns_db).to_owned())
    } else {
        None
    }
}

impl Replicator {
    pub async fn new(db: String) -> Result<Self> {
        let inner = bottomless::replicator::Replicator::new(db).await?;
        Ok(Replicator { inner })
    }

    pub(crate) async fn print_snapshot_summary(&self, generation: &uuid::Uuid) -> Result<()> {
        match self
            .client
            .get_object_attributes()
            .bucket(&self.bucket)
            .key(format!("{}-{}/db.gz", self.db_name, generation))
            .object_attributes(ObjectAttributes::ObjectSize)
            .send()
            .await
        {
            Ok(attrs) => {
                println!("\tmain database snapshot:");
                println!("\t\tobject size:   {}", attrs.object_size());
                println!(
                    "\t\tlast modified: {}",
                    attrs
                        .last_modified()
                        .map(|s| s.fmt(Format::DateTime).unwrap_or_else(|e| e.to_string()))
                        .as_deref()
                        .unwrap_or("never")
                );
            }
            Err(SdkError::ServiceError(err)) if err.err().is_no_such_key() => {
                println!("\tno main database snapshot file found")
            }
            Err(e) => println!("\tfailed to fetch main database snapshot info: {e}"),
        };
        Ok(())
    }

    pub(crate) async fn list_generations(
        &self,
        limit: Option<u64>,
        older_than: Option<chrono::NaiveDate>,
        newer_than: Option<chrono::NaiveDate>,
        verbose: bool,
    ) -> Result<()> {
        let mut limit = limit.unwrap_or(u64::MAX);
        let mut generations = self.client.list_generations();
        let mut found = false;
        while let Some(generation) = generations.next().await? {
            let created_at = if let Some(d) = generation.date_time() {
                d
            } else {
                bail!("failed to retrieve timestamp from UUID {}", uuid)
            };
            found = true;
            if created_at.date() < newer_than.unwrap_or(chrono::NaiveDate::MIN) {
                continue;
            }
            if created_at.date() > older_than.unwrap_or(chrono::NaiveDate::MAX) {
                continue;
            }
            if verbose {
                let summary = self.client.generation_summary(generation).await?;
                self.print_summary(summary);
                self.print_snapshot_summary(&generation).await?;
                println!()
            } else {
                println!(
                    "{} (created: {})",
                    generation,
                    created_at.and_utc().to_rfc3339()
                );
            }

            limit -= 1;
            if limit == 0 {
                break;
            }
        }
        if !found {
            println!("No generations found");
        }
        Ok(())
    }

    pub(crate) async fn remove_many(&self, older_than: NaiveDate, verbose: bool) -> Result<()> {
        let older_than = NaiveDateTime::new(older_than, NaiveTime::MIN);
        let delete_all = self.inner.delete_all(Some(older_than)).await?;
        if verbose {
            println!("Tombstoned {} at {}", self.inner.db_path, older_than);
        }
        let removed_generations = delete_all.commit().await?;
        if verbose {
            println!(
                "Removed {} generations of {} up to {}",
                removed_generations, self.inner.db_path, older_than
            );
        }
        Ok(())
    }

    pub(crate) async fn remove(&self, generation: uuid::Uuid, verbose: bool) -> Result<()> {
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
                    if verbose {
                        println!("No objects found")
                    }
                    return Ok(());
                }
            };

            for obj in objs {
                if let Some(key) = obj.key() {
                    if verbose {
                        println!("Removing {key}")
                    }
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
                if verbose {
                    println!("Removed {removed} snapshot generations");
                }
                return Ok(());
            }
        }
    }

    pub(crate) async fn list_generation(&self, generation: uuid::Uuid) -> Result<()> {
        let summary = self.client.generation_summary(generation).await?;
        self.print_summary(summary);
        self.print_snapshot_summary(&generation).await?;
        Ok(())
    }

    fn print_summary(&self, summary: GenerationSummary) {
        println!("Generation {} for {}", summary.generation, self.db_name);
        println!("\tchange counter:       {:?}", summary.change_counter);
        println!("\tWAL frame checksum:   {:x}", summary.init_crc);
        if let Some(created_at) = summary.generation.date_time() {
            println!("\tcreated at:           {}", created_at);
        }
        if let Some(last_frame) = summary.last_wal_segment {
            println!(
                "\tconsistent WAL frame: {} (timestamp: {})",
                last_frame.last_frame_no, last_frame.timestamp
            );
        }
        if let Some(page_size) = summary.page_size {
            println!("\tpage size:            {}", page_size);
        }

        if let Some(prev_gen) = summary.parent {
            println!("\tprevious generation:  {}", prev_gen);
        }
    }
}
