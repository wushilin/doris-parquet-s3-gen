//! Streaming Parquet output to S3 or a local directory.
//!
//! Nothing is staged on disk and no whole file is held in memory. Each writer
//! encodes one row group at a time and hands the bytes to an `object_store`
//! `BufWriter`, which uploads them as multipart parts as soon as each part
//! fills. Closing the writer completes the upload, so the object appears
//! atomically and Doris never sees a partial file.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use object_store::buffered::BufWriter;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::s3::OutputConfig;

/// Counters shared by every writer, sampled by the status display.
#[derive(Debug, Default)]
pub struct Stats {
    /// Rows produced by the generator threads.
    pub rows_generated: AtomicU64,
    /// Rows handed to a Parquet writer. Trails `rows_generated` by the queue.
    pub rows: AtomicU64,
    /// Rows whose bytes are actually known, i.e. encoded into a row group.
    /// Rows sitting in a half-built row group are counted by `rows` but have
    /// contributed no bytes yet, so only this figure pairs with the byte
    /// counters when deriving bytes-per-row.
    pub rows_flushed: AtomicU64,
    /// Bytes in files that have been closed and fully uploaded.
    pub bytes_written: AtomicU64,
    /// Bytes written so far into files still open, across all writers.
    pub active_bytes: AtomicU64,
    pub files_completed: AtomicU64,
    pub buffered_bytes: AtomicU64,
}

impl Stats {
    pub fn add_rows(&self, count: u64) {
        self.rows.fetch_add(count, Ordering::Relaxed);
    }

    /// Everything produced so far, closed files plus files in progress.
    pub fn total_bytes(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed) + self.active_bytes.load(Ordering::Relaxed)
    }
}

/// Where generated files land.
pub enum Destination {
    S3 { config: Box<OutputConfig> },
    Local { directory: std::path::PathBuf },
}

pub struct SinkSettings {
    pub schema: SchemaRef,
    pub row_group_rows: usize,
    pub part_size: usize,
    /// Multipart parts uploaded concurrently per file.
    pub max_concurrent_parts: usize,
    pub file_cap: Option<u64>,
    pub compression: Compression,
    pub dictionary: bool,
}

impl SinkSettings {
    #[cfg(test)]
    fn clone_for_test(&self) -> Self {
        Self {
            schema: self.schema.clone(),
            row_group_rows: self.row_group_rows,
            part_size: self.part_size,
            max_concurrent_parts: self.max_concurrent_parts,
            file_cap: self.file_cap,
            compression: self.compression,
            dictionary: self.dictionary,
        }
    }
}

pub fn parse_compression(name: &str) -> Result<Compression> {
    let codec = match name.to_ascii_lowercase().as_str() {
        "zstd" => Compression::ZSTD(ZstdLevel::default()),
        "snappy" => Compression::SNAPPY,
        "gzip" => Compression::GZIP(Default::default()),
        "lz4" => Compression::LZ4,
        "lz4_raw" => Compression::LZ4_RAW,
        "none" | "uncompressed" => Compression::UNCOMPRESSED,
        other => anyhow::bail!("unknown parquet compression `{}`", other),
    };
    Ok(codec)
}

/// Build the object store and the key prefix every file is written under.
pub fn build_store(destination: &Destination) -> Result<(Arc<dyn ObjectStore>, String)> {
    match destination {
        Destination::S3 { config } => {
            // from_env picks up AWS_ACCESS_KEY_ID and friends; explicit
            // credentials in the config file override it.
            let mut builder = AmazonS3Builder::from_env().with_bucket_name(&config.s3.bucket);
            if let Some(region) = &config.s3.region {
                builder = builder.with_region(region);
            }
            if let Some(endpoint) = &config.s3.endpoint {
                builder = builder.with_endpoint(endpoint);
            }
            if config.s3.allow_http {
                builder = builder.with_allow_http(true);
            }
            if config.s3.path_style {
                builder = builder.with_virtual_hosted_style_request(false);
            }
            if let Some(credentials) = &config.s3.credentials {
                builder = builder
                    .with_access_key_id(&credentials.access_key_id)
                    .with_secret_access_key(&credentials.secret_access_key);
                if let Some(token) = &credentials.session_token {
                    if !token.is_empty() {
                        builder = builder.with_token(token);
                    }
                }
            }
            let store = builder
                .build()
                .context("failed to construct the S3 client; check bucket, region and credentials")?;
            Ok((Arc::new(store), config.normalised_prefix()))
        }
        Destination::Local { directory } => {
            std::fs::create_dir_all(directory).with_context(|| {
                format!("failed to create output directory {}", directory.display())
            })?;
            let store = LocalFileSystem::new_with_prefix(directory).with_context(|| {
                format!("failed to open output directory {}", directory.display())
            })?;
            Ok((Arc::new(store), String::new()))
        }
    }
}

/// Move a shared gauge by the difference, tracking what this writer reported.
fn apply_delta(gauge: &AtomicU64, reported: &mut u64, current: u64) {
    if current >= *reported {
        gauge.fetch_add(current - *reported, Ordering::Relaxed);
    } else {
        gauge.fetch_sub(*reported - current, Ordering::Relaxed);
    }
    *reported = current;
}

struct ActiveFile {
    writer: AsyncArrowWriter<BufWriter>,
    path: String,
}

/// One writer, owned by a single producer task. Files roll at the size cap.
pub struct ParquetSink {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    writer_id: usize,
    file_index: u64,
    settings: Arc<SinkSettings>,
    stats: Arc<Stats>,
    active: Option<ActiveFile>,
    /// Bytes this writer has contributed to `stats.buffered_bytes`.
    reported_buffer: u64,
    /// Bytes this writer has contributed to `stats.active_bytes`.
    reported_active: u64,
    /// Rows this writer has contributed to `stats.rows_flushed`.
    reported_flushed: u64,
    /// Every row this writer has been handed, across all its files.
    rows_seen: u64,
}

impl ParquetSink {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        prefix: String,
        writer_id: usize,
        settings: Arc<SinkSettings>,
        stats: Arc<Stats>,
    ) -> Self {
        Self {
            store,
            prefix,
            writer_id,
            file_index: 0,
            settings,
            stats,
            active: None,
            reported_buffer: 0,
            reported_active: 0,
            reported_flushed: 0,
            rows_seen: 0,
        }
    }

    fn next_path(&mut self) -> String {
        self.file_index += 1;
        format!(
            "{}part-w{:02}-{:06}.parquet",
            self.prefix, self.writer_id, self.file_index
        )
    }

    fn writer_properties(&self) -> WriterProperties {
        WriterProperties::builder()
            .set_compression(self.settings.compression)
            .set_dictionary_enabled(self.settings.dictionary)
            .set_max_row_group_row_count(Some(self.settings.row_group_rows))
            .build()
    }

    async fn open(&mut self) -> Result<()> {
        let path = self.next_path();
        let object_path = ObjectPath::parse(&path)
            .with_context(|| format!("invalid object key `{}`", path))?;
        // Without with_max_concurrency the writer silently uses the default,
        // so the configured value would be ignored.
        let buffered = BufWriter::with_capacity(
            self.store.clone(),
            object_path,
            self.settings.part_size,
        )
        .with_max_concurrency(self.settings.max_concurrent_parts);
        let writer =
            AsyncArrowWriter::try_new(buffered, self.settings.schema.clone(), Some(self.writer_properties()))
                .with_context(|| format!("failed to start Parquet file `{}`", path))?;
        self.active = Some(ActiveFile { writer, path });
        Ok(())
    }

    /// Write one record batch, rolling to a new file when the cap is reached.
    pub async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        if self.active.is_none() {
            self.open().await?;
        }
        let rows = batch.num_rows() as u64;
        {
            let active = self.active.as_mut().expect("writer is open");
            active
                .writer
                .write(&batch)
                .await
                .with_context(|| format!("failed writing to `{}`", active.path))?;
        }
        self.rows_seen += rows;
        self.stats.add_rows(rows);
        self.sync_buffer_gauge();

        if let Some(cap) = self.settings.file_cap {
            let written = self.reported_active;
            // Files can only roll on a row group boundary, so the final size
            // lands between the cap and the cap plus one row group.
            if written >= cap {
                self.close_active().await?;
            }
        }
        Ok(())
    }

    /// Publish this writer's in-flight numbers into the shared gauges.
    /// Both are delta-updated so the totals stay correct across writers.
    fn sync_buffer_gauge(&mut self) {
        let (buffered, file_bytes, pending_rows) = match self.active.as_ref() {
            Some(active) => (
                active.writer.in_progress_size() as u64,
                active.writer.bytes_written() as u64,
                active.writer.in_progress_rows() as u64,
            ),
            // With no file open, everything this writer holds has been flushed.
            None => (0, 0, 0),
        };
        apply_delta(&self.stats.buffered_bytes, &mut self.reported_buffer, buffered);
        apply_delta(&self.stats.active_bytes, &mut self.reported_active, file_bytes);
        apply_delta(
            &self.stats.rows_flushed,
            &mut self.reported_flushed,
            self.rows_seen.saturating_sub(pending_rows),
        );
    }

    async fn close_active(&mut self) -> Result<()> {
        let Some(active) = self.active.take() else {
            return Ok(());
        };
        let path = active.path.clone();
        // close() writes the footer and completes the multipart upload.
        let metadata = active
            .writer
            .close()
            .await
            .with_context(|| format!("failed to finish Parquet file `{}`", path))?;
        let bytes: u64 = metadata
            .row_groups()
            .iter()
            .map(|group| group.compressed_size().max(0) as u64)
            .sum();
        self.stats.bytes_written.fetch_add(bytes, Ordering::Relaxed);
        self.stats.files_completed.fetch_add(1, Ordering::Relaxed);
        // Both byte gauges describe an open file, so they reset when it closes.
        apply_delta(&self.stats.buffered_bytes, &mut self.reported_buffer, 0);
        apply_delta(&self.stats.active_bytes, &mut self.reported_active, 0);
        // Closing flushes everything this writer was still holding.
        apply_delta(
            &self.stats.rows_flushed,
            &mut self.reported_flushed,
            self.rows_seen,
        );
        Ok(())
    }

    /// Finish the file in flight. Must be called or the object never appears.
    pub async fn finish(mut self) -> Result<()> {
        self.close_active().await
    }

    pub fn current_file(&self) -> Option<(String, u64)> {
        self.active
            .as_ref()
            .map(|active| (active.path.clone(), active.writer.bytes_written() as u64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parquet_out::BatchBuilder;
    use crate::schema::parse_schema;
    use crate::Value;

    fn settings(schema: SchemaRef, cap: Option<u64>) -> Arc<SinkSettings> {
        Arc::new(SinkSettings {
            schema,
            row_group_rows: 500,
            part_size: 5 << 20,
            max_concurrent_parts: 4,
            file_cap: cap,
            compression: Compression::UNCOMPRESSED,
            dictionary: true,
        })
    }

    #[test]
    fn maps_compression_names() {
        assert!(matches!(parse_compression("zstd").unwrap(), Compression::ZSTD(_)));
        assert!(matches!(parse_compression("SNAPPY").unwrap(), Compression::SNAPPY));
        assert!(matches!(
            parse_compression("none").unwrap(),
            Compression::UNCOMPRESSED
        ));
        assert!(parse_compression("brotli").is_err());
    }

    #[tokio::test]
    async fn writes_a_readable_parquet_file_locally() {
        let dir = std::env::temp_dir().join(format!("dpsg-sink-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let destination = Destination::Local { directory: dir.clone() };
        let (store, prefix) = build_store(&destination).expect("store");

        let tables = parse_schema(
            "CREATE TABLE t (id BIGINT NOT NULL, amount DECIMAL(19,4), ts DATETIME(6), note VARCHAR(64)) ENGINE=OLAP",
        )
        .unwrap();
        let columns = tables[0].columns.clone();
        let mut builder = BatchBuilder::new(columns, 256).unwrap();
        let stats = Arc::new(Stats::default());
        let mut sink = ParquetSink::new(
            store,
            prefix,
            0,
            settings(builder.schema(), None),
            stats.clone(),
        );

        for index in 0..1000i64 {
            let row: Vec<(String, Value)> = vec![
                ("id".into(), Value::I64(index)),
                ("amount".into(), Value::String(format!("{}.25", index % 90 + 1))),
                ("ts".into(), Value::String("2026-09-10 12:00:00.500000".into())),
                ("note".into(), Value::String(format!("row {}", index))),
            ];
            builder
                .append_row(|name| row.iter().find(|(key, _)| key == name).map(|(_, v)| v))
                .unwrap();
        }
        let batch = builder.finish().unwrap();
        sink.write(batch).await.expect("write batch");
        sink.finish().await.expect("finish");

        assert_eq!(stats.rows.load(Ordering::Relaxed), 1000);
        assert_eq!(stats.files_completed.load(Ordering::Relaxed), 1);
        assert_eq!(
            stats.buffered_bytes.load(Ordering::Relaxed),
            0,
            "buffer gauge must return to zero"
        );

        // Read the file back to prove it is valid Parquet with the right rows.
        let written: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "parquet"))
            .collect();
        assert_eq!(written.len(), 1, "expected exactly one parquet file");

        let file = std::fs::File::open(written[0].path()).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let total: usize = reader.map(|batch| batch.unwrap().num_rows()).sum();
        assert_eq!(total, 1000, "row count must survive the round trip");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn rolls_to_a_new_file_at_the_size_cap() {
        let dir = std::env::temp_dir().join(format!("dpsg-roll-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let destination = Destination::Local { directory: dir.clone() };
        let (store, prefix) = build_store(&destination).expect("store");

        let tables =
            parse_schema("CREATE TABLE t (id BIGINT NOT NULL, note VARCHAR(64)) ENGINE=OLAP").unwrap();
        let mut builder = BatchBuilder::new(tables[0].columns.clone(), 512).unwrap();
        let stats = Arc::new(Stats::default());
        // A tiny cap forces a roll after almost every batch.
        let mut sink = ParquetSink::new(
            store,
            prefix,
            3,
            settings(builder.schema(), Some(4096)),
            stats.clone(),
        );

        for round in 0..5 {
            for index in 0..500i64 {
                let row: Vec<(String, Value)> = vec![
                    ("id".into(), Value::I64(round * 500 + index)),
                    ("note".into(), Value::String(format!("value number {}", index))),
                ];
                builder
                    .append_row(|name| row.iter().find(|(key, _)| key == name).map(|(_, v)| v))
                    .unwrap();
            }
            let batch = builder.finish().unwrap();
            sink.write(batch).await.expect("write");
        }
        sink.finish().await.expect("finish");

        let mut names: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .filter(|name| name.ends_with(".parquet"))
            .collect();
        names.sort();
        assert!(names.len() > 1, "cap should have produced several files: {:?}", names);
        assert_eq!(
            stats.files_completed.load(Ordering::Relaxed) as usize,
            names.len()
        );
        // The writer id appears in the name so parallel writers never collide.
        assert!(names.iter().all(|name| name.starts_with("part-w03-")), "{:?}", names);
        assert_eq!(stats.rows.load(Ordering::Relaxed), 2500);

        std::fs::remove_dir_all(&dir).ok();
    }

    /// `row_group_rows` is a hard maximum, applied before writing: a batch that
    /// would overflow the current row group is split so the group lands exactly
    /// on the limit. The file size cap is the opposite, applied after a batch is
    /// written, so a file overshoots by up to one row group.
    #[tokio::test]
    async fn row_groups_are_split_to_the_exact_limit() {
        let dir = std::env::temp_dir().join(format!("dpsg-rg-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (store, prefix) = build_store(&Destination::Local { directory: dir.clone() }).unwrap();

        let tables =
            parse_schema("CREATE TABLE t (id BIGINT NOT NULL) ENGINE=OLAP").unwrap();
        let mut builder = BatchBuilder::new(tables[0].columns.clone(), 1000).unwrap();
        let mut settings = (*settings(builder.schema(), None)).clone_for_test();
        // 1500 does not divide the 1000-row batches, so a split must happen.
        settings.row_group_rows = 1500;
        let stats = Arc::new(Stats::default());
        let mut sink =
            ParquetSink::new(store, prefix, 0, Arc::new(settings), stats.clone());

        // Six batches of 1000 rows: 6000 rows total, so four full row groups.
        for round in 0..6i64 {
            for index in 0..1000i64 {
                let row = vec![("id".to_string(), Value::I64(round * 1000 + index))];
                builder
                    .append_row(|name| row.iter().find(|(key, _)| key == name).map(|(_, v)| v))
                    .unwrap();
            }
            let batch = builder.finish().unwrap();
            sink.write(batch).await.unwrap();
        }
        sink.finish().await.unwrap();

        let file = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .find(|entry| entry.path().extension().is_some_and(|ext| ext == "parquet"))
            .expect("one file");
        let handle = std::fs::File::open(file.path()).unwrap();
        let reader =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(handle).unwrap();
        let meta = reader.metadata();
        let counts: Vec<i64> = (0..meta.num_row_groups())
            .map(|index| meta.row_group(index).num_rows())
            .collect();
        assert_eq!(
            counts,
            vec![1500, 1500, 1500, 1500],
            "row groups must be split to exactly the limit, not overshoot"
        );

        std::fs::remove_dir_all(&dir).ok();
    }
}
