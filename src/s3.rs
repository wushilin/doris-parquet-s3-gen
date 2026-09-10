//! S3 output configuration.
//!
//! Loaded from a TOML file given with `--s3-config`. Unknown keys are rejected
//! so a typo fails at startup rather than silently using a default. Every
//! value is validated against the limits S3 actually enforces.

use std::path::Path;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde::Deserialize;

/// S3 requires every part except the last to be at least this large.
const MIN_PART_SIZE: u64 = 5 << 20;
/// S3 caps a single part at 5 GiB.
const MAX_PART_SIZE: u64 = 5 << 30;
/// S3 allows at most 10,000 parts per multipart upload.
const MAX_PARTS_PER_OBJECT: u64 = 10_000;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputConfig {
    pub s3: S3Section,
    #[serde(default)]
    pub upload: UploadSection,
    #[serde(default)]
    pub parquet: ParquetSection,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3Section {
    pub bucket: String,
    /// Key prefix for generated objects. A trailing slash is added if missing.
    #[serde(default)]
    pub prefix: String,
    #[serde(default)]
    pub region: Option<String>,
    /// Set for MinIO, Ceph, or any S3-compatible endpoint.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Path-style addressing. Required by most S3-compatible servers.
    #[serde(default)]
    pub path_style: bool,
    /// Permit a plain-HTTP endpoint. Off by default.
    #[serde(default)]
    pub allow_http: bool,
    #[serde(default)]
    pub credentials: Option<Credentials>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    #[serde(default)]
    pub session_token: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadSection {
    /// Multipart part size. Buffered in memory before each part is sent.
    #[serde(default = "default_part_size", deserialize_with = "de_byte_size")]
    pub part_size: u64,
    /// Parts uploaded concurrently per file.
    #[serde(default = "default_concurrency")]
    pub max_concurrent_parts: usize,
    #[serde(default = "default_retries")]
    pub retries: u32,
    #[serde(default = "default_timeout", deserialize_with = "de_duration")]
    pub timeout: Duration,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParquetSection {
    /// zstd, snappy, gzip, lz4, or none.
    #[serde(default = "default_compression")]
    pub compression: String,
    /// Rows per row group. A file can only roll on a row group boundary.
    #[serde(default = "default_row_group_rows")]
    pub row_group_rows: usize,
    /// Dictionary encoding for repeated string values.
    #[serde(default = "default_true")]
    pub dictionary: bool,
}

fn default_part_size() -> u64 {
    10 << 20
}
fn default_concurrency() -> usize {
    8
}
fn default_retries() -> u32 {
    3
}
fn default_timeout() -> Duration {
    Duration::from_secs(60)
}
fn default_compression() -> String {
    "zstd".to_string()
}
fn default_row_group_rows() -> usize {
    200_000
}
fn default_true() -> bool {
    true
}

impl Default for UploadSection {
    fn default() -> Self {
        Self {
            part_size: default_part_size(),
            max_concurrent_parts: default_concurrency(),
            retries: default_retries(),
            timeout: default_timeout(),
        }
    }
}

impl Default for ParquetSection {
    fn default() -> Self {
        Self {
            compression: default_compression(),
            row_group_rows: default_row_group_rows(),
            dictionary: default_true(),
        }
    }
}

fn de_byte_size<'de, D>(deserializer: D) -> std::result::Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error as _;
    let raw = SizeOrInt::deserialize(deserializer)?;
    match raw {
        SizeOrInt::Int(value) => Ok(value),
        SizeOrInt::Text(text) => crate::parse_byte_size(&text).map_err(D::Error::custom),
    }
}

fn de_duration<'de, D>(deserializer: D) -> std::result::Result<Duration, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error as _;
    let raw = SizeOrInt::deserialize(deserializer)?;
    match raw {
        SizeOrInt::Int(value) => Ok(Duration::from_secs(value)),
        SizeOrInt::Text(text) => crate::parse_duration(&text).map_err(D::Error::custom),
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum SizeOrInt {
    Int(u64),
    Text(String),
}

pub fn load(path: &Path) -> Result<OutputConfig> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read S3 config {}", path.display()))?;
    let config: OutputConfig = toml::from_str(&text)
        .with_context(|| format!("failed to parse S3 config {}", path.display()))?;
    config
        .validate()
        .with_context(|| format!("invalid S3 config {}", path.display()))?;
    Ok(config)
}

impl OutputConfig {
    pub fn validate(&self) -> Result<()> {
        if self.s3.bucket.trim().is_empty() {
            bail!("s3.bucket must not be empty");
        }
        if self.s3.endpoint.is_none() && self.s3.region.is_none() {
            bail!("set s3.region, or s3.endpoint for an S3-compatible server");
        }
        if let Some(endpoint) = &self.s3.endpoint {
            if endpoint.starts_with("http://") && !self.s3.allow_http {
                bail!(
                    "s3.endpoint `{}` is plain HTTP; set s3.allow_http = true to permit it",
                    endpoint
                );
            }
            if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
                bail!("s3.endpoint `{}` must start with http:// or https://", endpoint);
            }
        }
        if self.upload.part_size < MIN_PART_SIZE {
            bail!(
                "upload.part_size {} is below the S3 minimum of 5MiB",
                self.upload.part_size
            );
        }
        if self.upload.part_size > MAX_PART_SIZE {
            bail!(
                "upload.part_size {} exceeds the S3 maximum of 5GiB",
                self.upload.part_size
            );
        }
        if self.upload.max_concurrent_parts == 0 {
            bail!("upload.max_concurrent_parts must be at least 1");
        }
        if self.parquet.row_group_rows == 0 {
            bail!("parquet.row_group_rows must be at least 1");
        }
        const CODECS: [&str; 6] = ["zstd", "snappy", "gzip", "lz4", "lz4_raw", "none"];
        let codec = self.parquet.compression.to_ascii_lowercase();
        if !CODECS.contains(&codec.as_str()) {
            bail!(
                "parquet.compression `{}` is not one of: {}",
                self.parquet.compression,
                CODECS.join(", ")
            );
        }
        Ok(())
    }

    /// Largest file this config can produce, given the S3 part-count ceiling.
    pub fn max_file_bytes(&self) -> u64 {
        self.upload.part_size.saturating_mul(MAX_PARTS_PER_OBJECT)
    }

    /// Reject a per-file cap that would need more than 10,000 parts.
    pub fn check_file_cap(&self, cap: u64) -> Result<()> {
        let limit = self.max_file_bytes();
        if cap > limit {
            bail!(
                "a file cap of {} bytes needs more than {} parts at upload.part_size {}; \
                 raise upload.part_size or lower the cap",
                cap,
                MAX_PARTS_PER_OBJECT,
                self.upload.part_size
            );
        }
        Ok(())
    }

    /// Peak upload buffering per writer thread.
    pub fn buffer_bytes_per_thread(&self) -> u64 {
        self.upload
            .part_size
            .saturating_mul(self.upload.max_concurrent_parts as u64)
    }

    /// Object key prefix, normalised to end with a slash when non-empty.
    pub fn normalised_prefix(&self) -> String {
        let trimmed = self.s3.prefix.trim().trim_start_matches('/');
        if trimmed.is_empty() {
            String::new()
        } else if trimmed.ends_with('/') {
            trimmed.to_string()
        } else {
            format!("{}/", trimmed)
        }
    }
}

/// A commented starter config, written by `--emit-s3-config`.
pub const SAMPLE_TOML: &str = r#"# S3 target for generated Parquet files.
# Written by --emit-s3-config. Pass it back with --s3-config.
#
# Unknown keys are rejected, so a typo fails at startup rather than being
# silently ignored.

[s3]
bucket = "my-bucket"

# Key prefix for generated objects. Files land at <prefix>part-000001.parquet.
prefix = "doris/u347ug_data/"

# Region for real AWS S3.
region = "ap-southeast-1"

# For MinIO, Ceph, or any S3-compatible server, set endpoint instead.
# Most of them need path-style addressing.
# endpoint = "https://minio.internal:9000"
# path_style = true

# Plain-HTTP endpoints are refused unless this is set.
# allow_http = false

# Credentials are optional. Omit this whole section to use the standard
# AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY environment variables, an
# instance profile, or any other default AWS credential source.
# Keep this file at mode 600 if you do put keys in it.
# [s3.credentials]
# access_key_id = "AKIA..."
# secret_access_key = "..."
# session_token = ""

[upload]
# Multipart part size. Each part is buffered in memory before it is sent.
# S3 requires at least 5MiB and allows at most 10000 parts per object,
# so this also caps the largest file: 10MiB x 10000 is about 97GiB.
part_size = "10MiB"

# Parts in flight per file. Peak memory per writer thread is
# part_size x max_concurrent_parts, so 10MiB x 8 = 80MiB here.
max_concurrent_parts = 8

retries = 3
timeout = "60s"

[parquet]
# zstd, snappy, gzip, lz4, lz4_raw, or none. Doris reads all of these.
compression = "zstd"

# Rows per row group. Each upload worker buffers a whole row group in memory
# before encoding it, so this multiplied by the worker count drives peak
# memory. It also sets how finely file sizes and size targets can be tracked.
row_group_rows = 200000

dictionary = true
"#;

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> Result<OutputConfig> {
        let config: OutputConfig = toml::from_str(text)?;
        config.validate()?;
        Ok(config)
    }

    #[test]
    fn sample_config_is_valid() {
        let config = parse(SAMPLE_TOML).expect("shipped sample must parse and validate");
        assert_eq!(config.s3.bucket, "my-bucket");
        assert_eq!(config.upload.part_size, 10 << 20);
        assert_eq!(config.upload.max_concurrent_parts, 8);
        assert_eq!(config.parquet.compression, "zstd");
        assert_eq!(config.upload.timeout, Duration::from_secs(60));
        assert_eq!(config.buffer_bytes_per_thread(), 80 << 20);
        // 10MiB x 10000 parts, which is about 97GiB rather than a round 100GiB.
        assert_eq!(config.max_file_bytes(), 10_485_760 * 10_000);
    }

    #[test]
    fn minimal_config_fills_in_defaults() {
        let config = parse("[s3]\nbucket = \"b\"\nregion = \"us-east-1\"\n").expect("parse");
        assert_eq!(config.upload.part_size, 10 << 20);
        assert_eq!(config.parquet.row_group_rows, 200_000);
        assert!(config.parquet.dictionary);
        assert_eq!(config.normalised_prefix(), "");
    }

    #[test]
    fn rejects_unknown_keys() {
        let err = parse("[s3]\nbucket = \"b\"\nregion = \"r\"\nbuckett = \"typo\"\n")
            .expect_err("typo must fail");
        assert!(err.to_string().contains("buckett"), "unhelpful error: {}", err);
    }

    #[test]
    fn enforces_s3_part_size_limits() {
        let too_small = parse("[s3]\nbucket=\"b\"\nregion=\"r\"\n[upload]\npart_size=\"1MiB\"\n")
            .expect_err("below 5MiB must fail");
        assert!(too_small.to_string().contains("5MiB"), "{}", too_small);

        let too_big = parse("[s3]\nbucket=\"b\"\nregion=\"r\"\n[upload]\npart_size=\"6GiB\"\n")
            .expect_err("above 5GiB must fail");
        assert!(too_big.to_string().contains("5GiB"), "{}", too_big);

        assert!(parse("[s3]\nbucket=\"b\"\nregion=\"r\"\n[upload]\npart_size=\"5MiB\"\n").is_ok());
    }

    #[test]
    fn file_cap_must_fit_in_ten_thousand_parts() {
        let config = parse("[s3]\nbucket=\"b\"\nregion=\"r\"\n").expect("parse");
        // 2GiB at 10MiB parts is about 205 parts.
        assert!(config.check_file_cap(2 << 30).is_ok());
        let err = config.check_file_cap(200 << 30).expect_err("200GiB needs too many parts");
        assert!(err.to_string().contains("10000"), "{}", err);
    }

    #[test]
    fn guards_plain_http_endpoints() {
        let refused = parse("[s3]\nbucket=\"b\"\nendpoint=\"http://minio:9000\"\n")
            .expect_err("plain http must be opt-in");
        assert!(refused.to_string().contains("allow_http"), "{}", refused);

        assert!(parse(
            "[s3]\nbucket=\"b\"\nendpoint=\"http://minio:9000\"\nallow_http=true\n"
        )
        .is_ok());

        let bad_scheme = parse("[s3]\nbucket=\"b\"\nendpoint=\"minio:9000\"\n")
            .expect_err("scheme is required");
        assert!(bad_scheme.to_string().contains("http"), "{}", bad_scheme);
    }

    #[test]
    fn requires_a_region_or_an_endpoint() {
        let err = parse("[s3]\nbucket=\"b\"\n").expect_err("no region and no endpoint");
        assert!(err.to_string().contains("region"), "{}", err);
    }

    #[test]
    fn rejects_unknown_compression() {
        let err = parse("[s3]\nbucket=\"b\"\nregion=\"r\"\n[parquet]\ncompression=\"brotli\"\n")
            .expect_err("brotli is not offered");
        assert!(err.to_string().contains("zstd"), "{}", err);
    }

    #[test]
    fn normalises_the_key_prefix() {
        let with_slash = parse("[s3]\nbucket=\"b\"\nregion=\"r\"\nprefix=\"a/b/\"\n").unwrap();
        assert_eq!(with_slash.normalised_prefix(), "a/b/");

        let without = parse("[s3]\nbucket=\"b\"\nregion=\"r\"\nprefix=\"a/b\"\n").unwrap();
        assert_eq!(without.normalised_prefix(), "a/b/");

        let leading = parse("[s3]\nbucket=\"b\"\nregion=\"r\"\nprefix=\"/a/b\"\n").unwrap();
        assert_eq!(leading.normalised_prefix(), "a/b/", "leading slash is dropped");
    }

    #[test]
    fn accepts_credentials_when_given() {
        let config = parse(
            "[s3]\nbucket=\"b\"\nregion=\"r\"\n[s3.credentials]\naccess_key_id=\"A\"\nsecret_access_key=\"S\"\n",
        )
        .expect("parse");
        let credentials = config.s3.credentials.expect("credentials present");
        assert_eq!(credentials.access_key_id, "A");
        assert_eq!(credentials.session_token, None);
    }
}
