mod derive;
#[allow(dead_code)]
mod parquet_out;
// Fields become live when the S3 sink lands.
#[allow(dead_code)]
mod s3;
mod schema;
#[allow(dead_code)]
mod sink;
#[allow(dead_code)]
mod status;

use std::{
    collections::{HashMap, HashSet},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, Context, Result};
use base64::{engine::general_purpose, Engine as _};
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use clap::{ArgGroup, Parser};
use handlebars::{
    Context as HbContext, Handlebars, Helper, HelperResult, JsonRender, Output, RenderContext,
    RenderError, RenderErrorReason,
};
use rand::{distributions::Alphanumeric, prelude::*, rngs::StdRng, SeedableRng};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::mpsc;

#[derive(Parser, Debug)]
#[command(version, about = "Generate data from a Doris schema and stream it to S3 as Parquet")]
#[command(group(
    ArgGroup::new("rate")
        .args(["rows_per_second", "bytes_per_second"])
        .multiple(false)
))]
struct Args {
    #[arg(long)]
    spec: Option<PathBuf>,

    #[arg(long)]
    init: bool,

    /// Generation threads. Defaults to the machine's CPU count.
    #[arg(long)]
    threads: Option<usize>,

    /// Upload workers draining the batch queue.
    #[arg(long, default_value_t = 8)]
    upload_threads: usize,

    /// Batches that may wait between generation and upload. Deeper rides out
    /// longer upload stalls without starving the generators, at roughly
    /// batch_rows x bytes-per-row of memory per slot.
    #[arg(long, default_value_t = 100)]
    queue_depth: usize,

    /// Rows per batch handed to the queue.
    #[arg(long, default_value_t = 32_768)]
    batch_rows: usize,

    #[arg(long)]
    rows: Option<u64>,

    #[arg(long, value_parser = parse_duration)]
    time: Option<Duration>,

    #[arg(long)]
    rows_per_second: Option<u64>,

    #[arg(long, value_parser = parse_byte_size)]
    bytes_per_second: Option<u64>,

    #[arg(long)]
    no_header: bool,

    /// Write output to FILE instead of stdout
    #[arg(long)]
    output: Option<PathBuf>,

    /// When --output is set, start a new file after each SIZE bytes (e.g. 100MB).
    /// Each new file receives the CSV header. Splits on whole-row boundaries.
    #[arg(long, value_parser = parse_byte_size, requires = "output")]
    split_bytes: Option<u64>,

    /// Use a built-in schema preset instead of --spec.
    /// Available: user, order, product, event
    #[arg(long, conflicts_with = "spec")]
    preset: Option<String>,

    /// Doris CREATE TABLE file. Supplies Parquet column types and order.
    /// Combine with --spec to keep the schema's types and your own generators.
    #[arg(long, conflicts_with = "preset", value_name = "FILE")]
    schema: Option<PathBuf>,

    /// Table to use when --schema declares more than one.
    #[arg(long, requires = "schema", value_name = "NAME")]
    table: Option<String>,

    /// Pre-flight: write the field spec derived from --schema, then exit.
    /// Edit the result and pass it back with --spec. Use `-` for stdout.
    #[arg(long, requires = "schema", value_name = "FILE")]
    emit_spec: Option<PathBuf>,

    /// S3 target and upload settings, as TOML. See --emit-s3-config.
    #[arg(long, value_name = "FILE")]
    s3_config: Option<PathBuf>,

    /// Pre-flight: write a commented starter S3 config, then exit.
    /// Use `-` for stdout.
    #[arg(long, value_name = "FILE")]
    emit_s3_config: Option<PathBuf>,

    /// Stop once this much Parquet has been produced, e.g. 10GiB.
    #[arg(long, value_parser = parse_byte_size, conflicts_with = "rows", value_name = "SIZE")]
    target_size: Option<u64>,

    /// Roll to a new Parquet object after this many bytes.
    #[arg(long, value_parser = parse_byte_size, default_value = "2GiB", value_name = "SIZE")]
    file_size: u64,

    /// Write Parquet to this local directory instead of S3. For testing.
    #[arg(long, conflicts_with = "s3_config", value_name = "DIR")]
    out_dir: Option<PathBuf>,

    /// zstd level for --out-dir runs. Range 1-22; higher trades CPU for size.
    #[arg(long, value_name = "N")]
    compression_level: Option<i32>,

    /// Suppress the live status display.
    #[arg(long)]
    no_progress: bool,
}

#[derive(Debug, Deserialize)]
struct RawSpec {
    version: u32,
    #[serde(default)]
    csv: CsvSpec,
    #[serde(default)]
    context: ContextSpec,
    #[serde(default)]
    batch: BatchSpec,
    fields: Vec<FieldSpec>,
}

#[derive(Debug, Deserialize)]
struct CsvSpec {
    #[serde(default = "default_delimiter")]
    delimiter: char,
    #[serde(default = "default_quote")]
    quote: char,
    #[serde(default = "default_escape")]
    escape: char,
    #[serde(default = "default_newline")]
    newline: String,
    #[serde(default)]
    null: String,
}

impl Default for CsvSpec {
    fn default() -> Self {
        Self {
            delimiter: default_delimiter(),
            quote: default_quote(),
            escape: default_escape(),
            newline: default_newline(),
            null: String::new(),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
struct ContextSpec {
    #[serde(default = "default_context_reset")]
    reset: ContextReset,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ContextReset {
    #[default]
    Row,
    Batch,
    Never,
}

#[derive(Debug, Deserialize)]
struct BatchSpec {
    #[serde(default = "default_batch_rows")]
    rows: usize,
}

impl Default for BatchSpec {
    fn default() -> Self {
        Self {
            rows: default_batch_rows(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct FieldSpec {
    name: String,
    #[serde(default)]
    hidden: bool,
    #[serde(default)]
    order: i64,
    gen: GeneratorSpec,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum GeneratorSpec {
    Constant(ConstantSpec),
    Sequence(SequenceSpec),
    SequenceString(SequenceStringSpec),
    Name(NameSpec),
    Email(EmailSpec),
    Lorem(LoremSpec),
    Address(AddressSpec),
    Template(TemplateSpec),
    IntRange(IntRangeSpec),
    FloatRange(FloatRangeSpec),
    DecimalRange(DecimalRangeSpec),
    Fluctuating(FluctuatingSpec),
    #[serde(rename = "datetime_around")]
    DateTimeAround(DateTimeAroundSpec),
    #[serde(rename = "datetime_range")]
    DateTimeRange(DateTimeRangeSpec),
    Choice(ChoiceSpec),
    WeightedChoice(WeightedChoiceSpec),
    Uuid(UuidSpec),
    RandomBytes(RandomBytesSpec),
    #[serde(rename = "javascript")]
    JavaScript(JavaScriptSpec),
}

#[derive(Debug, Clone, Deserialize)]
struct ConstantSpec {
    value: Value,
    #[serde(default)]
    null_rate: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
struct SequenceSpec {
    #[serde(default = "default_sequence_start")]
    start: i64,
    #[serde(default = "default_sequence_step")]
    step: i64,
    #[serde(default)]
    null_rate: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
struct SequenceStringSpec {
    /// Literal text with one `{}` where the counter goes.
    #[serde(default = "default_sequence_string_template")]
    template: String,
    #[serde(default = "default_sequence_string_start")]
    start: u64,
    #[serde(default = "default_sequence_string_step")]
    step: u64,
    /// Total width of the rendered value. The counter is zero-padded to fill
    /// whatever the literal text leaves over.
    #[serde(default = "default_sequence_string_width")]
    width: usize,
    #[serde(default)]
    null_rate: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
struct NameSpec {
    #[serde(default = "default_name_part")]
    part: NamePart,
    #[serde(default)]
    null_rate: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
enum NamePart {
    First,
    Last,
    Full,
}

#[derive(Debug, Clone, Deserialize)]
struct EmailSpec {
    #[serde(default)]
    style: Option<String>,
    #[serde(default)]
    null_rate: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
struct LoremSpec {
    #[serde(default = "default_words_min")]
    words_min: usize,
    #[serde(default = "default_words_max")]
    words_max: usize,
    #[serde(default)]
    null_rate: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
struct AddressSpec {
    #[serde(default = "default_address_part")]
    part: AddressPart,
    #[serde(default)]
    null_rate: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
enum AddressPart {
    Street,
    City,
    State,
    Country,
    PostalCode,
    Full,
}

#[derive(Debug, Clone, Deserialize)]
struct TemplateSpec {
    value: String,
    #[serde(default)]
    null_rate: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
struct IntRangeSpec {
    min: i64,
    max: i64,
    #[serde(default)]
    null_rate: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
struct FloatRangeSpec {
    min: f64,
    max: f64,
    #[serde(default)]
    precision: Option<usize>,
    #[serde(default)]
    null_rate: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
struct DecimalRangeSpec {
    min: Decimal,
    max: Decimal,
    scale: u32,
    #[serde(default)]
    null_rate: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
struct FluctuatingSpec {
    data_type: NumericDataType,
    start: Decimal,
    min: Decimal,
    max: Decimal,
    #[serde(default = "default_initial_direction")]
    initial_direction: InitialDirection,
    step_min: Decimal,
    step_max: Decimal,
    #[serde(default)]
    flip_chance: f64,
    #[serde(default)]
    precision: Option<usize>,
    #[serde(default)]
    scale: Option<u32>,
    #[serde(default)]
    null_rate: Option<f64>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum NumericDataType {
    Int,
    Float,
    Double,
    Decimal,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum InitialDirection {
    Up,
    Down,
    Random,
}

#[derive(Debug, Clone, Deserialize)]
struct DateTimeAroundSpec {
    #[serde(default)]
    base: Option<String>,
    #[serde(default)]
    offset_seconds_min: i64,
    #[serde(default)]
    offset_seconds_max: i64,
    #[serde(default = "default_datetime_format")]
    format: String,
    #[serde(default)]
    null_rate: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
struct DateTimeRangeSpec {
    start: String,
    end: String,
    #[serde(default = "default_datetime_format")]
    format: String,
    #[serde(default)]
    null_rate: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
struct ChoiceSpec {
    values: Vec<Value>,
    #[serde(default)]
    null_rate: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
struct WeightedChoiceSpec {
    values: Vec<WeightedValue>,
    #[serde(default)]
    null_rate: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
struct WeightedValue {
    value: Value,
    weight: f64,
}

#[derive(Debug, Clone, Deserialize)]
struct UuidSpec {
    #[serde(default)]
    null_rate: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
struct RandomBytesSpec {
    min_bytes: usize,
    max_bytes: usize,
    encoding: RandomBytesEncoding,
    #[serde(default)]
    null_rate: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
struct JavaScriptSpec {
    function: String,
    file: String,
    #[serde(default)]
    deps: Vec<String>,
    #[serde(default)]
    null_rate: Option<f64>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RandomBytesEncoding {
    Hex,
    Base64,
    Base64url,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum Value {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    String(String),
    /// Microseconds since the Unix epoch, carrying the format the text paths
    /// render it with. Keeping the number means the Parquet writer takes it
    /// as-is; formatting a timestamp only to parse it straight back was the
    /// single largest item in the generator profile.
    ///
    /// `skip` because `Value` is an untagged `Deserialize` used for literals
    /// in a spec file: these two are produced by generators, never parsed.
    #[serde(skip)]
    Timestamp { micros: i64, format: Arc<str> },
    /// Fixed point: `units` scaled by ten to the minus `scale`.
    #[serde(skip)]
    Decimal { units: i128, scale: u32 },
}

impl Value {
    fn csv_string(&self, null: &str) -> String {
        match self {
            Value::Null => null.to_string(),
            Value::Bool(value) => value.to_string(),
            Value::I64(value) => value.to_string(),
            Value::F64(value) => value.to_string(),
            Value::String(value) => value.clone(),
            Value::Timestamp { micros, format } => format_timestamp_micros(*micros, format),
            Value::Decimal { units, scale } => format_decimal_units(*units, *scale),
        }
    }

    fn template_value(&self) -> serde_json::Value {
        match self {
            Value::Null => serde_json::Value::Null,
            Value::Bool(value) => serde_json::Value::Bool(*value),
            Value::I64(value) => serde_json::Value::Number((*value).into()),
            Value::F64(value) => serde_json::Number::from_f64(*value)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Value::String(value) => serde_json::Value::String(value.clone()),
            Value::Timestamp { micros, format } => {
                serde_json::Value::String(format_timestamp_micros(*micros, format))
            }
            Value::Decimal { units, scale } => {
                serde_json::Value::String(format_decimal_units(*units, *scale))
            }
        }
    }
}

type RowContext = HashMap<String, Value>;
type BatchMessage = std::result::Result<Vec<Vec<u8>>, String>;

#[derive(Clone)]
struct CompiledSpec {
    csv: CsvSpecRuntime,
    context_reset: ContextReset,
    batch_rows: usize,
    fields: Arc<Vec<CompiledField>>,
    generation_order: Arc<Vec<usize>>,
    output_order: Arc<Vec<usize>>,
    has_stateful_ordered: bool,
}

impl CompiledSpec {
    /// Check every fixed-width counter against the highest index this run can
    /// reach. Generator threads claim counter values in blocks and abandon
    /// whatever they have not used, so allow one unfinished block each.
    fn check_sequence_capacity(&self, rows: Option<u64>, threads: usize) -> Result<()> {
        // Size- and time-bounded runs have no row count to check against; the
        // per-row guard still stops them, it just cannot warn up front.
        let Some(rows) = rows else {
            return Ok(());
        };
        // A thread reaches its highest index only after every other thread has
        // claimed a block ahead of it, so the waste is bounded by the blocks
        // those `threads - 1` others hold, not by one block each.
        let slack = (threads.saturating_sub(1) as u64).saturating_mul(SEQUENCE_STRING_BLOCK);
        let highest_index = rows.saturating_sub(1).saturating_add(slack);
        for field in self.fields.iter() {
            field
                .generator
                .check_capacity(highest_index)
                .with_context(|| format!("field `{}`", field.name))?;
        }
        Ok(())
    }
}

#[derive(Clone)]
struct CsvSpecRuntime {
    delimiter: u8,
    quote: u8,
    escape: u8,
    newline: Vec<u8>,
    null: String,
}

#[derive(Clone)]
struct CompiledField {
    name: String,
    hidden: bool,
    order: i64,
    definition_index: usize,
    generator: Generator,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum GeneratorKind {
    Stateless,
    StatefulOrdered,
}

#[derive(Clone)]
enum Generator {
    Constant(ConstantGenerator),
    Sequence(SequenceGenerator),
    SequenceString(SequenceStringGenerator),
    Name(NameGenerator),
    Email(EmailGenerator),
    Lorem(LoremGenerator),
    Address(AddressGenerator),
    Template(TemplateGenerator),
    IntRange(IntRangeGenerator),
    FloatRange(FloatRangeGenerator),
    DecimalRange(DecimalRangeGenerator),
    Fluctuating(FluctuatingGenerator),
    DateTimeAround(DateTimeAroundGenerator),
    DateTimeRange(DateTimeRangeGenerator),
    Choice(ChoiceGenerator),
    WeightedChoice(WeightedChoiceGenerator),
    Uuid(UuidGenerator),
    RandomBytes(RandomBytesGenerator),
    JavaScript(JavaScriptGenerator),
}

impl Generator {
    /// Refuse a run that would outgrow a fixed-width counter partway through,
    /// rather than letting it discover the ceiling in hour forty.
    fn check_capacity(&self, highest_index: u64) -> Result<()> {
        let Generator::SequenceString(generator) = self else {
            return Ok(());
        };
        let highest = generator
            .start
            .saturating_add(highest_index.saturating_mul(generator.step));
        if highest > generator.max_counter {
            bail!(
                "this run reaches sequence_string counter {}, past the {} that {} digits \
                 hold. Widen `width` or shorten the template's literal text.",
                highest,
                generator.max_counter,
                generator.digits
            );
        }
        Ok(())
    }

    fn kind(&self) -> GeneratorKind {
        match self {
            // `sequence_string` is deliberately absent: it carries state, but
            // its counter is shared and claimed in blocks, so producers stay
            // independent. Listing it here would clamp generation to one
            // thread and cost far more than the UUIDs it replaces.
            Generator::Sequence(_) | Generator::Fluctuating(_) | Generator::JavaScript(_) => {
                GeneratorKind::StatefulOrdered
            }
            _ => GeneratorKind::Stateless,
        }
    }

    fn dependencies(&self) -> &[String] {
        match self {
            Generator::Template(generator) => &generator.dependencies,
            Generator::JavaScript(generator) => &generator.deps,
            _ => &[],
        }
    }

    fn generate(&mut self, ctx: &RowContext, rng: &mut StdRng) -> Result<Value> {
        match self {
            Generator::Constant(generator) => generator.generate(ctx, rng),
            Generator::Sequence(generator) => generator.generate(ctx, rng),
            Generator::SequenceString(generator) => generator.generate(ctx, rng),
            Generator::Name(generator) => generator.generate(ctx, rng),
            Generator::Email(generator) => generator.generate(ctx, rng),
            Generator::Lorem(generator) => generator.generate(ctx, rng),
            Generator::Address(generator) => generator.generate(ctx, rng),
            Generator::Template(generator) => generator.generate(ctx, rng),
            Generator::IntRange(generator) => generator.generate(ctx, rng),
            Generator::FloatRange(generator) => generator.generate(ctx, rng),
            Generator::DecimalRange(generator) => generator.generate(ctx, rng),
            Generator::Fluctuating(generator) => generator.generate(ctx, rng),
            Generator::DateTimeAround(generator) => generator.generate(ctx, rng),
            Generator::DateTimeRange(generator) => generator.generate(ctx, rng),
            Generator::Choice(generator) => generator.generate(ctx, rng),
            Generator::WeightedChoice(generator) => generator.generate(ctx, rng),
            Generator::Uuid(generator) => generator.generate(ctx, rng),
            Generator::RandomBytes(generator) => generator.generate(ctx, rng),
            Generator::JavaScript(generator) => generator.generate(ctx, rng),
        }
    }
}

trait FieldGenerator {
    fn generate(&mut self, ctx: &RowContext, rng: &mut StdRng) -> Result<Value>;
}

#[derive(Clone)]
struct ConstantGenerator {
    value: Value,
    null_rate: Option<f64>,
}

impl FieldGenerator for ConstantGenerator {
    fn generate(&mut self, _ctx: &RowContext, rng: &mut StdRng) -> Result<Value> {
        if should_emit_null(self.null_rate, rng)? {
            return Ok(Value::Null);
        }
        Ok(self.value.clone())
    }
}

#[derive(Clone)]
struct SequenceGenerator {
    next: i64,
    step: i64,
    null_rate: Option<f64>,
}

impl FieldGenerator for SequenceGenerator {
    fn generate(&mut self, _ctx: &RowContext, rng: &mut StdRng) -> Result<Value> {
        if should_emit_null(self.null_rate, rng)? {
            return Ok(Value::Null);
        }
        let value = self.next;
        self.next = self.next.saturating_add(self.step);
        Ok(Value::I64(value))
    }
}

/// How many distinct counters `digits` decimal characters hold, saturating at
/// `u64::MAX`, which is the counter's own ceiling anyway.
fn sequence_string_capacity(digits: usize) -> u64 {
    let mut capacity: u64 = 1;
    for _ in 0..digits {
        match capacity.checked_mul(10) {
            Some(next) => capacity = next,
            None => return u64::MAX,
        }
    }
    capacity
}

/// How many counter values a generator claims from the shared cursor at once.
/// Claiming in blocks keeps the atomic off the per-row path, so the cost per
/// row is a compare and an add.
const SEQUENCE_STRING_BLOCK: u64 = 1 << 16;

/// A counter rendered into a fixed-width string, as a cheap stand-in for
/// `uuid` when a column only needs to be unique and the right size. Producing
/// a v4 UUID costs a draw from the OS entropy pool per value; this costs an
/// increment and a format.
///
/// Every clone shares one cursor and claims its own block of the counter, so
/// values stay unique across generator threads. That is what a key column
/// needs, and it is why this is not simply `sequence` with a format applied.
struct SequenceStringGenerator {
    /// Literal text before and after the counter, split from the template.
    prefix: String,
    suffix: String,
    /// Zero-padding width for the counter, so the whole value hits `width`.
    digits: usize,
    /// Largest counter that still fits `digits`. Rust's zero-padding widens
    /// rather than truncates, so without this the values would quietly grow a
    /// character partway through a long run instead of failing.
    max_counter: u64,
    start: u64,
    step: u64,
    /// Next unclaimed index, shared by every clone of this generator.
    cursor: Arc<AtomicU64>,
    /// The half-open block of indices this instance still owns.
    next_index: u64,
    block_end: u64,
    null_rate: Option<f64>,
}

/// Cloning hands the copy an empty block so it claims indices of its own
/// rather than replaying the ones this instance is partway through.
impl Clone for SequenceStringGenerator {
    fn clone(&self) -> Self {
        Self {
            prefix: self.prefix.clone(),
            suffix: self.suffix.clone(),
            digits: self.digits,
            max_counter: self.max_counter,
            start: self.start,
            step: self.step,
            cursor: self.cursor.clone(),
            next_index: 0,
            block_end: 0,
            null_rate: self.null_rate,
        }
    }
}

impl SequenceStringGenerator {
    fn claim_block(&mut self) {
        let first = self
            .cursor
            .fetch_add(SEQUENCE_STRING_BLOCK, Ordering::Relaxed);
        self.next_index = first;
        self.block_end = first.saturating_add(SEQUENCE_STRING_BLOCK);
    }
}

impl FieldGenerator for SequenceStringGenerator {
    fn generate(&mut self, _ctx: &RowContext, rng: &mut StdRng) -> Result<Value> {
        if should_emit_null(self.null_rate, rng)? {
            return Ok(Value::Null);
        }
        if self.next_index >= self.block_end {
            self.claim_block();
        }
        let index = self.next_index;
        self.next_index += 1;
        let counter = self.start.saturating_add(index.saturating_mul(self.step));
        if counter > self.max_counter {
            bail!(
                "sequence_string counter {} no longer fits {} digits; widen `width` \
                 or shorten the template's literal text",
                counter,
                self.digits
            );
        }
        Ok(Value::String(format!(
            "{}{:0width$}{}",
            self.prefix,
            counter,
            self.suffix,
            width = self.digits
        )))
    }
}

#[derive(Clone)]
struct NameGenerator {
    part: NamePart,
    null_rate: Option<f64>,
}

impl FieldGenerator for NameGenerator {
    fn generate(&mut self, _ctx: &RowContext, rng: &mut StdRng) -> Result<Value> {
        if should_emit_null(self.null_rate, rng)? {
            return Ok(Value::Null);
        }
        let first = pick(FIRST_NAMES, rng);
        let last = pick(LAST_NAMES, rng);
        let value = match self.part {
            NamePart::First => first.to_string(),
            NamePart::Last => last.to_string(),
            NamePart::Full => format!("{first} {last}"),
        };
        Ok(Value::String(value))
    }
}

#[derive(Clone)]
struct EmailGenerator {
    _style: Option<String>,
    null_rate: Option<f64>,
}

impl FieldGenerator for EmailGenerator {
    fn generate(&mut self, _ctx: &RowContext, rng: &mut StdRng) -> Result<Value> {
        if should_emit_null(self.null_rate, rng)? {
            return Ok(Value::Null);
        }
        let local: String = (&mut *rng)
            .sample_iter(&Alphanumeric)
            .take(12)
            .map(char::from)
            .collect::<String>()
            .to_ascii_lowercase();
        Ok(Value::String(format!(
            "{local}@{}",
            pick(EMAIL_DOMAINS, rng)
        )))
    }
}

#[derive(Clone)]
struct LoremGenerator {
    words_min: usize,
    words_max: usize,
    null_rate: Option<f64>,
}

impl FieldGenerator for LoremGenerator {
    fn generate(&mut self, _ctx: &RowContext, rng: &mut StdRng) -> Result<Value> {
        if should_emit_null(self.null_rate, rng)? {
            return Ok(Value::Null);
        }
        ensure_range(self.words_min, self.words_max, "lorem words")?;
        let count = rng.gen_range(self.words_min..=self.words_max);
        let mut words = Vec::with_capacity(count);
        for _ in 0..count {
            words.push(pick(LOREM_WORDS, rng));
        }
        Ok(Value::String(words.join(" ")))
    }
}

#[derive(Clone)]
struct AddressGenerator {
    part: AddressPart,
    null_rate: Option<f64>,
}

impl FieldGenerator for AddressGenerator {
    fn generate(&mut self, _ctx: &RowContext, rng: &mut StdRng) -> Result<Value> {
        if should_emit_null(self.null_rate, rng)? {
            return Ok(Value::Null);
        }
        let street = format!(
            "{} {} {}",
            rng.gen_range(100..=9999),
            pick(STREET_NAMES, rng),
            pick(STREET_SUFFIXES, rng)
        );
        let city = pick(CITIES, rng);
        let state = pick(STATES, rng);
        let country = pick(COUNTRIES, rng);
        let postal_code = format!("{:05}", rng.gen_range(10000..=99999));
        let value = match self.part {
            AddressPart::Street => street,
            AddressPart::City => city.to_string(),
            AddressPart::State => state.to_string(),
            AddressPart::Country => country.to_string(),
            AddressPart::PostalCode => postal_code,
            AddressPart::Full => format!("{street}, {city}, {state} {postal_code}, {country}"),
        };
        Ok(Value::String(value))
    }
}

#[derive(Clone)]
struct TemplateGenerator {
    handlebars: Arc<Handlebars<'static>>,
    dependencies: Vec<String>,
    null_rate: Option<f64>,
}

impl FieldGenerator for TemplateGenerator {
    fn generate(&mut self, ctx: &RowContext, rng: &mut StdRng) -> Result<Value> {
        if should_emit_null(self.null_rate, rng)? {
            return Ok(Value::Null);
        }
        let data = ctx
            .iter()
            .map(|(key, value)| (key.clone(), value.template_value()))
            .collect::<serde_json::Map<_, _>>();
        let rendered = self.handlebars.render("value", &data)?;
        Ok(Value::String(rendered))
    }
}

#[derive(Clone)]
struct IntRangeGenerator {
    min: i64,
    max: i64,
    null_rate: Option<f64>,
}

impl FieldGenerator for IntRangeGenerator {
    fn generate(&mut self, _ctx: &RowContext, rng: &mut StdRng) -> Result<Value> {
        if should_emit_null(self.null_rate, rng)? {
            return Ok(Value::Null);
        }
        ensure_range(self.min, self.max, "int_range")?;
        Ok(Value::I64(rng.gen_range(self.min..=self.max)))
    }
}

#[derive(Clone)]
struct FloatRangeGenerator {
    min: f64,
    max: f64,
    precision: Option<usize>,
    null_rate: Option<f64>,
}

impl FieldGenerator for FloatRangeGenerator {
    fn generate(&mut self, _ctx: &RowContext, rng: &mut StdRng) -> Result<Value> {
        if should_emit_null(self.null_rate, rng)? {
            return Ok(Value::Null);
        }
        ensure_float_range(self.min, self.max, "float_range")?;
        let mut value = rng.gen_range(self.min..=self.max);
        if let Some(precision) = self.precision {
            value = round_to_precision(value, precision);
        }
        Ok(Value::F64(value))
    }
}

/// Bounds are held as unscaled units at `scale`, converted once when the spec
/// is compiled. Drawing an integer in those units avoids a round trip through
/// f64 and `Decimal`, and avoids rendering a string the Parquet writer would
/// only parse back.
#[derive(Clone)]
struct DecimalRangeGenerator {
    min_units: i128,
    max_units: i128,
    scale: u32,
    null_rate: Option<f64>,
}

impl FieldGenerator for DecimalRangeGenerator {
    fn generate(&mut self, _ctx: &RowContext, rng: &mut StdRng) -> Result<Value> {
        if should_emit_null(self.null_rate, rng)? {
            return Ok(Value::Null);
        }
        let units = if self.min_units == self.max_units {
            self.min_units
        } else {
            rng.gen_range(self.min_units..=self.max_units)
        };
        Ok(Value::Decimal {
            units,
            scale: self.scale,
        })
    }
}

#[derive(Clone)]
struct FluctuatingGenerator {
    data_type: NumericDataType,
    current: Decimal,
    min: Decimal,
    max: Decimal,
    direction: i8,
    step_min: Decimal,
    step_max: Decimal,
    flip_chance: f64,
    precision: Option<usize>,
    scale: Option<u32>,
    null_rate: Option<f64>,
}

impl FieldGenerator for FluctuatingGenerator {
    fn generate(&mut self, _ctx: &RowContext, rng: &mut StdRng) -> Result<Value> {
        if should_emit_null(self.null_rate, rng)? {
            return Ok(Value::Null);
        }
        if rng.gen_bool(self.flip_chance) {
            self.direction *= -1;
        }
        let min = decimal_to_f64(self.step_min)?;
        let max = decimal_to_f64(self.step_max)?;
        ensure_float_range(min, max, "fluctuating step")?;
        let step = Decimal::from_f64_retain(rng.gen_range(min..=max))
            .ok_or_else(|| anyhow!("failed to generate fluctuating step"))?;
        let next = if self.direction >= 0 {
            self.current + step
        } else {
            self.current - step
        };
        if next > self.max {
            self.direction = -1;
            self.current = self.max;
        } else if next < self.min {
            self.direction = 1;
            self.current = self.min;
        } else {
            self.current = next;
        }
        match self.data_type {
            NumericDataType::Int => Ok(Value::I64(
                decimal_to_f64(self.current)?
                    .round()
                    .clamp(i64::MIN as f64, i64::MAX as f64) as i64,
            )),
            NumericDataType::Float | NumericDataType::Double => {
                let mut value = decimal_to_f64(self.current)?;
                if let Some(precision) = self.precision {
                    value = round_to_precision(value, precision);
                }
                Ok(Value::F64(value))
            }
            NumericDataType::Decimal => {
                let value = self
                    .scale
                    .map(|scale| self.current.round_dp(scale))
                    .unwrap_or(self.current);
                Ok(Value::String(value.to_string()))
            }
        }
    }
}

/// Rows between clock reads when the spec gives no explicit `base`. Offsets
/// here span hours or days, so a base that trails real time by the few
/// milliseconds it takes to emit this many rows is invisible in the data, and
/// it removes a clock read per field per row.
///
/// Caching the clock would flatten the sub-second digits, which used to come
/// from `Utc::now()` and gave the column most of its cardinality. The offset
/// is drawn in microseconds rather than seconds to put that back, so the
/// values stay as distinct as they were. That matters: a datetime column that
/// suddenly compresses well would quietly change what a load test measures.
const NOW_REFRESH_ROWS: u32 = 4096;

#[derive(Clone)]
struct DateTimeAroundGenerator {
    base: Option<DateTime<Utc>>,
    offset_micros_min: i64,
    offset_micros_max: i64,
    format: Arc<str>,
    cached_now: Option<DateTime<Utc>>,
    rows_until_refresh: u32,
    null_rate: Option<f64>,
}

impl DateTimeAroundGenerator {
    fn base(&mut self) -> DateTime<Utc> {
        if let Some(base) = self.base {
            return base;
        }
        match self.cached_now {
            Some(now) if self.rows_until_refresh > 0 => {
                self.rows_until_refresh -= 1;
                now
            }
            _ => {
                let now = Utc::now();
                self.cached_now = Some(now);
                self.rows_until_refresh = NOW_REFRESH_ROWS;
                now
            }
        }
    }
}

impl FieldGenerator for DateTimeAroundGenerator {
    fn generate(&mut self, _ctx: &RowContext, rng: &mut StdRng) -> Result<Value> {
        if should_emit_null(self.null_rate, rng)? {
            return Ok(Value::Null);
        }
        let offset = if self.offset_micros_min == self.offset_micros_max {
            self.offset_micros_min
        } else {
            rng.gen_range(self.offset_micros_min..=self.offset_micros_max)
        };
        Ok(Value::Timestamp {
            micros: self.base().timestamp_micros().saturating_add(offset),
            format: self.format.clone(),
        })
    }
}

#[derive(Clone)]
struct DateTimeRangeGenerator {
    start_seconds: i64,
    end_seconds: i64,
    format: Arc<str>,
    null_rate: Option<f64>,
}

impl FieldGenerator for DateTimeRangeGenerator {
    fn generate(&mut self, _ctx: &RowContext, rng: &mut StdRng) -> Result<Value> {
        if should_emit_null(self.null_rate, rng)? {
            return Ok(Value::Null);
        }
        let seconds = if self.start_seconds == self.end_seconds {
            self.start_seconds
        } else {
            rng.gen_range(self.start_seconds..=self.end_seconds)
        };
        Ok(Value::Timestamp {
            micros: seconds.saturating_mul(1_000_000),
            format: self.format.clone(),
        })
    }
}

#[derive(Clone)]
struct ChoiceGenerator {
    values: Vec<Value>,
    null_rate: Option<f64>,
}

impl FieldGenerator for ChoiceGenerator {
    fn generate(&mut self, _ctx: &RowContext, rng: &mut StdRng) -> Result<Value> {
        if should_emit_null(self.null_rate, rng)? {
            return Ok(Value::Null);
        }
        if self.values.is_empty() {
            bail!("choice requires at least one value");
        }
        Ok(self.values[rng.gen_range(0..self.values.len())].clone())
    }
}

#[derive(Clone)]
struct WeightedChoiceGenerator {
    values: Vec<WeightedValue>,
    total_weight: f64,
    null_rate: Option<f64>,
}

impl FieldGenerator for WeightedChoiceGenerator {
    fn generate(&mut self, _ctx: &RowContext, rng: &mut StdRng) -> Result<Value> {
        if should_emit_null(self.null_rate, rng)? {
            return Ok(Value::Null);
        }
        if self.values.is_empty() || self.total_weight <= 0.0 {
            bail!("weighted_choice requires positive weighted values");
        }
        let mut threshold = rng.gen_range(0.0..self.total_weight);
        for entry in &self.values {
            threshold -= entry.weight;
            if threshold <= 0.0 {
                return Ok(entry.value.clone());
            }
        }
        Ok(self.values[self.values.len() - 1].value.clone())
    }
}

#[derive(Clone)]
struct UuidGenerator {
    null_rate: Option<f64>,
}

impl FieldGenerator for UuidGenerator {
    fn generate(&mut self, _ctx: &RowContext, rng: &mut StdRng) -> Result<Value> {
        if should_emit_null(self.null_rate, rng)? {
            return Ok(Value::Null);
        }
        // `Uuid::new_v4` draws from the OS entropy pool, which is a getrandom
        // syscall per value on any kernel without the vDSO entry point added
        // in 6.11. A spec with three UUID columns made three syscalls a row.
        // The generator is already handed a ChaCha12 stream seeded from that
        // same pool once per thread, so take the sixteen bytes from there: the
        // result is a v4 UUID by the same construction, just without the trip
        // into the kernel.
        let mut bytes = [0u8; 16];
        rng.fill_bytes(&mut bytes);
        let uuid = uuid::Builder::from_random_bytes(bytes).into_uuid();
        // encode_lower writes into a stack buffer. `to_string` would go
        // through Display and the formatting machinery to reach the same 36
        // characters.
        let mut buffer = [0u8; uuid::fmt::Hyphenated::LENGTH];
        Ok(Value::String(
            uuid.hyphenated().encode_lower(&mut buffer).to_string(),
        ))
    }
}

#[derive(Clone)]
struct RandomBytesGenerator {
    min_bytes: usize,
    max_bytes: usize,
    encoding: RandomBytesEncoding,
    null_rate: Option<f64>,
}

impl FieldGenerator for RandomBytesGenerator {
    fn generate(&mut self, _ctx: &RowContext, rng: &mut StdRng) -> Result<Value> {
        if should_emit_null(self.null_rate, rng)? {
            return Ok(Value::Null);
        }
        ensure_range(self.min_bytes, self.max_bytes, "random_bytes length")?;
        let len = rng.gen_range(self.min_bytes..=self.max_bytes);
        let mut bytes = vec![0u8; len];
        rng.fill_bytes(&mut bytes);
        let value = match self.encoding {
            RandomBytesEncoding::Hex => hex_encode(&bytes),
            RandomBytesEncoding::Base64 => general_purpose::STANDARD.encode(bytes),
            RandomBytesEncoding::Base64url => general_purpose::URL_SAFE_NO_PAD.encode(bytes),
        };
        Ok(Value::String(value))
    }
}

/// Short byte rendering for startup messages.
fn fmt_bytes_short(value: u64) -> String {
    const UNITS: [(u64, &str); 3] = [(1u64 << 30, "GiB"), (1u64 << 20, "MiB"), (1u64 << 10, "KiB")];
    for (scale, suffix) in UNITS {
        if value >= scale {
            return format!("{:.0}{}", value as f64 / scale as f64, suffix);
        }
    }
    format!("{}B", value)
}

/// Write a pre-flight file for the user to edit. Refuses to clobber.
fn write_text_file(dest: &Path, text: &str, what: &str) -> Result<()> {
    if dest == Path::new("-") {
        let mut stdout = io::stdout().lock();
        stdout.write_all(text.as_bytes())?;
        stdout.flush()?;
        return Ok(());
    }
    if dest.exists() {
        bail!(
            "{} already exists; delete it or choose another path",
            dest.display()
        );
    }
    std::fs::write(dest, text)
        .with_context(|| format!("failed to write {}", dest.display()))?;
    eprintln!("wrote {} {}", what, dest.display());
    Ok(())
}

/// Write a derived spec for the user to edit. Refuses to clobber an existing file.
fn write_emitted_spec(dest: &Path, spec_text: &str) -> Result<()> {
    if dest == Path::new("-") {
        let mut stdout = io::stdout().lock();
        stdout.write_all(spec_text.as_bytes())?;
        stdout.flush()?;
        return Ok(());
    }
    if dest.exists() {
        bail!(
            "{} already exists; delete it or choose another path",
            dest.display()
        );
    }
    std::fs::write(dest, spec_text)
        .with_context(|| format!("failed to write {}", dest.display()))?;
    eprintln!("wrote {}", dest.display());
    Ok(())
}

/// Load the Doris DDL and pick the table to generate for.
fn load_schema_table(path: &Path, table: Option<&str>) -> Result<schema::Table> {
    let sql = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read schema {}", path.display()))?;
    let tables = schema::parse_schema(&sql)
        .with_context(|| format!("failed to parse schema {}", path.display()))?;
    schema::select_table(tables, table)
}

/// Every schema column needs a generator. Extra spec fields are allowed:
/// they act as hidden intermediates and are simply not written.
fn validate_spec_covers(spec: &CompiledSpec, columns: &[schema::Column]) -> Result<()> {
    let generated: HashSet<&str> = spec.fields.iter().map(|f| f.name.as_str()).collect();
    let missing: Vec<&str> = columns
        .iter()
        .map(|column| column.name.as_str())
        .filter(|name| !generated.contains(name))
        .collect();
    if !missing.is_empty() {
        bail!(
            "the spec has no field for these schema columns: {}",
            missing.join(", ")
        );
    }
    Ok(())
}

/// Generate rows into record batches and hand them to the queue.
///
/// This never touches the network. Upload work happens in `upload_worker`, so
/// a row-group encode or a slow part upload cannot stall generation; the queue
/// absorbs the difference.
#[allow(clippy::too_many_arguments)]
async fn generator_loop(
    spec: CompiledSpec,
    columns: Vec<schema::Column>,
    queue: async_channel::Sender<arrow::record_batch::RecordBatch>,
    batch_rows: usize,
    remaining_rows: Option<Arc<AtomicU64>>,
    target_bytes: Option<u64>,
    stats: Arc<sink::Stats>,
    deadline: Option<Instant>,
) -> Result<()> {
    let mut rng = StdRng::from_entropy();
    let mut fields = (*spec.fields).clone();
    let mut ctx = RowContext::with_capacity(spec.fields.len());
    let mut carry = RowContext::new();
    let mut builder = parquet_out::BatchBuilder::new(columns, batch_rows)?;

    loop {
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            break;
        }
        if target_bytes.is_some_and(|target| reached_size_target(&stats, target)) {
            break;
        }
        let batch_target = reserve_batch(batch_rows, remaining_rows.as_ref());
        if batch_target == 0 {
            break;
        }

        if matches!(spec.context_reset, ContextReset::Batch) {
            carry.clear();
        }
        for _ in 0..batch_target {
            match spec.context_reset {
                ContextReset::Row => ctx.clear(),
                ContextReset::Batch | ContextReset::Never => ctx.clone_from(&carry),
            }
            for &field_index in spec.generation_order.iter() {
                let field = &mut fields[field_index];
                let value = field
                    .generator
                    .generate(&ctx, &mut rng)
                    .with_context(|| format!("failed to generate field `{}`", field.name))?;
                ctx.insert(field.name.clone(), value);
            }
            if !matches!(spec.context_reset, ContextReset::Row) {
                carry.clone_from(&ctx);
            }
            builder.append_row(|name| ctx.get(name))?;
        }

        let rows = builder.rows() as u64;
        let batch = builder.finish()?;
        // Blocks only when the queue is full, which is the backpressure we want.
        if queue.send(batch).await.is_err() {
            // Every upload worker is gone; nothing left to do.
            break;
        }
        stats
            .rows_generated
            .fetch_add(rows, Ordering::Relaxed);
    }
    Ok(())
}

/// Drain the queue, writing batches to this worker's own Parquet files.
/// Runs until the queue is closed and empty, then closes the file in flight.
/// Drain the queue into one Parquet writer.
///
/// A failure here has already been through every HTTP-level retry the store
/// was configured with, so the question is what a run measured in days should
/// do about it. Taking the whole job down over one file is the wrong answer:
/// the writer abandons that file, counts it, and keeps draining. `file_retries`
/// bounds how much of that is tolerable before the run really does fail, and
/// zero restores failing on the first error.
async fn upload_worker(
    queue: async_channel::Receiver<arrow::record_batch::RecordBatch>,
    mut sink: sink::ParquetSink,
    file_retries: u32,
    writer_id: usize,
    remaining_rows: Option<Arc<AtomicU64>>,
) -> Result<()> {
    let mut abandoned = 0u32;
    while let Ok(batch) = queue.recv().await {
        let Err(error) = sink.write(batch).await else {
            continue;
        };
        abandoned += 1;
        if abandoned > file_retries {
            return Err(error).with_context(|| {
                format!(
                    "writer {} gave up after {} failed files; raise upload.file_retries \
                     to tolerate more, or upload.retry_timeout to retry each request longer",
                    writer_id, abandoned
                )
            });
        }
        eprintln!(
            "writer {}: upload failed ({} of {} tolerated), abandoning this file \
             and starting a new one: {:#}",
            writer_id, abandoned, file_retries, error
        );
        let lost = sink.abandon_active().await;
        // A --rows run spends its quota when a row is generated, not when it
        // lands, so without this the run would finish short by exactly the
        // rows in the file just thrown away. Putting them back has the
        // generators make up the difference, provided they are still running.
        // A --target-size run needs no such help: abandoned bytes never reach
        // the counter the target is measured against.
        if let Some(remaining) = remaining_rows.as_ref() {
            remaining.fetch_add(lost, Ordering::Relaxed);
        }
    }
    // Always close: an unclosed multipart upload never becomes an object.
    if let Err(error) = sink.finish().await {
        abandoned += 1;
        if abandoned > file_retries {
            return Err(error)
                .with_context(|| format!("writer {} failed to close its last file", writer_id));
        }
        eprintln!(
            "writer {}: could not close the last file ({} of {} tolerated): {:#}",
            writer_id, abandoned, file_retries, error
        );
    }
    Ok(())
}

/// Redraw the status block once a second while generation runs.
#[allow(clippy::too_many_arguments)]
async fn status_loop(
    stats: Arc<sink::Stats>,
    threads: usize,
    writers: usize,
    queue: async_channel::Receiver<arrow::record_batch::RecordBatch>,
    target_rows: Option<u64>,
    target_bytes: Option<u64>,
    file_cap: Option<u64>,
    started: Instant,
    draining: Arc<std::sync::atomic::AtomicBool>,
    done: Arc<std::sync::atomic::AtomicBool>,
) {
    use std::io::IsTerminal;
    let interactive = io::stderr().is_terminal();
    let mut painted = 0usize;
    // Bytes only materialise when a row group finishes encoding, which happens
    // every few seconds. A one-second delta therefore reads 0, then a spike.
    // Averaging over a short window reports the real throughput instead.
    const RATE_WINDOW: Duration = Duration::from_secs(5);
    let mut history: std::collections::VecDeque<(Instant, u64, u64)> =
        std::collections::VecDeque::new();

    loop {
        let finished = done.load(Ordering::Relaxed);
        let now = Instant::now();
        let rows = stats.rows_generated.load(Ordering::Relaxed);
        let bytes = stats.total_bytes();
        history.push_back((now, rows, bytes));
        while history.len() > 1
            && now.duration_since(history.front().expect("non-empty").0) > RATE_WINDOW
        {
            history.pop_front();
        }
        let (oldest, base_rows, base_bytes) = *history.front().expect("just pushed");
        let seconds = now.duration_since(oldest).as_secs_f64();
        // A single sample carries no interval, so report nothing rather than
        // dividing by an arbitrary epsilon and printing a wild number.
        let (rows_per_sec, bytes_per_sec) = if seconds < 0.5 {
            (0.0, 0.0)
        } else {
            (
                rows.saturating_sub(base_rows) as f64 / seconds,
                bytes.saturating_sub(base_bytes) as f64 / seconds,
            )
        };

        let snapshot = status::Snapshot {
            elapsed: now.duration_since(started),
            threads,
            writers,
            queued: queue.len(),
            queue_capacity: queue.capacity().unwrap_or(0),
            rows,
            target_rows,
            bytes_generated: bytes,
            bytes_uploaded: bytes,
            target_bytes,
            buffered_bytes: stats.buffered_bytes.load(Ordering::Relaxed),
            files_completed: stats.files_completed.load(Ordering::Relaxed),
            active_files: Vec::new(),
            rows_per_sec,
            // Generation produces rows, not bytes: nothing weighs anything
            // until a row group is encoded. Only the upload side has a real
            // byte rate, so do not invent a second one from the same delta.
            gen_bytes_per_sec: 0.0,
            upload_bytes_per_sec: bytes_per_sec,
            file_cap,
            draining: draining.load(Ordering::Relaxed) && !finished,
            finished,
        };
        let width = terminal_width();
        let lines = status::render(&snapshot, width);
        if interactive {
            let mut out = String::new();
            if painted > 0 {
                // Move back over the previous block and clear each line.
                out.push_str(&format!("\x1b[{}A", painted));
            }
            for line in &lines {
                out.push_str("\x1b[2K");
                out.push_str(line);
                out.push('\n');
            }
            eprint!("{}", out);
            let _ = io::stderr().flush();
            painted = lines.len();
        } else if finished || now.duration_since(started).as_secs() % 10 == 0 {
            // Non-interactive output would scroll, so keep it sparse.
            eprintln!("{}", lines.join(" | "));
        }

        if finished {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Has enough data been produced to satisfy a size target?
///
/// Bytes only become known when a row group is encoded, so rows sitting in the
/// queue or in a half-built row group weigh nothing yet. Projecting the
/// observed bytes-per-row over every row generated counts them, which stops
/// generation at the right moment instead of long past it.
fn reached_size_target(stats: &sink::Stats, target: u64) -> bool {
    let produced = stats.total_bytes();
    if produced >= target {
        return true;
    }
    // Pair the byte total with the rows those bytes actually came from.
    // Using every row handed to a writer would divide known bytes by more rows
    // than produced them, understating bytes-per-row and overshooting.
    let flushed = stats.rows_flushed.load(Ordering::Relaxed);
    if flushed == 0 || produced == 0 {
        return false;
    }
    let generated = stats.rows_generated.load(Ordering::Relaxed);
    let projected = (produced as u128) * (generated as u128) / (flushed as u128);
    projected >= target as u128
}

/// Generation threads default to the machine's CPU count.
fn default_threads() -> usize {
    std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(4)
}

/// Rough peak memory one upload worker holds: the row group it is building
/// plus the multipart parts it has in flight.
fn estimate_writer_bytes(
    row_group_rows: usize,
    columns: &[schema::Column],
    part_size: usize,
) -> u64 {
    // A crude per-value figure; string columns dominate real schemas.
    let bytes_per_value = 24u64;
    let row_group = row_group_rows as u64 * columns.len() as u64 * bytes_per_value;
    row_group + part_size as u64
}

/// Terminal width, falling back to 80 when it cannot be determined.
fn terminal_width() -> usize {
    std::env::var("COLUMNS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .or_else(|| {
            let output = std::process::Command::new("stty")
                .arg("size")
                .stdin(std::process::Stdio::inherit())
                .output()
                .ok()?;
            let text = String::from_utf8_lossy(&output.stdout);
            text.split_whitespace().nth(1)?.parse::<usize>().ok()
        })
        .filter(|width| *width >= 20)
        .unwrap_or(80)
}

/// Spawn one writer per thread, run them to completion, then report.
async fn run_parquet(
    args: Args,
    compiled: CompiledSpec,
    table: schema::Table,
    s3_config: Option<s3::OutputConfig>,
) -> Result<()> {
    use std::sync::atomic::AtomicBool;

    let mut gen_threads = args.threads.unwrap_or_else(default_threads).max(1);
    if compiled.has_stateful_ordered && gen_threads > 1 {
        eprintln!(
            "stateful ordered generators detected; generation threads reduced from {} to 1",
            gen_threads
        );
        gen_threads = 1;
    }
    compiled.check_sequence_capacity(args.rows, gen_threads)?;
    let upload_threads = args.upload_threads.max(1);
    let queue_depth = args.queue_depth.max(1);

    // A local run writes straight to a file, so there is no upload to give up
    // on and nothing to tolerate.
    let file_retries = s3_config
        .as_ref()
        .map(|config| config.upload.file_retries)
        .unwrap_or(0);
    let (destination, part_size, max_concurrent_parts, row_group_rows, compression, dictionary) = match &s3_config {
        Some(config) => (
            sink::Destination::S3 { config: Box::new(config.clone()) },
            config.upload.part_size as usize,
            config.upload.max_concurrent_parts,
            config.parquet.row_group_rows,
            sink::parse_compression(&config.parquet.compression, config.parquet.compression_level)?,
            config.parquet.dictionary,
        ),
        None => (
            sink::Destination::Local {
                directory: args.out_dir.clone().expect("checked by the caller"),
            },
            10 << 20,
            8,
            200_000,
            sink::parse_compression("zstd", args.compression_level)?,
            true,
        ),
    };

    let columns = table.columns.clone();
    let schema_ref = parquet_out::arrow_schema(&columns)?;
    let settings = Arc::new(sink::SinkSettings {
        schema: schema_ref,
        row_group_rows,
        part_size,
        max_concurrent_parts,
        file_cap: Some(args.file_size),
        compression,
        dictionary,
    });

    let (store, prefix) = sink::build_store(&destination)?;
    let stats = Arc::new(sink::Stats::default());

    match &s3_config {
        Some(config) => eprintln!(
            "writing Parquet to s3://{}/{}  files roll at {}  part {} x {} in flight",
            config.s3.bucket,
            config.normalised_prefix(),
            fmt_bytes_short(args.file_size),
            fmt_bytes_short(config.upload.part_size),
            config.upload.max_concurrent_parts,
        ),
        None => eprintln!(
            "writing Parquet to {}  files roll at {}",
            args.out_dir.as_ref().expect("local destination").display(),
            fmt_bytes_short(args.file_size),
        ),
    }
    let batch_rows = args.batch_rows.max(1).min(row_group_rows.max(1));
    eprintln!(
        "table `{}`  {} columns  {} generator + {} upload thread(s)  queue {} x {} rows",
        table.name,
        columns.len(),
        gen_threads,
        upload_threads,
        queue_depth,
        batch_rows,
    );
    // Peak memory is dominated by what each upload worker holds open.
    let per_writer = estimate_writer_bytes(row_group_rows, &columns, part_size);
    eprintln!(
        "estimated peak memory ~{} ({} per upload worker x {})",
        fmt_bytes_short(per_writer * upload_threads as u64),
        fmt_bytes_short(per_writer),
        upload_threads,
    );

    let started = Instant::now();
    let deadline = args.time.map(|limit| started + limit);
    let remaining_rows = args.rows.map(AtomicU64::new).map(Arc::new);

    let (tx, rx) = async_channel::bounded(queue_depth);

    let done = Arc::new(AtomicBool::new(false));
    let draining = Arc::new(AtomicBool::new(false));
    let status = if args.no_progress {
        None
    } else {
        Some(tokio::spawn(status_loop(
            stats.clone(),
            gen_threads,
            upload_threads,
            rx.clone(),
            args.rows,
            args.target_size,
            Some(args.file_size),
            started,
            draining.clone(),
            done.clone(),
        )))
    };

    // Upload workers start first so they are ready to drain immediately.
    let mut uploaders = Vec::with_capacity(upload_threads);
    for writer_id in 0..upload_threads {
        let sink = sink::ParquetSink::new(
            store.clone(),
            prefix.clone(),
            writer_id,
            settings.clone(),
            stats.clone(),
        );
        uploaders.push(tokio::spawn(upload_worker(
            rx.clone(),
            sink,
            file_retries,
            writer_id,
            remaining_rows.clone(),
        )));
    }
    // Only the workers should hold receivers, so the queue can close cleanly.
    drop(rx);

    let mut generators = Vec::with_capacity(gen_threads);
    for _ in 0..gen_threads {
        generators.push(tokio::spawn(generator_loop(
            compiled.clone(),
            columns.clone(),
            tx.clone(),
            batch_rows,
            remaining_rows.clone(),
            args.target_size,
            stats.clone(),
            deadline,
        )));
    }
    // Dropping the last sender is what eventually closes the queue.
    drop(tx);

    let mut failure: Option<anyhow::Error> = None;
    for generator in generators {
        match generator.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                failure.get_or_insert(error);
            }
            Err(error) => {
                failure.get_or_insert(anyhow!("generator task panicked: {}", error));
            }
        }
    }
    // Generators are done and their senders dropped, so the workers now drain
    // whatever is left in the queue and finish their files.
    draining.store(true, Ordering::Relaxed);
    for uploader in uploaders {
        match uploader.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                failure.get_or_insert(error);
            }
            Err(error) => {
                failure.get_or_insert(anyhow!("upload worker panicked: {}", error));
            }
        }
    }

    done.store(true, Ordering::Relaxed);
    if let Some(status) = status {
        let _ = status.await;
    }

    if let Some(error) = failure {
        return Err(error);
    }

    let elapsed = started.elapsed();
    let rows = stats.rows.load(Ordering::Relaxed);
    let bytes = stats.bytes_written.load(Ordering::Relaxed);
    debug_assert_eq!(rows, stats.rows_generated.load(Ordering::Relaxed));
    let abandoned = stats.files_abandoned.load(Ordering::Relaxed);
    eprintln!(
        "done: {} rows in {} files, {} in {:.1}s ({} rows/s){}",
        rows,
        stats.files_completed.load(Ordering::Relaxed),
        fmt_bytes_short(bytes),
        elapsed.as_secs_f64(),
        (rows as f64 / elapsed.as_secs_f64()) as u64,
        if abandoned == 0 {
            String::new()
        } else {
            format!(
                "  [{} file(s) abandoned after repeated upload failures, \
                 {} rows lost]",
                abandoned,
                stats.rows_lost.load(Ordering::Relaxed)
            )
        },
    );
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Parquet encoding is synchronous CPU work that occupies a runtime worker
    // for its duration. Size the pool for generators plus upload workers so a
    // batch of concurrent encodes cannot starve generation.
    let workers = args.threads.unwrap_or_else(default_threads).max(1)
        + args.upload_threads.max(1)
        + 2;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers.min(64))
        .enable_all()
        .build()
        .context("failed to start the async runtime")?;
    runtime.block_on(run(args))
}

async fn run(args: Args) -> Result<()> {
    if args.init {
        init_sample_spec()?;
        return Ok(());
    }

    if let Some(dest) = args.emit_s3_config.as_ref() {
        return write_text_file(dest, s3::SAMPLE_TOML, "S3 config");
    }

    // Fail on a bad S3 config before spending time generating anything.
    let s3_config = match args.s3_config.as_ref() {
        Some(path) => {
            let config = s3::load(path)?;
            config.check_file_cap(args.file_size)?;
            Some(config)
        }
        None => None,
    };

    // The schema supplies Parquet column types, order and nullability.
    let schema_table = match args.schema.as_ref() {
        Some(path) => Some(load_schema_table(path, args.table.as_deref())?),
        None => None,
    };

    if let Some(dest) = args.emit_spec.as_ref() {
        let table = schema_table
            .as_ref()
            .ok_or_else(|| anyhow!("--emit-spec requires --schema"))?;
        let yaml = derive::spec_yaml_from_table(table)?;
        return write_emitted_spec(dest, &yaml);
    }

    // A hand-written spec wins over one derived from the schema.
    let spec_text = if let Some(name) = &args.preset {
        preset_yaml(name)
            .with_context(|| format!("unknown preset '{}'; available: user, order, product, event", name))?
            .to_owned()
    } else if let Some(spec_path) = args.spec.as_ref() {
        std::fs::read_to_string(spec_path)
            .with_context(|| format!("failed to read spec {}", spec_path.display()))?
    } else if let Some(table) = schema_table.as_ref() {
        eprintln!(
            "deriving a spec for table `{}`: {} columns",
            table.name,
            table.columns.len()
        );
        derive::spec_yaml_from_table(table)?
    } else {
        use clap::CommandFactory;
        let mut cmd = Args::command();
        cmd.print_help()?;
        eprintln!("\nerror: one of --spec <FILE>, --schema <FILE> or --preset <NAME> is required");
        std::process::exit(1);
    };

    let raw: RawSpec = serde_yaml::from_str(&spec_text).context("failed to parse YAML spec")?;
    let compiled = compile_spec(raw)?;

    // Parquet output is selected by naming a destination.
    if s3_config.is_some() || args.out_dir.is_some() {
        let table = schema_table.ok_or_else(|| {
            anyhow!("Parquet output needs --schema: column types come from the Doris DDL")
        })?;
        validate_spec_covers(&compiled, &table.columns)?;
        return run_parquet(args, compiled, table, s3_config).await;
    }

    let mut effective_threads = args.threads.unwrap_or_else(default_threads).max(1);
    if compiled.has_stateful_ordered && effective_threads > 1 {
        eprintln!(
            "stateful ordered generators detected; effective producer threads reduced from {} to 1",
            effective_threads
        );
        effective_threads = 1;
    }
    compiled.check_sequence_capacity(args.rows, effective_threads)?;

    let (tx, rx) = mpsc::channel::<BatchMessage>(3);
    let remaining_rows = args.rows.map(AtomicU64::new).map(Arc::new);
    for _ in 0..effective_threads {
        let tx = tx.clone();
        let spec = compiled.clone();
        let remaining_rows = remaining_rows.clone();
        tokio::spawn(async move {
            let _ = producer_loop(spec, tx, remaining_rows).await;
        });
    }
    drop(tx);

    let limiter = RateLimiter::new(args.rows_per_second, args.bytes_per_second);
    let output_dest = match args.output {
        Some(path) => OutputDest::File { path, split_bytes: args.split_bytes },
        None => OutputDest::Stdout,
    };
    consumer_loop(compiled, rx, limiter, args.no_header, args.time, &output_dest).await
}

fn init_sample_spec() -> Result<()> {
    let yaml_path = PathBuf::from("sample.yaml");
    let js_path = PathBuf::from("sample_script.js");
    if yaml_path.exists() || js_path.exists() {
        eprintln!(
            "sample.yaml or sample_script.js already exists; \
             delete both files and re-run --init to regenerate"
        );
        return Ok(());
    }
    std::fs::write(&js_path, SAMPLE_JS).context("failed to write sample_script.js")?;
    eprintln!("created sample_script.js");
    std::fs::write(&yaml_path, SAMPLE_SPEC).context("failed to write sample.yaml")?;
    eprintln!("created sample.yaml");
    Ok(())
}

async fn producer_loop(
    spec: CompiledSpec,
    tx: mpsc::Sender<BatchMessage>,
    remaining_rows: Option<Arc<AtomicU64>>,
) -> Result<()> {
    let mut rng = StdRng::from_entropy();
    let mut fields = (*spec.fields).clone();
    // Reused across every row — cleared or cloned-into rather than reallocated.
    let mut ctx = RowContext::with_capacity(spec.fields.len());
    // Carry context for Batch / Never reset modes.
    let mut carry = RowContext::new();

    loop {
        let batch_target = reserve_batch(spec.batch_rows, remaining_rows.as_ref());
        if batch_target == 0 {
            break;
        }
        let mut batch: Vec<Vec<u8>> = Vec::with_capacity(batch_target);

        // Batch mode resets the carry at each batch boundary.
        if matches!(spec.context_reset, ContextReset::Batch) {
            carry.clear();
        }

        for _ in 0..batch_target {
            match spec.context_reset {
                ContextReset::Row => ctx.clear(),
                ContextReset::Batch | ContextReset::Never => ctx.clone_from(&carry),
            }

            for &field_index in spec.generation_order.iter() {
                let field = &mut fields[field_index];
                let value = match field.generator.generate(&ctx, &mut rng) {
                    Ok(value) => value,
                    Err(error) => {
                        let message =
                            format!("failed to generate field '{}': {error:#}", field.name);
                        let _ = tx.send(Err(message)).await;
                        return Ok(());
                    }
                };
                ctx.insert(field.name.clone(), value);
            }

            if !matches!(spec.context_reset, ContextReset::Row) {
                carry.clone_from(&ctx);
            }

            match encode_row(&spec, &ctx) {
                Ok(encoded) => batch.push(encoded),
                Err(error) => {
                    let _ = tx.send(Err(format!("{error:#}"))).await;
                    return Ok(());
                }
            }
        }

        if tx.send(Ok(batch)).await.is_err() {
            break;
        }
    }

    Ok(())
}

fn reserve_batch(batch_rows: usize, remaining_rows: Option<&Arc<AtomicU64>>) -> usize {
    let batch_rows = batch_rows.max(1);
    let Some(remaining) = remaining_rows else {
        return batch_rows;
    };

    loop {
        let current = remaining.load(Ordering::Relaxed);
        if current == 0 {
            return 0;
        }
        let take = current.min(batch_rows as u64);
        if remaining
            .compare_exchange(current, current - take, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return take as usize;
        }
    }
}

enum OutputDest {
    Stdout,
    File { path: PathBuf, split_bytes: Option<u64> },
}

fn make_writer(dest: &OutputDest, file_index: u32) -> Result<Box<dyn Write>> {
    match dest {
        OutputDest::Stdout => Ok(Box::new(io::stdout())),
        OutputDest::File { path, split_bytes } => {
            let file_path = if split_bytes.is_some() {
                let stem = path.file_stem().unwrap_or_default().to_string_lossy();
                let ext = path
                    .extension()
                    .map(|e| format!(".{}", e.to_string_lossy()))
                    .unwrap_or_default();
                let parent = path.parent().unwrap_or(Path::new("."));
                parent.join(format!("{}.{:06}{}", stem, file_index, ext))
            } else {
                path.clone()
            };
            eprintln!("writing to {}", file_path.display());
            let file = std::fs::File::create(&file_path)
                .with_context(|| format!("failed to create output file {}", file_path.display()))?;
            Ok(Box::new(io::BufWriter::new(file)))
        }
    }
}

async fn consumer_loop(
    spec: CompiledSpec,
    mut rx: mpsc::Receiver<BatchMessage>,
    limiter: RateLimiter,
    no_header: bool,
    time_limit: Option<Duration>,
    dest: &OutputDest,
) -> Result<()> {
    let started = Instant::now();
    let header = if no_header { vec![] } else { encode_header(&spec)? };
    let mut file_index: u32 = 1;
    let mut writer = make_writer(dest, file_index)?;
    let mut bytes_in_file: u64 = 0;

    if !header.is_empty() {
        limiter.limit_bytes(header.len() as u64).await?;
        writer.write_all(&header)?;
        bytes_in_file += header.len() as u64;
    }

    while let Some(message) = rx.recv().await {
        let batch = match message {
            Ok(batch) => batch,
            Err(message) => bail!("{message}"),
        };
        for encoded in batch {
            if time_limit.is_some_and(|limit| started.elapsed() >= limit) {
                drop(rx);
                writer.flush()?;
                return Ok(());
            }
            limiter.limit_row().await?;
            limiter.limit_bytes(encoded.len() as u64).await?;
            writer.write_all(&encoded)?;
            bytes_in_file += encoded.len() as u64;

            if let OutputDest::File { split_bytes: Some(threshold), .. } = dest {
                if bytes_in_file >= *threshold {
                    writer.flush()?;
                    file_index += 1;
                    writer = make_writer(dest, file_index)?;
                    bytes_in_file = 0;
                    if !header.is_empty() {
                        writer.write_all(&header)?;
                        bytes_in_file += header.len() as u64;
                    }
                }
            }
        }
    }

    writer.flush()?;
    Ok(())
}

struct RateLimiter {
    rows: Option<TokenQuota>,
    bytes: Option<TokenQuota>,
}

impl RateLimiter {
    fn new(rows_per_second: Option<u64>, bytes_per_second: Option<u64>) -> Self {
        let rows = rows_per_second.map(TokenQuota::new);
        let bytes = bytes_per_second.map(TokenQuota::new);
        Self { rows, bytes }
    }

    async fn limit_row(&self) -> Result<()> {
        if let Some(quota) = &self.rows {
            quota.acquire(1).await;
        }
        Ok(())
    }

    async fn limit_bytes(&self, bytes: u64) -> Result<()> {
        if let Some(quota) = &self.bytes {
            quota.acquire(bytes).await;
        }
        Ok(())
    }
}

struct TokenQuota {
    quota: Arc<precise_rate_limiter::FastQuota>,
    max_acquire: usize,
}

impl TokenQuota {
    fn new(rate_per_second: u64) -> Self {
        let rate = rate_per_second.max(1) as usize;
        // For rates >= 100: refill rate/100 tokens every 10ms (100 ticks/sec).
        // For rates < 100: refill 1 token every 1000/rate ms (exact 1-token ticks).
        // burst = 2 * rate gives up to 2 seconds of initial burst.
        let (refill_amount, interval_ms): (usize, u64) = if rate >= 100 {
            (rate / 100, 10)
        } else {
            (1, 1000 / rate as u64)
        };
        let burst = rate * 2;
        let quota = precise_rate_limiter::FastQuota::new(
            burst,
            refill_amount,
            Duration::from_millis(interval_ms),
        );
        Self {
            quota,
            max_acquire: burst,
        }
    }

    async fn acquire(&self, mut tokens: u64) {
        while tokens > 0 {
            let chunk = tokens.min(self.max_acquire as u64) as usize;
            self.quota.acquire(chunk).await;
            tokens -= chunk as u64;
        }
    }
}

struct JsState {
    context: rquickjs::Context,
    // Runtime must outlive Context; declare it after so it is dropped after.
    _runtime: rquickjs::Runtime,
}

impl JsState {
    fn new(source: &str) -> Result<Self> {
        let runtime =
            rquickjs::Runtime::new().context("failed to create JavaScript runtime")?;
        let context =
            rquickjs::Context::full(&runtime).context("failed to create JavaScript context")?;
        context
            .with(|ctx| ctx.eval::<(), _>(source))
            .context("failed to evaluate JavaScript file")?;
        Ok(Self {
            context,
            _runtime: runtime,
        })
    }
}

struct JavaScriptGenerator {
    function_name: String,
    script_source: Arc<String>,
    deps: Vec<String>,
    null_rate: Option<f64>,
    // Initialized on first generate() call; each producer clone starts with None.
    state: Option<Box<JsState>>,
}

// SAFETY: JavaScriptGenerator is StatefulOrdered (forces single-producer execution).
// Only one task ever holds a JavaScriptGenerator, generate() is synchronous with no
// await points, so the QuickJS runtime is never touched from two threads simultaneously.
// rquickjs's !Send/!Sync comes from Rc<> reference counting, not thread-unsafe global
// state. With exclusive single-producer access this is safe.
unsafe impl Send for JsState {}
unsafe impl Sync for JsState {}
unsafe impl Send for JavaScriptGenerator {}
unsafe impl Sync for JavaScriptGenerator {}

impl Clone for JavaScriptGenerator {
    fn clone(&self) -> Self {
        Self {
            function_name: self.function_name.clone(),
            script_source: Arc::clone(&self.script_source),
            deps: self.deps.clone(),
            null_rate: self.null_rate,
            state: None,
        }
    }
}

impl FieldGenerator for JavaScriptGenerator {
    fn generate(&mut self, ctx: &RowContext, rng: &mut StdRng) -> Result<Value> {
        if should_emit_null(self.null_rate, rng)? {
            return Ok(Value::Null);
        }
        if self.state.is_none() {
            self.state = Some(Box::new(JsState::new(&self.script_source)?));
        }
        let state = self.state.as_mut().unwrap();
        state
            .context
            .with(|js_ctx| {
                let func: rquickjs::Function =
                    js_ctx.globals().get(self.function_name.as_str())?;
                let obj = rquickjs::Object::new(js_ctx.clone())?;
                for (key, val) in ctx {
                    match val {
                        Value::Null => {} // appears as undefined in JS
                        Value::Bool(b) => obj.set(key.as_str(), *b)?,
                        Value::I64(n) => obj.set(key.as_str(), *n as f64)?,
                        Value::F64(f) => obj.set(key.as_str(), *f)?,
                        Value::String(s) => obj.set(key.as_str(), s.as_str())?,
                        // JS has no decimal or timestamp type, so hand these
                        // over as the text a template would have seen.
                        Value::Timestamp { .. } | Value::Decimal { .. } => {
                            obj.set(key.as_str(), val.csv_string("").as_str())?
                        }
                    }
                }
                let result: rquickjs::Value = func.call((obj,))?;
                // Coerce every primitive return type to String.
                let s = if result.is_null() || result.is_undefined() {
                    String::new()
                } else if let Some(b) = result.as_bool() {
                    b.to_string()
                } else if let Some(n) = result.as_int() {
                    n.to_string()
                } else if let Some(f) = result.as_float() {
                    f.to_string()
                } else if let Some(js_str) = result.as_string() {
                    js_str.to_string()?
                } else {
                    return Err(rquickjs::Error::Unknown);
                };
                Ok(Value::String(s))
            })
            .map_err(|e| anyhow!("JavaScript function '{}' failed: {e}", self.function_name))
    }
}

fn compile_spec(raw: RawSpec) -> Result<CompiledSpec> {
    if raw.version != 1 {
        bail!(
            "unsupported spec version {}; only version 1 is supported",
            raw.version
        );
    }
    if raw.fields.is_empty() {
        bail!("spec must define at least one field");
    }

    let csv = compile_csv(raw.csv)?;
    let mut seen = HashSet::new();
    let mut fields = Vec::with_capacity(raw.fields.len());
    for (definition_index, field) in raw.fields.into_iter().enumerate() {
        if field.name.trim().is_empty() {
            bail!("field at index {definition_index} has an empty name");
        }
        if !seen.insert(field.name.clone()) {
            bail!("duplicate field name '{}'", field.name);
        }
        let generator = compile_generator(field.gen)
            .with_context(|| format!("failed to compile field '{}'", field.name))?;
        fields.push(CompiledField {
            name: field.name,
            hidden: field.hidden,
            order: field.order,
            definition_index,
            generator,
        });
    }

    let generation_order = build_generation_order(&fields)?;
    let mut output_order: Vec<usize> = fields
        .iter()
        .enumerate()
        .filter_map(|(index, field)| (!field.hidden).then_some(index))
        .collect();
    output_order.sort_by_key(|&index| (fields[index].order, fields[index].definition_index));
    let has_stateful_ordered = fields
        .iter()
        .any(|field| field.generator.kind() == GeneratorKind::StatefulOrdered);

    Ok(CompiledSpec {
        csv,
        context_reset: raw.context.reset,
        batch_rows: raw.batch.rows.max(1),
        fields: Arc::new(fields),
        generation_order: Arc::new(generation_order),
        output_order: Arc::new(output_order),
        has_stateful_ordered,
    })
}

fn compile_csv(spec: CsvSpec) -> Result<CsvSpecRuntime> {
    let delimiter = char_to_single_byte(spec.delimiter, "csv.delimiter")?;
    let quote = char_to_single_byte(spec.quote, "csv.quote")?;
    let escape = char_to_single_byte(spec.escape, "csv.escape")?;
    Ok(CsvSpecRuntime {
        delimiter,
        quote,
        escape,
        newline: spec.newline.into_bytes(),
        null: spec.null,
    })
}

fn compile_generator(spec: GeneratorSpec) -> Result<Generator> {
    let generator = match spec {
        GeneratorSpec::Constant(spec) => Generator::Constant(ConstantGenerator {
            value: spec.value,
            null_rate: spec.null_rate,
        }),
        GeneratorSpec::Sequence(spec) => Generator::Sequence(SequenceGenerator {
            next: spec.start,
            step: spec.step,
            null_rate: spec.null_rate,
        }),
        GeneratorSpec::SequenceString(spec) => {
            let (prefix, suffix) = split_sequence_template(&spec.template)?;
            let literal = prefix.len() + suffix.len();
            if literal > spec.width {
                bail!(
                    "sequence_string template `{}` has {} literal characters, \
                     which does not fit width {}",
                    spec.template,
                    literal,
                    spec.width
                );
            }
            if spec.step == 0 {
                bail!("sequence_string step must be at least 1");
            }
            let digits = spec.width - literal;
            if digits == 0 {
                bail!(
                    "sequence_string template `{}` fills the whole width of {}, \
                     leaving no room for the counter",
                    spec.template,
                    spec.width
                );
            }
            Generator::SequenceString(SequenceStringGenerator {
                prefix,
                suffix,
                digits,
                max_counter: sequence_string_capacity(digits).saturating_sub(1),
                start: spec.start,
                step: spec.step,
                cursor: Arc::new(AtomicU64::new(0)),
                next_index: 0,
                block_end: 0,
                null_rate: spec.null_rate,
            })
        }
        GeneratorSpec::Name(spec) => Generator::Name(NameGenerator {
            part: spec.part,
            null_rate: spec.null_rate,
        }),
        GeneratorSpec::Email(spec) => Generator::Email(EmailGenerator {
            _style: spec.style,
            null_rate: spec.null_rate,
        }),
        GeneratorSpec::Lorem(spec) => Generator::Lorem(LoremGenerator {
            words_min: spec.words_min,
            words_max: spec.words_max,
            null_rate: spec.null_rate,
        }),
        GeneratorSpec::Address(spec) => Generator::Address(AddressGenerator {
            part: spec.part,
            null_rate: spec.null_rate,
        }),
        GeneratorSpec::Template(spec) => {
            let mut handlebars = handlebars();
            handlebars
                .register_template_string("value", &spec.value)
                .context("invalid Handlebars template")?;
            let dependencies = extract_template_dependencies(&spec.value)?;
            Generator::Template(TemplateGenerator {
                handlebars: Arc::new(handlebars),
                dependencies,
                null_rate: spec.null_rate,
            })
        }
        GeneratorSpec::IntRange(spec) => Generator::IntRange(IntRangeGenerator {
            min: spec.min,
            max: spec.max,
            null_rate: spec.null_rate,
        }),
        GeneratorSpec::FloatRange(spec) => Generator::FloatRange(FloatRangeGenerator {
            min: spec.min,
            max: spec.max,
            precision: spec.precision,
            null_rate: spec.null_rate,
        }),
        GeneratorSpec::DecimalRange(spec) => {
            ensure_decimal_range(spec.min, spec.max, "decimal_range")?;
            if spec.scale > MAX_DECIMAL_SCALE {
                bail!(
                    "decimal_range scale {} is above the maximum of {}",
                    spec.scale,
                    MAX_DECIMAL_SCALE
                );
            }
            Generator::DecimalRange(DecimalRangeGenerator {
                min_units: decimal_to_units(spec.min, spec.scale, "decimal_range min")?,
                max_units: decimal_to_units(spec.max, spec.scale, "decimal_range max")?,
                scale: spec.scale,
                null_rate: spec.null_rate,
            })
        }
        GeneratorSpec::Fluctuating(spec) => {
            ensure_decimal_range(spec.min, spec.max, "fluctuating")?;
            ensure_decimal_range(spec.step_min, spec.step_max, "fluctuating step")?;
            if !(0.0..=1.0).contains(&spec.flip_chance) {
                bail!("fluctuating flip_chance must be between 0.0 and 1.0");
            }
            let direction = match spec.initial_direction {
                InitialDirection::Up => 1,
                InitialDirection::Down => -1,
                InitialDirection::Random => {
                    if thread_rng().gen_bool(0.5) {
                        1
                    } else {
                        -1
                    }
                }
            };
            Generator::Fluctuating(FluctuatingGenerator {
                data_type: spec.data_type,
                current: spec.start,
                min: spec.min,
                max: spec.max,
                direction,
                step_min: spec.step_min,
                step_max: spec.step_max,
                flip_chance: spec.flip_chance,
                precision: spec.precision,
                scale: spec.scale,
                null_rate: spec.null_rate,
            })
        }
        GeneratorSpec::DateTimeAround(spec) => {
            ensure_range(
                spec.offset_seconds_min,
                spec.offset_seconds_max,
                "datetime offset",
            )?;
            Generator::DateTimeAround(DateTimeAroundGenerator {
                base: spec.base.as_deref().map(parse_datetime).transpose()?,
                offset_micros_min: seconds_to_micros(spec.offset_seconds_min)?,
                offset_micros_max: seconds_to_micros(spec.offset_seconds_max)?,
                format: Arc::from(spec.format),
                cached_now: None,
                rows_until_refresh: 0,
                null_rate: spec.null_rate,
            })
        }
        GeneratorSpec::DateTimeRange(spec) => {
            let start_seconds = parse_datetime(&spec.start)?.timestamp();
            let end_seconds = parse_datetime(&spec.end)?.timestamp();
            ensure_range(start_seconds, end_seconds, "datetime_range")?;
            Generator::DateTimeRange(DateTimeRangeGenerator {
                start_seconds,
                end_seconds,
                format: Arc::from(spec.format),
                null_rate: spec.null_rate,
            })
        }
        GeneratorSpec::Choice(spec) => Generator::Choice(ChoiceGenerator {
            values: spec.values,
            null_rate: spec.null_rate,
        }),
        GeneratorSpec::WeightedChoice(spec) => {
            let total_weight = spec.values.iter().try_fold(0.0, |acc, entry| {
                if !entry.weight.is_finite() || entry.weight <= 0.0 {
                    bail!("weighted_choice weights must be positive finite numbers");
                }
                Ok(acc + entry.weight)
            })?;
            Generator::WeightedChoice(WeightedChoiceGenerator {
                values: spec.values,
                total_weight,
                null_rate: spec.null_rate,
            })
        }
        GeneratorSpec::Uuid(spec) => Generator::Uuid(UuidGenerator {
            null_rate: spec.null_rate,
        }),
        GeneratorSpec::RandomBytes(spec) => {
            ensure_range(spec.min_bytes, spec.max_bytes, "random_bytes length")?;
            Generator::RandomBytes(RandomBytesGenerator {
                min_bytes: spec.min_bytes,
                max_bytes: spec.max_bytes,
                encoding: spec.encoding,
                null_rate: spec.null_rate,
            })
        }
        GeneratorSpec::JavaScript(spec) => {
            let script_source = std::fs::read_to_string(&spec.file)
                .with_context(|| format!("failed to read JavaScript file '{}'", spec.file))?;
            Generator::JavaScript(JavaScriptGenerator {
                function_name: spec.function,
                script_source: Arc::new(script_source),
                deps: spec.deps,
                null_rate: spec.null_rate,
                state: None,
            })
        }
    };
    Ok(generator)
}

fn build_generation_order(fields: &[CompiledField]) -> Result<Vec<usize>> {
    let name_to_index: HashMap<_, _> = fields
        .iter()
        .enumerate()
        .map(|(index, field)| (field.name.as_str(), index))
        .collect();
    let mut indegree = vec![0usize; fields.len()];
    let mut outgoing = vec![Vec::new(); fields.len()];

    for (field_index, field) in fields.iter().enumerate() {
        for dependency in field.generator.dependencies() {
            let Some(&dependency_index) = name_to_index.get(dependency.as_str()) else {
                bail!(
                    "field '{}' references missing dependency '{}'",
                    field.name,
                    dependency
                );
            };
            indegree[field_index] += 1;
            outgoing[dependency_index].push(field_index);
        }
    }

    // Priority queue: among simultaneously-ready fields, prefer lower order then earlier definition.
    // std::collections::BinaryHeap is a max-heap, so negate the keys.
    use std::collections::BinaryHeap;
    use std::cmp::Reverse;
    let mut ready: BinaryHeap<Reverse<(i64, usize, usize)>> = indegree
        .iter()
        .enumerate()
        .filter_map(|(index, &count)| {
            (count == 0)
                .then_some(Reverse((fields[index].order, fields[index].definition_index, index)))
        })
        .collect();
    let mut order = Vec::with_capacity(fields.len());

    while let Some(Reverse((_, _, index))) = ready.pop() {
        order.push(index);
        for &dependent in &outgoing[index] {
            indegree[dependent] -= 1;
            if indegree[dependent] == 0 {
                let f = &fields[dependent];
                ready.push(Reverse((f.order, f.definition_index, dependent)));
            }
        }
    }

    if order.len() != fields.len() {
        let cyclic = indegree
            .iter()
            .enumerate()
            .filter_map(|(index, &count)| (count > 0).then_some(fields[index].name.as_str()))
            .collect::<Vec<_>>()
            .join(", ");
        bail!("field dependency DAG cannot be satisfied; possible cycle involving: {cyclic}");
    }

    Ok(order)
}

fn encode_header(spec: &CompiledSpec) -> Result<Vec<u8>> {
    let mut values = Vec::with_capacity(spec.output_order.len());
    for &index in spec.output_order.iter() {
        values.push(Value::String(spec.fields[index].name.clone()));
    }
    encode_values(spec, values)
}

fn encode_row(spec: &CompiledSpec, ctx: &RowContext) -> Result<Vec<u8>> {
    let mut values = Vec::with_capacity(spec.output_order.len());
    for &index in spec.output_order.iter() {
        let field = &spec.fields[index];
        let value = ctx
            .get(&field.name)
            .ok_or_else(|| anyhow!("field '{}' missing from row context", field.name))?
            .clone();
        values.push(value);
    }
    encode_values(spec, values)
}

fn encode_values(spec: &CompiledSpec, values: Vec<Value>) -> Result<Vec<u8>> {
    let mut writer = csv::WriterBuilder::new()
        .delimiter(spec.csv.delimiter)
        .quote(spec.csv.quote)
        .escape(spec.csv.escape)
        .double_quote(false)
        .has_headers(false)
        .from_writer(Vec::new());

    let row = values
        .iter()
        .map(|value| value.csv_string(&spec.csv.null))
        .collect::<Vec<_>>();
    writer.write_record(row)?;
    let mut data = writer.into_inner()?;
    if spec.csv.newline != b"\n" {
        if data.ends_with(b"\n") {
            data.pop();
            data.extend_from_slice(&spec.csv.newline);
        }
    }
    Ok(data)
}

fn handlebars() -> Handlebars<'static> {
    let mut handlebars = Handlebars::new();
    handlebars.set_strict_mode(true);
    handlebars.register_helper(
        "lower",
        Box::new(string_helper(|value| value.to_ascii_lowercase())),
    );
    handlebars.register_helper(
        "upper",
        Box::new(string_helper(|value| value.to_ascii_uppercase())),
    );
    handlebars.register_helper("title", Box::new(string_helper(title_case)));
    handlebars.register_helper("slug", Box::new(string_helper(slug)));
    handlebars
}

fn string_helper<F>(
    transform: F,
) -> impl Fn(&Helper, &Handlebars, &HbContext, &mut RenderContext, &mut dyn Output) -> HelperResult
       + Send
       + Sync
       + 'static
where
    F: Fn(&str) -> String + Send + Sync + 'static,
{
    move |h: &Helper,
          _r: &Handlebars,
          _ctx: &HbContext,
          _rc: &mut RenderContext,
          out: &mut dyn Output| {
        let value = h
            .param(0)
            .ok_or_else(|| {
                RenderError::from(RenderErrorReason::Other("missing helper parameter".into()))
            })?
            .value()
            .render();
        out.write(&transform(&value))?;
        Ok(())
    }
}

fn extract_template_dependencies(template: &str) -> Result<Vec<String>> {
    let mut deps = HashSet::new();
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        rest = &rest[start + 2..];
        let Some(end) = rest.find("}}") else {
            bail!("unclosed template expression");
        };
        let expr = rest[..end].trim();
        rest = &rest[end + 2..];
        if expr.is_empty()
            || expr.starts_with('#')
            || expr.starts_with('/')
            || expr.starts_with('!')
        {
            continue;
        }
        let parts = expr.split_whitespace().collect::<Vec<_>>();
        let dep = match parts.as_slice() {
            [single] => *single,
            [helper, field] if matches!(*helper, "lower" | "upper" | "title" | "slug") => *field,
            _ => continue,
        };
        let dep = dep.trim_matches(|ch| ch == '"' || ch == '\'');
        if is_identifier(dep) {
            deps.insert(dep.to_string());
        }
    }
    let mut deps = deps.into_iter().collect::<Vec<_>>();
    deps.sort();
    Ok(deps)
}

fn is_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn parse_byte_size(value: &str) -> std::result::Result<u64, String> {
    let value = value.trim();
    let split = value
        .find(|c: char| c.is_ascii_alphabetic())
        .unwrap_or(value.len());
    let (num_str, unit_str) = value.split_at(split);
    let num: f64 = num_str
        .trim()
        .parse()
        .map_err(|_| format!("invalid byte size '{value}'"))?;
    if num < 0.0 {
        return Err(format!("byte size must be non-negative: '{value}'"));
    }
    let multiplier: f64 = match unit_str.trim().to_ascii_lowercase().as_str() {
        "" => 1.0,
        "k" | "kb" | "kib" => 1024.0,
        "m" | "mb" | "mib" => 1024.0 * 1024.0,
        "g" | "gb" | "gib" => 1024.0 * 1024.0 * 1024.0,
        other => return Err(format!("unknown byte unit '{other}' in '{value}'")),
    };
    let bytes = (num * multiplier).ceil();
    if bytes > u64::MAX as f64 {
        return Err(format!("byte size overflow in '{value}'"));
    }
    Ok(bytes as u64)
}

fn parse_duration(value: &str) -> std::result::Result<Duration, String> {
    let mut total = 0u64;
    let mut number = String::new();
    for ch in value.chars() {
        if ch.is_ascii_digit() {
            number.push(ch);
            continue;
        }
        if number.is_empty() {
            return Err(format!("invalid duration '{value}'"));
        }
        let parsed = number
            .parse::<u64>()
            .map_err(|_| format!("invalid duration '{value}'"))?;
        number.clear();
        match ch {
            'h' => total = total.saturating_add(parsed.saturating_mul(3600)),
            'm' => total = total.saturating_add(parsed.saturating_mul(60)),
            's' => total = total.saturating_add(parsed),
            _ => return Err(format!("invalid duration unit '{ch}' in '{value}'")),
        }
    }
    if !number.is_empty() {
        total = total.saturating_add(
            number
                .parse::<u64>()
                .map_err(|_| format!("invalid duration '{value}'"))?,
        );
    }
    if total == 0 {
        return Err("duration must be greater than zero".into());
    }
    Ok(Duration::from_secs(total))
}

fn should_emit_null(null_rate: Option<f64>, rng: &mut StdRng) -> Result<bool> {
    let Some(rate) = null_rate else {
        return Ok(false);
    };
    if !(0.0..=1.0).contains(&rate) {
        bail!("null_rate must be between 0.0 and 1.0");
    }
    Ok(rng.gen_bool(rate))
}

fn ensure_range<T>(min: T, max: T, name: &str) -> Result<()>
where
    T: PartialOrd + std::fmt::Display,
{
    if min > max {
        bail!("{name} min must be less than or equal to max");
    }
    Ok(())
}

fn ensure_float_range(min: f64, max: f64, name: &str) -> Result<()> {
    if !min.is_finite() || !max.is_finite() {
        bail!("{name} bounds must be finite");
    }
    ensure_range(min, max, name)
}

fn ensure_decimal_range(min: Decimal, max: Decimal, name: &str) -> Result<()> {
    if min > max {
        bail!("{name} min must be less than or equal to max");
    }
    Ok(())
}

fn parse_datetime(value: &str) -> Result<DateTime<Utc>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
        return Ok(dt.with_timezone(&Utc));
    }
    let naive = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
        .with_context(|| format!("invalid datetime '{value}'"))?;
    Ok(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
}

fn format_datetime(dt: DateTime<Utc>, format: &str) -> String {
    dt.format(format).to_string()
}

/// Render epoch microseconds with a strftime pattern. Only the text outputs
/// call this; the Parquet writer wants the number.
fn format_timestamp_micros(micros: i64, format: &str) -> String {
    match Utc.timestamp_micros(micros).single() {
        Some(dt) => format_datetime(dt, format),
        // Unreachable for values a generator produced, and a panic here would
        // take down a writer thread over one row.
        None => String::new(),
    }
}

/// Render a fixed-point decimal without building a `Decimal` to do it.
fn format_decimal_units(units: i128, scale: u32) -> String {
    if scale == 0 {
        return units.to_string();
    }
    let magnitude = units.unsigned_abs();
    let divisor = 10u128.pow(scale);
    format!(
        "{}{}.{:0width$}",
        if units < 0 { "-" } else { "" },
        magnitude / divisor,
        magnitude % divisor,
        width = scale as usize
    )
}

/// Convert a spec's decimal bound into unscaled units at `scale`, once, so the
/// per-row path is an integer draw.
fn decimal_to_units(value: Decimal, scale: u32, name: &str) -> Result<i128> {
    let factor = Decimal::from_i128_with_scale(10i128.pow(scale), 0);
    (value.round_dp(scale) * factor)
        .round()
        .to_i128()
        .ok_or_else(|| anyhow!("{} bound {} does not fit a 128-bit decimal", name, value))
}

/// `rust_decimal` carries at most this many fractional digits.
const MAX_DECIMAL_SCALE: u32 = 28;

fn seconds_to_micros(seconds: i64) -> Result<i64> {
    seconds
        .checked_mul(1_000_000)
        .ok_or_else(|| anyhow!("offset of {} seconds is too large to express in microseconds", seconds))
}

fn decimal_to_f64(value: Decimal) -> Result<f64> {
    value
        .to_string()
        .parse::<f64>()
        .context("failed to convert decimal to f64")
}

fn round_to_precision(value: f64, precision: usize) -> f64 {
    let factor = 10_f64.powi(precision as i32);
    (value * factor).round() / factor
}

fn title_case(value: &str) -> String {
    value
        .split_whitespace()
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                Some(first) => {
                    first.to_uppercase().collect::<String>() + &chars.as_str().to_ascii_lowercase()
                }
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn slug(value: &str) -> String {
    let mut output = String::new();
    let mut last_dash = false;
    for ch in value.chars().flat_map(char::to_lowercase) {
        if ch.is_ascii_alphanumeric() {
            output.push(ch);
            last_dash = false;
        } else if !last_dash && !output.is_empty() {
            output.push('-');
            last_dash = true;
        }
    }
    output.trim_matches('-').to_string()
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn char_to_single_byte(ch: char, name: &str) -> Result<u8> {
    if ch.len_utf8() != 1 {
        bail!("{name} must be a single-byte character");
    }
    Ok(ch as u8)
}

fn pick<'a>(values: &'a [&'a str], rng: &mut StdRng) -> &'a str {
    values[rng.gen_range(0..values.len())]
}

fn default_delimiter() -> char {
    ','
}

fn default_quote() -> char {
    '"'
}

fn default_escape() -> char {
    '"'
}

fn default_newline() -> String {
    "\n".to_string()
}

fn default_context_reset() -> ContextReset {
    ContextReset::Row
}

fn default_batch_rows() -> usize {
    1000
}

fn default_sequence_start() -> i64 {
    1
}

fn default_sequence_step() -> i64 {
    1
}

fn default_sequence_string_template() -> String {
    "{}".to_string()
}

fn default_sequence_string_start() -> u64 {
    1
}

fn default_sequence_string_step() -> u64 {
    1
}

/// 36 characters, the width of a hyphenated UUID, so swapping `uuid` for
/// `sequence_string` leaves column widths and file sizes where they were.
fn default_sequence_string_width() -> usize {
    36
}

/// Split `prefix{}suffix` into its two literal halves.
fn split_sequence_template(template: &str) -> Result<(String, String)> {
    let (prefix, rest) = template
        .split_once("{}")
        .ok_or_else(|| anyhow!("sequence_string template `{}` needs a `{{}}` placeholder", template))?;
    if rest.contains("{}") {
        bail!(
            "sequence_string template `{}` has more than one `{{}}` placeholder",
            template
        );
    }
    Ok((prefix.to_string(), rest.to_string()))
}

fn default_name_part() -> NamePart {
    NamePart::Full
}

fn default_words_min() -> usize {
    3
}

fn default_words_max() -> usize {
    12
}

fn default_address_part() -> AddressPart {
    AddressPart::Full
}

fn default_initial_direction() -> InitialDirection {
    InitialDirection::Up
}

fn default_datetime_format() -> String {
    "%Y-%m-%dT%H:%M:%S".to_string()
}

const FIRST_NAMES: &[&str] = &[
    "Adam",
    "Ava",
    "Benjamin",
    "Charlotte",
    "Daniel",
    "Evelyn",
    "Henry",
    "Liam",
    "Mia",
    "Sophia",
    "Zoe",
];

const LAST_NAMES: &[&str] = &[
    "Anderson", "Brown", "Chen", "Davis", "Garcia", "Johnson", "Miller", "Smith", "Taylor",
    "Wilson",
];

const EMAIL_DOMAINS: &[&str] = &["example.com", "example.net", "example.org"];
const STREET_NAMES: &[&str] = &[
    "Cedar", "Elm", "Harbor", "Maple", "Oak", "Pine", "River", "Sunset",
];
const STREET_SUFFIXES: &[&str] = &["Avenue", "Boulevard", "Drive", "Lane", "Road", "Street"];
const CITIES: &[&str] = &[
    "Singapore",
    "Tokyo",
    "Sydney",
    "San Francisco",
    "London",
    "Berlin",
];
const STATES: &[&str] = &["CA", "NY", "TX", "WA", "ON", "NSW"];
const COUNTRIES: &[&str] = &[
    "USA",
    "Singapore",
    "Japan",
    "Australia",
    "Germany",
    "United Kingdom",
];
const LOREM_WORDS: &[&str] = &[
    "alpha", "bravo", "cedar", "delta", "ember", "forest", "harbor", "indigo", "juniper", "kernel",
    "lunar", "matrix", "north", "orbit", "prairie", "quartz", "river", "signal", "thunder",
    "violet",
];

const SAMPLE_SPEC: &str = r#"version: 1

csv:
  delimiter: ","
  quote: "\""
  escape: "\""
  newline: "\n"
  null: ""

context:
  reset: row

batch:
  rows: 1000

fields:
  - name: id
    order: -100
    gen:
      type: sequence
      start: 1
      step: 1

  - name: email_domain
    hidden: true
    gen:
      type: choice
      values:
        - example.com
        - example.net
        - example.org

  - name: first_name
    gen:
      type: name
      part: first

  - name: last_name
    gen:
      type: name
      part: last

  - name: email
    gen:
      type: template
      value: "{{lower first_name}}.{{lower last_name}}.{{id}}@{{email_domain}}"

  - name: age
    gen:
      type: int_range
      min: 18
      max: 90

  - name: score
    gen:
      type: fluctuating
      data_type: int
      start: 50
      min: 1
      max: 100
      initial_direction: random
      step_min: 1
      step_max: 3
      flip_chance: 0.05

  - name: token
    gen:
      type: random_bytes
      min_bytes: 16
      max_bytes: 16
      encoding: base64url

  - name: created_at
    order: 100
    gen:
      type: datetime_around
      offset_seconds_min: -86400
      offset_seconds_max: 86400
      format: "%Y-%m-%d %H:%M:%S"

  - name: status
    order: 200
    gen:
      type: weighted_choice
      values:
        - value: active
          weight: 80
        - value: inactive
          weight: 15
        - value: blocked
          weight: 5

  - name: label
    order: 300
    gen:
      type: javascript
      function: function_a
      file: sample_script.js
      deps:
        - id
      # Declare only the fields your function actually reads from ctx.
      # Unlike the auto-deps behavior, explicit deps let other fields depend on this one.
"#;

// Sample JavaScript file generated alongside sample.yaml by --init.
const SAMPLE_JS: &str = r#"// Global counter — persists for the lifetime of this producer across all rows.
// Demonstrates that JavaScript globals are stateful between generate() calls.
var _global_seq = 0;

// function_a is called once per row. ctx contains all previously-generated
// fields for the current row (read-only from the Rust side).
function function_a(ctx) {
    _global_seq += 1;
    // Return a label that combines the row id from context with the call count.
    return "row-" + ctx.id + "/call-" + _global_seq;
}
"#;

fn preset_yaml(name: &str) -> Option<&'static str> {
    match name {
        "user" => Some(PRESET_USER),
        "order" => Some(PRESET_ORDER),
        "product" => Some(PRESET_PRODUCT),
        "event" => Some(PRESET_EVENT),
        _ => None,
    }
}

const PRESET_USER: &str = r#"version: 1
csv:
  delimiter: ","
  quote: "\""
  escape: "\""
  newline: "\n"
  null: ""
context:
  reset: row
batch:
  rows: 1000
fields:
  - name: user_id
    order: -100
    gen:
      type: sequence
      start: 1
      step: 1
  - name: _domain
    hidden: true
    gen:
      type: choice
      values: [example.com, example.net, example.org, mail.com]
  - name: first_name
    gen:
      type: name
      part: first
  - name: last_name
    gen:
      type: name
      part: last
  - name: email
    gen:
      type: template
      value: "{{lower first_name}}.{{lower last_name}}.{{user_id}}@{{_domain}}"
  - name: age
    gen:
      type: int_range
      min: 18
      max: 90
  - name: status
    gen:
      type: weighted_choice
      values:
        - value: active
          weight: 80
        - value: inactive
          weight: 15
        - value: blocked
          weight: 5
  - name: created_at
    order: 100
    gen:
      type: datetime_around
      offset_seconds_min: -31536000
      offset_seconds_max: 0
      format: "%Y-%m-%d %H:%M:%S"
"#;

const PRESET_ORDER: &str = r#"version: 1
csv:
  delimiter: ","
  quote: "\""
  escape: "\""
  newline: "\n"
  null: ""
context:
  reset: row
batch:
  rows: 1000
fields:
  - name: order_id
    gen:
      type: uuid
  - name: customer_id
    gen:
      type: int_range
      min: 1
      max: 100000
  - name: item
    gen:
      type: choice
      values:
        - Laptop
        - Headphones
        - Keyboard
        - Monitor
        - Mouse
        - Desk Chair
        - Webcam
        - USB Hub
  - name: quantity
    gen:
      type: int_range
      min: 1
      max: 20
  - name: unit_price
    gen:
      type: decimal_range
      min: "9.99"
      max: "999.99"
      scale: 2
  - name: status
    gen:
      type: weighted_choice
      values:
        - value: pending
          weight: 10
        - value: processing
          weight: 20
        - value: shipped
          weight: 30
        - value: delivered
          weight: 35
        - value: cancelled
          weight: 5
  - name: ordered_at
    order: 100
    gen:
      type: datetime_around
      offset_seconds_min: -7776000
      offset_seconds_max: 0
      format: "%Y-%m-%d %H:%M:%S"
"#;

const PRESET_PRODUCT: &str = r#"version: 1
csv:
  delimiter: ","
  quote: "\""
  escape: "\""
  newline: "\n"
  null: ""
context:
  reset: row
batch:
  rows: 1000
fields:
  - name: product_id
    gen:
      type: uuid
  - name: name
    gen:
      type: choice
      values:
        - Wireless Keyboard
        - Ergonomic Mouse
        - 4K Monitor
        - Noise-Cancelling Headphones
        - Standing Desk
        - Laptop Stand
        - USB-C Hub
        - Mechanical Keyboard
        - Gaming Chair
        - Webcam HD
  - name: category
    gen:
      type: choice
      values: [Electronics, Furniture, Accessories, Peripherals]
  - name: price
    gen:
      type: decimal_range
      min: "4.99"
      max: "1299.99"
      scale: 2
  - name: stock_qty
    gen:
      type: int_range
      min: 0
      max: 500
  - name: active
    gen:
      type: weighted_choice
      values:
        - value: "true"
          weight: 90
        - value: "false"
          weight: 10
  - name: updated_at
    order: 100
    gen:
      type: datetime_around
      offset_seconds_min: -2592000
      offset_seconds_max: 0
      format: "%Y-%m-%dT%H:%M:%SZ"
"#;

const PRESET_EVENT: &str = r#"version: 1
csv:
  delimiter: ","
  quote: "\""
  escape: "\""
  newline: "\n"
  null: ""
context:
  reset: row
batch:
  rows: 1000
fields:
  - name: event_id
    gen:
      type: uuid
  - name: event_type
    gen:
      type: weighted_choice
      values:
        - value: page_view
          weight: 40
        - value: click
          weight: 30
        - value: search
          weight: 15
        - value: add_to_cart
          weight: 8
        - value: purchase
          weight: 5
        - value: logout
          weight: 2
  - name: user_id
    gen:
      type: int_range
      min: 1
      max: 1000000
  - name: session_id
    gen:
      type: uuid
  - name: occurred_at
    gen:
      type: datetime_around
      offset_seconds_min: -86400
      offset_seconds_max: 0
      format: "%Y-%m-%dT%H:%M:%SZ"
  - name: payload
    gen:
      type: lorem
      words_min: 2
      words_max: 8
"#;

#[cfg(test)]
mod tests {
    use super::*;

    fn compile_yaml(yaml: &str) -> Result<CompiledSpec> {
        let raw = serde_yaml::from_str::<RawSpec>(yaml)?;
        compile_spec(raw)
    }

    /// Pull `count` values out of one generator instance.
    fn take_values(generator: &mut Generator, count: usize) -> Vec<String> {
        let mut rng = StdRng::seed_from_u64(7);
        let ctx = RowContext::new();
        (0..count)
            .map(|_| match generator.generate(&ctx, &mut rng).unwrap() {
                Value::String(value) => value,
                other => panic!("expected a string, got {:?}", other),
            })
            .collect()
    }

    #[test]
    fn renders_fixed_point_decimals() {
        assert_eq!(format_decimal_units(4512, 2), "45.12");
        assert_eq!(format_decimal_units(-4512, 2), "-45.12");
        // The fraction is zero-padded, not trimmed.
        assert_eq!(format_decimal_units(5, 2), "0.05");
        assert_eq!(format_decimal_units(70000, 4), "7.0000");
        assert_eq!(format_decimal_units(-5, 2), "-0.05");
        assert_eq!(format_decimal_units(42, 0), "42");
    }

    #[test]
    fn decimal_range_stays_in_bounds_at_the_declared_scale() {
        let mut generator =
            compile_gen("type: decimal_range\nmin: \"1.00\"\nmax: \"99.99\"\nscale: 2\n").unwrap();
        for value in take_raw_values(&mut generator, 500) {
            match value {
                Value::Decimal { units, scale } => {
                    assert_eq!(scale, 2);
                    assert!((100..=9999).contains(&units), "out of range: {}", units);
                }
                other => panic!("expected a decimal, got {:?}", other),
            }
        }
    }

    #[test]
    fn decimal_range_rejects_an_inverted_range() {
        let error = compile_gen_err(
            "type: decimal_range\nmin: \"9.00\"\nmax: \"1.00\"\nscale: 2\n",
            "min above max",
        );
        assert!(error.to_string().contains("min must be"), "{}", error);
    }

    #[test]
    fn datetime_around_keeps_the_configured_format() {
        let mut generator = compile_gen(
            "type: datetime_around\nbase: \"2020-06-15T12:00:00Z\"\noffset_seconds_min: 0\noffset_seconds_max: 0\nformat: \"%Y-%m-%d %H:%M:%S\"\n",
        )
        .unwrap();
        let values = take_raw_values(&mut generator, 2);
        assert!(matches!(values[0], Value::Timestamp { .. }));
        assert_eq!(values[0].csv_string(""), "2020-06-15 12:00:00");
    }

    /// The cached clock must not leak between fields: an explicit base always
    /// wins, and a generator without one still lands near now.
    #[test]
    fn datetime_around_without_a_base_tracks_the_clock() {
        let mut generator = compile_gen(
            "type: datetime_around\noffset_seconds_min: 0\noffset_seconds_max: 0\nformat: \"%Y-%m-%d %H:%M:%S\"\n",
        )
        .unwrap();
        let before = Utc::now().timestamp_micros();
        let values = take_raw_values(&mut generator, NOW_REFRESH_ROWS as usize + 10);
        let after = Utc::now().timestamp_micros();
        for value in values {
            match value {
                Value::Timestamp { micros, .. } => {
                    assert!(micros >= before && micros <= after, "{} out of window", micros);
                }
                other => panic!("expected a timestamp, got {:?}", other),
            }
        }
    }

    #[test]
    fn datetime_range_rejects_an_inverted_range() {
        let error = compile_gen_err(
            "type: datetime_range\nstart: \"2021-01-01T00:00:00Z\"\nend: \"2020-01-01T00:00:00Z\"\nformat: \"%Y-%m-%d\"\n",
            "start after end",
        );
        assert!(error.to_string().contains("min must be"), "{}", error);
    }

    /// Pull `count` values out of one generator instance, untouched.
    fn take_raw_values(generator: &mut Generator, count: usize) -> Vec<Value> {
        let mut rng = StdRng::seed_from_u64(11);
        let ctx = RowContext::new();
        (0..count)
            .map(|_| generator.generate(&ctx, &mut rng).unwrap())
            .collect()
    }

    use uuid::Uuid;

    fn compile_gen(yaml: &str) -> Result<Generator> {
        let spec = serde_yaml::from_str::<GeneratorSpec>(yaml)?;
        compile_generator(spec)
    }

    /// `Generator` is not `Debug`, so `expect_err` is not available here.
    fn compile_gen_err(yaml: &str, why: &str) -> anyhow::Error {
        match compile_gen(yaml) {
            Ok(_) => panic!("expected a failure: {}", why),
            Err(error) => error,
        }
    }

    #[test]
    fn sequence_string_pads_to_the_configured_width() {
        let mut generator = compile_gen("type: sequence_string\n").unwrap();
        let values = take_values(&mut generator, 3);
        assert_eq!(values[0].len(), 36, "{}", values[0]);
        assert_eq!(values[0], "0".repeat(35) + "1");
        assert_eq!(values[2], "0".repeat(35) + "3");
    }

    #[test]
    fn sequence_string_fills_a_template_to_uuid_shape() {
        let mut generator = compile_gen(
            "type: sequence_string\ntemplate: \"00000000-0000-4000-8000-{}\"\n",
        )
        .unwrap();
        let values = take_values(&mut generator, 2);
        assert_eq!(values[0], "00000000-0000-4000-8000-000000000001");
        assert_eq!(values[1], "00000000-0000-4000-8000-000000000002");
        assert!(values.iter().all(|value| value.len() == 36));
    }

    #[test]
    fn sequence_string_honours_start_and_step() {
        let mut generator =
            compile_gen("type: sequence_string\nstart: 10\nstep: 5\nwidth: 4\n")
                .unwrap();
        assert_eq!(take_values(&mut generator, 3), vec!["0010", "0015", "0020"]);
    }

    /// The point of the shared cursor: clones are what the generator threads
    /// get, and a key column cannot afford them repeating each other.
    #[test]
    fn sequence_string_clones_do_not_collide() {
        let generator = compile_gen("type: sequence_string\n").unwrap();
        let mut first = generator.clone();
        let mut second = generator.clone();
        // More than one block each, so the refill path is covered too.
        let count = (SEQUENCE_STRING_BLOCK as usize) + 100;
        let mut seen: HashSet<String> = take_values(&mut first, count).into_iter().collect();
        for value in take_values(&mut second, count) {
            assert!(seen.insert(value.clone()), "duplicate value {}", value);
        }
        assert_eq!(seen.len(), count * 2);
    }

    /// Zero-padding in Rust widens rather than truncates, so a counter that
    /// outgrows its width would silently start emitting 37-character values.
    #[test]
    fn sequence_string_refuses_to_outgrow_its_width() {
        let mut generator =
            compile_gen("type: sequence_string\nwidth: 4\nstart: 9998\n").unwrap();
        let mut rng = StdRng::seed_from_u64(3);
        let ctx = RowContext::new();
        for expected in ["9998", "9999"] {
            match generator.generate(&ctx, &mut rng).unwrap() {
                Value::String(value) => assert_eq!(value, expected),
                other => panic!("expected a string, got {:?}", other),
            }
        }
        let error = generator
            .generate(&ctx, &mut rng)
            .expect_err("10000 does not fit four digits");
        assert!(error.to_string().contains("no longer fits"), "{}", error);
    }

    /// The same ceiling, caught before a long run starts rather than partway
    /// through it.
    #[test]
    fn a_run_that_would_outgrow_the_width_is_refused_up_front() {
        let spec = compile_yaml(
            "version: 1\nfields:\n  - name: id\n    gen:\n      type: sequence_string\n      width: 4\n",
        )
        .unwrap();
        assert!(spec.check_sequence_capacity(Some(1_000), 1).is_ok());
        let error = spec
            .check_sequence_capacity(Some(100_000), 1)
            .expect_err("100k rows do not fit four digits");
        let text = error.to_string();
        assert!(text.contains("id") || format!("{:#}", error).contains("id"), "{:#}", error);
        // No row count to check against: the per-row guard is the backstop.
        assert!(spec.check_sequence_capacity(None, 1).is_ok());
    }

    /// 36 digits is what sample.spec uses, and it has to be beyond reach.
    #[test]
    fn a_full_width_counter_has_room_for_any_run() {
        let spec = compile_yaml(
            "version: 1\nfields:\n  - name: id\n    gen:\n      type: sequence_string\n      width: 36\n",
        )
        .unwrap();
        // 1e13 rows is roughly ten times the 40 TB this data compresses to.
        assert!(spec.check_sequence_capacity(Some(10_000_000_000_000), 64).is_ok());
    }

    #[test]
    fn sequence_string_rejects_a_template_that_does_not_fit() {
        let too_wide = compile_gen_err(
            "type: sequence_string\ntemplate: \"invoice-{}\"\nwidth: 4\n",
            "literal text is wider than the width",
        );
        assert!(too_wide.to_string().contains("does not fit"), "{}", too_wide);

        let no_placeholder = compile_gen_err(
            "type: sequence_string\ntemplate: \"invoice\"\n",
            "no placeholder",
        );
        assert!(
            no_placeholder.to_string().contains("placeholder"),
            "{}",
            no_placeholder
        );
    }

    fn generate_one_row(spec: &CompiledSpec) -> Result<RowContext> {
        let mut rng = StdRng::seed_from_u64(42);
        let mut fields = (*spec.fields).clone();
        let mut ctx = RowContext::new();
        for &field_index in spec.generation_order.iter() {
            let field = &mut fields[field_index];
            let value = field.generator.generate(&ctx, &mut rng)?;
            ctx.insert(field.name.clone(), value);
        }
        Ok(ctx)
    }

    #[test]
    fn parse_byte_size_variants() {
        assert_eq!(parse_byte_size("1000").unwrap(), 1000);
        assert_eq!(parse_byte_size("1k").unwrap(), 1024);
        assert_eq!(parse_byte_size("1K").unwrap(), 1024);
        assert_eq!(parse_byte_size("1KB").unwrap(), 1024);
        assert_eq!(parse_byte_size("1kb").unwrap(), 1024);
        assert_eq!(parse_byte_size("1KiB").unwrap(), 1024);
        assert_eq!(parse_byte_size("1kib").unwrap(), 1024);
        assert_eq!(parse_byte_size("1M").unwrap(), 1024 * 1024);
        assert_eq!(parse_byte_size("1MB").unwrap(), 1024 * 1024);
        assert_eq!(parse_byte_size("1MiB").unwrap(), 1024 * 1024);
        assert_eq!(parse_byte_size("1G").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_byte_size("1GB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_byte_size("1GiB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(
            parse_byte_size("23.3KB").unwrap(),
            (23.3f64 * 1024.0).ceil() as u64
        );
        assert_eq!(parse_byte_size("233KiB").unwrap(), 233 * 1024);
        assert!(parse_byte_size("abc").is_err());
        assert!(parse_byte_size("-1").is_err());
        assert!(parse_byte_size("1TB").is_err());
    }

    #[test]
    fn init_sample_spec_follows_current_schema() {
        // compile_spec reads the JS file from disk; write it to crate root (test cwd).
        let js_path = std::path::PathBuf::from("sample_script.js");
        std::fs::write(&js_path, SAMPLE_JS).expect("could not write sample_script.js for test");
        let result = compile_yaml(SAMPLE_SPEC);
        let _ = std::fs::remove_file(&js_path);
        result.expect("embedded sample spec should compile");
    }

    #[test]
    fn generates_each_generator_type() {
        let spec = compile_yaml(
            r#"
version: 1
fields:
  - name: constant_field
    gen:
      type: constant
      value: hello
  - name: sequence_field
    gen:
      type: sequence
      start: 7
      step: 2
  - name: name_field
    gen:
      type: name
      part: full
  - name: email_field
    gen:
      type: email
      style: random
  - name: lorem_field
    gen:
      type: lorem
      words_min: 2
      words_max: 4
  - name: address_field
    gen:
      type: address
      part: full
  - name: template_source
    hidden: true
    gen:
      type: constant
      value: Mixed Case
  - name: template_field
    gen:
      type: template
      value: "{{slug template_source}}"
  - name: int_range_field
    gen:
      type: int_range
      min: 1
      max: 3
  - name: float_range_field
    gen:
      type: float_range
      min: 1.0
      max: 3.0
      precision: 2
  - name: decimal_range_field
    gen:
      type: decimal_range
      min: "10.00"
      max: "20.00"
      scale: 2
  - name: fluctuating_field
    gen:
      type: fluctuating
      data_type: int
      start: 50
      min: 1
      max: 100
      initial_direction: up
      step_min: 1
      step_max: 2
      flip_chance: 0.0
  - name: datetime_around_now_field
    gen:
      type: datetime_around
      offset_seconds_min: -1
      offset_seconds_max: 1
      format: "%Y"
  - name: datetime_around_field
    gen:
      type: datetime_around
      base: "2020-01-01T00:00:00Z"
      format: "%Y-%m-%d"
  - name: datetime_range_field
    gen:
      type: datetime_range
      start: "2020-01-01T00:00:00Z"
      end: "2020-01-01T00:00:00Z"
      format: "%Y-%m-%d"
  - name: choice_field
    gen:
      type: choice
      values:
        - red
        - blue
  - name: weighted_choice_field
    gen:
      type: weighted_choice
      values:
        - value: only
          weight: 1
  - name: uuid_field
    gen:
      type: uuid
  - name: random_bytes_field
    gen:
      type: random_bytes
      min_bytes: 4
      max_bytes: 4
      encoding: hex
"#,
        )
        .expect("spec should compile");

        let row = generate_one_row(&spec).expect("row should generate");

        assert!(
            matches!(row.get("constant_field"), Some(Value::String(value)) if value == "hello")
        );
        assert!(matches!(row.get("sequence_field"), Some(Value::I64(7))));
        assert!(matches!(row.get("name_field"), Some(Value::String(value)) if value.contains(' ')));
        assert!(
            matches!(row.get("email_field"), Some(Value::String(value)) if value.contains('@'))
        );
        assert!(
            matches!(row.get("lorem_field"), Some(Value::String(value)) if value.split_whitespace().count() >= 2)
        );
        assert!(
            matches!(row.get("address_field"), Some(Value::String(value)) if value.contains(','))
        );
        assert!(
            matches!(row.get("template_field"), Some(Value::String(value)) if value == "mixed-case")
        );
        assert!(
            matches!(row.get("int_range_field"), Some(Value::I64(value)) if (1..=3).contains(value))
        );
        assert!(
            matches!(row.get("float_range_field"), Some(Value::F64(value)) if *value >= 1.0 && *value <= 3.0)
        );
        assert!(
            matches!(row.get("decimal_range_field"), Some(value @ Value::Decimal { .. }) if !value.csv_string("").is_empty())
        );
        assert!(
            matches!(row.get("fluctuating_field"), Some(Value::I64(value)) if *value >= 1 && *value <= 100)
        );
        assert!(
            matches!(row.get("datetime_around_now_field"), Some(value @ Value::Timestamp { .. }) if value.csv_string("").len() == 4)
        );
        assert!(
            matches!(row.get("datetime_around_field"), Some(value @ Value::Timestamp { .. }) if value.csv_string("") == "2020-01-01")
        );
        assert!(
            matches!(row.get("datetime_range_field"), Some(value @ Value::Timestamp { .. }) if value.csv_string("") == "2020-01-01")
        );
        assert!(
            matches!(row.get("choice_field"), Some(Value::String(value)) if value == "red" || value == "blue")
        );
        assert!(
            matches!(row.get("weighted_choice_field"), Some(Value::String(value)) if value == "only")
        );
        assert!(
            matches!(row.get("uuid_field"), Some(Value::String(value)) if Uuid::parse_str(value).is_ok())
        );
        assert!(
            matches!(row.get("random_bytes_field"), Some(Value::String(value)) if value.len() == 8)
        );
    }

    #[test]
    fn rejects_dependency_cycles() {
        let result = compile_yaml(
            r#"
version: 1
fields:
  - name: field_a
    gen:
      type: template
      value: "{{field_b}}"
  - name: field_b
    gen:
      type: template
      value: "{{field_a}}"
"#,
        );

        let message = format!("{:#}", result.err().expect("cycle should fail validation"));
        assert!(message.contains("DAG cannot be satisfied"));
    }

    #[test]
    fn supports_hidden_later_field_references() {
        let spec = compile_yaml(
            r#"
version: 1
fields:
  - name: visible
    gen:
      type: template
      value: "{{hidden_value}}"
  - name: hidden_value
    hidden: true
    gen:
      type: constant
      value: ok
"#,
        )
        .expect("spec should compile");

        assert_eq!(spec.output_order.len(), 1);
        assert_eq!(spec.fields[spec.output_order[0]].name, "visible");
        let generation_names = spec
            .generation_order
            .iter()
            .map(|&index| spec.fields[index].name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(generation_names, vec!["hidden_value", "visible"]);
    }

    #[test]
    fn fuzzy_output_order_uses_i64_default_zero() {
        let spec = compile_yaml(
            r#"
version: 1
fields:
  - name: default_a
    gen:
      type: constant
      value: a
  - name: early
    order: -1
    gen:
      type: constant
      value: early
  - name: default_b
    gen:
      type: constant
      value: b
  - name: late
    order: 1
    gen:
      type: constant
      value: late
"#,
        )
        .expect("spec should compile");

        let output_names = spec
            .output_order
            .iter()
            .map(|&index| spec.fields[index].name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            output_names,
            vec!["early", "default_a", "default_b", "late"]
        );
    }

    #[test]
    fn derived_spec_from_doris_schema_compiles_and_generates() {
        let sql = r#"
            CREATE TABLE IF NOT EXISTS tracking_db.vehicle
            (
                `vin`        CHAR(17)       NOT NULL,
                `id`         BIGINT         NOT NULL,
                `email`      VARCHAR(255),
                `speed`      DOUBLE,
                `amount`     DECIMAL(20, 4),
                `day`        DATE,
                `ts`         DATETIME(3),
                `notes`      STRING,
                `flag`       BOOLEAN
            )
            ENGINE=OLAP
            UNIQUE KEY(`vin`)
            DISTRIBUTED BY HASH(`vin`) BUCKETS 32
            PROPERTIES ("replication_num" = "1");
        "#;

        let tables = schema::parse_schema(sql).expect("parse schema");
        let table = schema::select_table(tables, None).expect("select table");
        let yaml = derive::spec_yaml_from_table(&table).expect("derive spec");

        let spec = compile_yaml(&yaml).expect("derived spec must compile");
        assert_eq!(spec.fields.len(), table.columns.len());

        let row = generate_one_row(&spec).expect("generate row");
        for column in &table.columns {
            let value = row
                .get(&column.name)
                .unwrap_or_else(|| panic!("missing generated value for `{}`", column.name));
            assert!(
                !matches!(value, Value::Null),
                "column `{}` generated null unexpectedly",
                column.name
            );
        }

        // A CHAR(17) column must never produce more characters than declared.
        let vin = row.get("vin").unwrap().csv_string("");
        assert!(vin.len() <= 17, "vin `{}` exceeds CHAR(17)", vin);

        // Output column order must follow the DDL.
        let names: Vec<&str> = spec
            .output_order
            .iter()
            .map(|&index| spec.fields[index].name.as_str())
            .collect();
        let expected: Vec<&str> = table.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, expected, "output order must match DDL order");
    }

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("dpsg-{}-{}", tag, std::process::id()));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    #[test]
    fn emit_spec_never_clobbers_an_existing_file() {
        let dir = temp_dir("emit");
        let path = dir.join("spec.yaml");

        write_emitted_spec(&path, "version: 1\n").expect("first write");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "version: 1\n");

        let err = write_emitted_spec(&path, "version: 2\n")
            .expect_err("second write must fail")
            .to_string();
        assert!(err.contains("already exists"), "unexpected error: {}", err);
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "version: 1\n",
            "existing spec must survive"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn schema_file_becomes_a_compilable_spec() {
        let dir = temp_dir("schema");
        let path = dir.join("schema.sql");
        std::fs::write(
            &path,
            "CREATE TABLE db.a (id BIGINT NOT NULL, vin CHAR(17)) ENGINE=OLAP;\n\
             CREATE TABLE db.b (x INT) ENGINE=OLAP;",
        )
        .expect("write schema");

        // Two tables, so an explicit choice is required.
        assert!(load_schema_table(&path, None).is_err());

        let table = load_schema_table(&path, Some("a")).expect("select table a");
        let yaml = derive::spec_yaml_from_table(&table).expect("derive spec");
        let spec = compile_yaml(&yaml).expect("derived spec compiles");
        assert_eq!(spec.fields.len(), 2);

        // A spec covering every column passes validation; a short one fails.
        validate_spec_covers(&spec, &table.columns).expect("derived spec covers the table");
        let partial = compile_yaml("version: 1\nfields:\n  - name: id\n    gen:\n      type: uuid\n")
            .expect("compiles");
        assert!(validate_spec_covers(&partial, &table.columns).is_err());

        assert!(load_schema_table(&path, Some("nope")).is_err(), "unknown table must fail");

        std::fs::remove_dir_all(&dir).ok();
    }
}
