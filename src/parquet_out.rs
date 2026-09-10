//! Doris types to Arrow, and generated values to Arrow arrays.
//!
//! Values arrive from the generators as the loose `Value` enum, mostly
//! strings. This module converts them to the native Parquet type implied by
//! the Doris column, so Doris reads the files back without casting.

use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, Int8Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use chrono::{NaiveDate, NaiveDateTime};
use std::sync::Arc;

use crate::schema::{Column, DorisType};
use crate::Value;

/// Doris LARGEINT is a 128-bit integer, carried as DECIMAL(38,0) in Parquet.
const LARGEINT_PRECISION: u8 = 38;

pub fn arrow_type(ty: &DorisType) -> Result<DataType> {
    let mapped = match ty {
        DorisType::Boolean => DataType::Boolean,
        DorisType::TinyInt => DataType::Int8,
        DorisType::SmallInt => DataType::Int16,
        DorisType::Int => DataType::Int32,
        DorisType::BigInt => DataType::Int64,
        DorisType::LargeInt => DataType::Decimal128(LARGEINT_PRECISION, 0),
        DorisType::Float => DataType::Float32,
        DorisType::Double => DataType::Float64,
        DorisType::Decimal { precision, scale } => DataType::Decimal128(*precision, *scale as i8),
        DorisType::Date => DataType::Date32,
        // Microseconds covers every DATETIME scale Doris allows.
        DorisType::DateTime { .. } => DataType::Timestamp(TimeUnit::Microsecond, None),
        DorisType::Char { .. } | DorisType::Varchar { .. } | DorisType::String => DataType::Utf8,
        DorisType::Json | DorisType::Variant => DataType::Utf8,
        other => bail!("cannot write Doris type {:?} to Parquet yet", other),
    };
    Ok(mapped)
}

pub fn arrow_schema(columns: &[Column]) -> Result<SchemaRef> {
    let fields = columns
        .iter()
        .map(|column| {
            arrow_type(&column.ty)
                .map(|ty| Field::new(&column.name, ty, column.nullable))
                .with_context(|| format!("column `{}`", column.name))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

/// One typed accumulator per column.
enum ColumnBuilder {
    Boolean(BooleanBuilder),
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Decimal { builder: Decimal128Builder, precision: u8, scale: u8 },
    Date32(Date32Builder),
    Timestamp(TimestampMicrosecondBuilder),
    Utf8(StringBuilder),
}

impl ColumnBuilder {
    fn new(ty: &DorisType, capacity: usize) -> Result<Self> {
        let builder = match ty {
            DorisType::Boolean => ColumnBuilder::Boolean(BooleanBuilder::with_capacity(capacity)),
            DorisType::TinyInt => ColumnBuilder::Int8(Int8Builder::with_capacity(capacity)),
            DorisType::SmallInt => ColumnBuilder::Int16(Int16Builder::with_capacity(capacity)),
            DorisType::Int => ColumnBuilder::Int32(Int32Builder::with_capacity(capacity)),
            DorisType::BigInt => ColumnBuilder::Int64(Int64Builder::with_capacity(capacity)),
            DorisType::LargeInt => ColumnBuilder::Decimal {
                builder: Decimal128Builder::with_capacity(capacity),
                precision: LARGEINT_PRECISION,
                scale: 0,
            },
            DorisType::Float => ColumnBuilder::Float32(Float32Builder::with_capacity(capacity)),
            DorisType::Double => ColumnBuilder::Float64(Float64Builder::with_capacity(capacity)),
            DorisType::Decimal { precision, scale } => ColumnBuilder::Decimal {
                builder: Decimal128Builder::with_capacity(capacity),
                precision: *precision,
                scale: *scale,
            },
            DorisType::Date => ColumnBuilder::Date32(Date32Builder::with_capacity(capacity)),
            DorisType::DateTime { .. } => {
                ColumnBuilder::Timestamp(TimestampMicrosecondBuilder::with_capacity(capacity))
            }
            DorisType::Char { .. } | DorisType::Varchar { .. } | DorisType::String => {
                ColumnBuilder::Utf8(StringBuilder::with_capacity(capacity, capacity * 16))
            }
            DorisType::Json | DorisType::Variant => {
                ColumnBuilder::Utf8(StringBuilder::with_capacity(capacity, capacity * 16))
            }
            other => bail!("cannot write Doris type {:?} to Parquet yet", other),
        };
        Ok(builder)
    }

    fn append(&mut self, value: &Value) -> Result<()> {
        if matches!(value, Value::Null) {
            self.append_null();
            return Ok(());
        }
        match self {
            ColumnBuilder::Boolean(builder) => builder.append_value(as_bool(value)?),
            ColumnBuilder::Int8(builder) => builder.append_value(as_int(value, i8::MIN as i128, i8::MAX as i128)? as i8),
            ColumnBuilder::Int16(builder) => builder.append_value(as_int(value, i16::MIN as i128, i16::MAX as i128)? as i16),
            ColumnBuilder::Int32(builder) => builder.append_value(as_int(value, i32::MIN as i128, i32::MAX as i128)? as i32),
            ColumnBuilder::Int64(builder) => builder.append_value(as_int(value, i64::MIN as i128, i64::MAX as i128)? as i64),
            ColumnBuilder::Float32(builder) => builder.append_value(as_f64(value)? as f32),
            ColumnBuilder::Float64(builder) => builder.append_value(as_f64(value)?),
            ColumnBuilder::Decimal { builder, precision, scale } => {
                builder.append_value(as_decimal(value, *precision, *scale)?)
            }
            ColumnBuilder::Date32(builder) => builder.append_value(as_date(value)?),
            ColumnBuilder::Timestamp(builder) => builder.append_value(as_timestamp_micros(value)?),
            ColumnBuilder::Utf8(builder) => builder.append_value(as_str(value)),
        }
        Ok(())
    }

    fn append_null(&mut self) {
        match self {
            ColumnBuilder::Boolean(builder) => builder.append_null(),
            ColumnBuilder::Int8(builder) => builder.append_null(),
            ColumnBuilder::Int16(builder) => builder.append_null(),
            ColumnBuilder::Int32(builder) => builder.append_null(),
            ColumnBuilder::Int64(builder) => builder.append_null(),
            ColumnBuilder::Float32(builder) => builder.append_null(),
            ColumnBuilder::Float64(builder) => builder.append_null(),
            ColumnBuilder::Decimal { builder, .. } => builder.append_null(),
            ColumnBuilder::Date32(builder) => builder.append_null(),
            ColumnBuilder::Timestamp(builder) => builder.append_null(),
            ColumnBuilder::Utf8(builder) => builder.append_null(),
        }
    }

    fn finish(&mut self) -> Result<ArrayRef> {
        let array: ArrayRef = match self {
            ColumnBuilder::Boolean(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Int8(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Int16(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Int32(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Int64(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Float32(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Float64(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Decimal { builder, precision, scale } => Arc::new(
                builder
                    .finish()
                    .with_precision_and_scale(*precision, *scale as i8)?,
            ),
            ColumnBuilder::Date32(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Timestamp(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Utf8(builder) => Arc::new(builder.finish()),
        };
        Ok(array)
    }
}

fn as_str(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Bool(flag) => flag.to_string(),
        Value::I64(number) => number.to_string(),
        Value::F64(number) => number.to_string(),
        Value::Timestamp { .. } | Value::Decimal { .. } => value.csv_string(""),
        Value::Null => String::new(),
    }
}

fn as_bool(value: &Value) -> Result<bool> {
    match value {
        Value::Bool(flag) => Ok(*flag),
        Value::I64(number) => Ok(*number != 0),
        Value::String(text) => match text.trim().to_ascii_lowercase().as_str() {
            "true" | "t" | "1" | "yes" => Ok(true),
            "false" | "f" | "0" | "no" => Ok(false),
            other => bail!("`{}` is not a boolean", other),
        },
        Value::Decimal { units, .. } => Ok(*units != 0),
        other => bail!("cannot read a boolean from {:?}", other),
    }
}

fn as_int(value: &Value, min: i128, max: i128) -> Result<i128> {
    let number = match value {
        Value::I64(number) => *number as i128,
        Value::Bool(flag) => *flag as i128,
        Value::F64(number) => number.trunc() as i128,
        Value::String(text) => text
            .trim()
            .parse::<i128>()
            .map_err(|_| anyhow!("`{}` is not an integer", text))?,
        // Truncates towards zero, matching the F64 arm above.
        Value::Decimal { units, scale } => units / 10i128.pow(*scale),
        Value::Timestamp { .. } => bail!("cannot read an integer from a timestamp"),
        Value::Null => unreachable!("nulls are handled before conversion"),
    };
    if number < min || number > max {
        bail!("{} does not fit the column's integer range", number);
    }
    Ok(number)
}

fn as_f64(value: &Value) -> Result<f64> {
    match value {
        Value::F64(number) => Ok(*number),
        Value::I64(number) => Ok(*number as f64),
        Value::String(text) => text
            .trim()
            .parse::<f64>()
            .map_err(|_| anyhow!("`{}` is not a number", text)),
        Value::Decimal { units, scale } => Ok(*units as f64 / 10f64.powi(*scale as i32)),
        other => bail!("cannot read a number from {:?}", other),
    }
}

/// Parse a decimal into its unscaled 128-bit form at the column's scale.
fn as_decimal(value: &Value, precision: u8, scale: u8) -> Result<i128> {
    // A generator that already knows the number hands it over directly. Only
    // literals from a spec file take the text path below.
    if let Value::Decimal {
        units,
        scale: value_scale,
    } = value
    {
        return rescale_decimal(*units, *value_scale, precision, scale);
    }
    let text = match value {
        Value::String(text) => text.trim().to_string(),
        Value::I64(number) => number.to_string(),
        Value::F64(number) => format!("{}", number),
        other => bail!("cannot read a decimal from {:?}", other),
    };

    let (negative, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(&text)),
    };

    let (whole, fraction) = match digits.split_once('.') {
        Some((whole, fraction)) => (whole, fraction),
        None => (digits, ""),
    };
    if whole.is_empty() && fraction.is_empty() {
        bail!("`{}` is not a decimal", text);
    }
    if !whole.chars().all(|ch| ch.is_ascii_digit())
        || !fraction.chars().all(|ch| ch.is_ascii_digit())
    {
        bail!("`{}` is not a decimal", text);
    }
    // Refuse to silently drop digits the column cannot hold.
    if fraction.len() > scale as usize {
        bail!(
            "`{}` has {} decimal places but the column holds {}",
            text,
            fraction.len(),
            scale
        );
    }

    let padded = format!("{}{:0<width$}", whole, fraction, width = scale as usize);
    let unscaled: i128 = padded
        .parse()
        .map_err(|_| anyhow!("`{}` does not fit a 128-bit decimal", text))?;

    let limit = 10i128
        .checked_pow(precision as u32)
        .ok_or_else(|| anyhow!("precision {} is too large", precision))?;
    if unscaled >= limit {
        bail!("`{}` exceeds the column's precision of {}", text, precision);
    }
    Ok(if negative { -unscaled } else { unscaled })
}

/// Move an unscaled decimal from `from_scale` to the column's scale,
/// refusing anything that would drop digits or overflow the precision.
fn rescale_decimal(units: i128, from_scale: u32, precision: u8, scale: u8) -> Result<i128> {
    let target = scale as u32;
    let unscaled = if from_scale == target {
        units
    } else if from_scale < target {
        units
            .checked_mul(10i128.pow(target - from_scale))
            .ok_or_else(|| anyhow!("decimal does not fit a 128-bit value"))?
    } else {
        let divisor = 10i128.pow(from_scale - target);
        if units % divisor != 0 {
            bail!(
                "`{}` has {} decimal places but the column holds {}",
                format_units(units, from_scale),
                from_scale,
                scale
            );
        }
        units / divisor
    };
    let limit = 10i128
        .checked_pow(precision as u32)
        .ok_or_else(|| anyhow!("precision {} is too large", precision))?;
    if unscaled.unsigned_abs() >= limit.unsigned_abs() {
        bail!(
            "`{}` exceeds the column's precision of {}",
            format_units(units, from_scale),
            precision
        );
    }
    Ok(unscaled)
}

/// Only used to name a value in an error message.
fn format_units(units: i128, scale: u32) -> String {
    Value::Decimal { units, scale }.csv_string("")
}

fn as_date(value: &Value) -> Result<i32> {
    if let Value::Timestamp { micros, .. } = value {
        return Ok(micros.div_euclid(MICROS_PER_DAY) as i32);
    }
    let text = as_str(value);
    let trimmed = text.trim();
    // Accept a full timestamp and keep only the date part.
    let date_part = trimmed.split(['T', ' ']).next().unwrap_or(trimmed);
    let date = NaiveDate::parse_from_str(date_part, "%Y-%m-%d")
        .map_err(|_| anyhow!("`{}` is not a date (expected YYYY-MM-DD)", trimmed))?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch");
    Ok((date - epoch).num_days() as i32)
}

const MICROS_PER_DAY: i64 = 24 * 60 * 60 * 1_000_000;

fn as_timestamp_micros(value: &Value) -> Result<i64> {
    // The whole point of Value::Timestamp: no format, no parse.
    if let Value::Timestamp { micros, .. } = value {
        return Ok(*micros);
    }
    let text = as_str(value);
    let trimmed = text.trim();
    const FORMATS: [&str; 4] = [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ];
    for format in FORMATS {
        if let Ok(parsed) = NaiveDateTime::parse_from_str(trimmed, format) {
            return parsed
                .and_utc()
                .timestamp_micros()
                .checked_abs()
                .map(|_| parsed.and_utc().timestamp_micros())
                .ok_or_else(|| anyhow!("`{}` is out of timestamp range", trimmed));
        }
    }
    // A bare date is a valid timestamp at midnight.
    if let Ok(date) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        return Ok(date
            .and_hms_opt(0, 0, 0)
            .expect("midnight is valid")
            .and_utc()
            .timestamp_micros());
    }
    bail!("`{}` is not a timestamp", trimmed)
}

/// Accumulates generated rows and turns them into Arrow record batches.
pub struct BatchBuilder {
    schema: SchemaRef,
    columns: Vec<Column>,
    builders: Vec<ColumnBuilder>,
    rows: usize,
    capacity: usize,
}

impl BatchBuilder {
    pub fn new(columns: Vec<Column>, capacity: usize) -> Result<Self> {
        let schema = arrow_schema(&columns)?;
        let builders = columns
            .iter()
            .map(|column| ColumnBuilder::new(&column.ty, capacity))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            schema,
            columns,
            builders,
            rows: 0,
            capacity,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Append one row, taking each column's value from `lookup`.
    pub fn append_row<'a, F>(&mut self, lookup: F) -> Result<()>
    where
        F: Fn(&str) -> Option<&'a Value>,
    {
        for (index, column) in self.columns.iter().enumerate() {
            let value = lookup(&column.name).ok_or_else(|| {
                anyhow!("no generated value for column `{}`", column.name)
            })?;
            self.builders[index]
                .append(value)
                .with_context(|| format!("column `{}`", column.name))?;
        }
        self.rows += 1;
        Ok(())
    }

    /// Take the accumulated rows as a record batch, resetting the builder.
    pub fn finish(&mut self) -> Result<arrow::record_batch::RecordBatch> {
        let arrays = self
            .builders
            .iter_mut()
            .map(|builder| builder.finish())
            .collect::<Result<Vec<_>>>()?;
        let batch = arrow::record_batch::RecordBatch::try_new(self.schema.clone(), arrays)?;
        self.rows = 0;
        // Builders reset themselves on finish, but capacity is not retained.
        for (index, column) in self.columns.iter().enumerate() {
            self.builders[index] = ColumnBuilder::new(&column.ty, self.capacity)?;
        }
        Ok(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::parse_schema;

    fn column(sql_type: &str) -> Column {
        let sql = format!("CREATE TABLE t (c {}) ENGINE=OLAP", sql_type);
        parse_schema(&sql).expect("parse").remove(0).columns.remove(0)
    }

    #[test]
    fn maps_doris_types_to_arrow() {
        assert_eq!(arrow_type(&column("BOOLEAN").ty).unwrap(), DataType::Boolean);
        assert_eq!(arrow_type(&column("INT").ty).unwrap(), DataType::Int32);
        assert_eq!(arrow_type(&column("BIGINT").ty).unwrap(), DataType::Int64);
        assert_eq!(
            arrow_type(&column("LARGEINT").ty).unwrap(),
            DataType::Decimal128(38, 0)
        );
        assert_eq!(
            arrow_type(&column("DECIMAL(19,4)").ty).unwrap(),
            DataType::Decimal128(19, 4)
        );
        assert_eq!(arrow_type(&column("DATE").ty).unwrap(), DataType::Date32);
        assert_eq!(
            arrow_type(&column("DATETIME(6)").ty).unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(arrow_type(&column("VARCHAR(256)").ty).unwrap(), DataType::Utf8);
    }

    #[test]
    fn schema_carries_nullability_from_the_ddl() {
        let tables = parse_schema("CREATE TABLE t (a INT NOT NULL, b INT) ENGINE=OLAP").unwrap();
        let schema = arrow_schema(&tables[0].columns).unwrap();
        assert!(!schema.field(0).is_nullable());
        assert!(schema.field(1).is_nullable());
    }

    /// The typed fast path exists only as an optimisation, so it has to agree
    /// with parsing the text the same value renders to.
    #[test]
    fn typed_decimals_agree_with_the_text_path() {
        for (units, scale) in [(451_200i128, 4u32), (-451_200, 4), (10_000, 4), (7, 4)] {
            let typed = Value::Decimal { units, scale };
            let text = Value::String(typed.csv_string(""));
            assert_eq!(
                as_decimal(&typed, 19, 4).unwrap(),
                as_decimal(&text, 19, 4).unwrap(),
                "disagreement on {:?}",
                typed
            );
        }
        // A coarser value than the column scales up, exactly as the text does.
        let coarse = Value::Decimal {
            units: 4512,
            scale: 2,
        };
        assert_eq!(as_decimal(&coarse, 19, 4).unwrap(), 451_200);
    }

    #[test]
    fn typed_decimals_refuse_to_drop_digits_or_overflow() {
        // 0.123456 at scale 6 does not fit a scale-4 column.
        let too_precise = Value::Decimal {
            units: 123_456,
            scale: 6,
        };
        assert!(as_decimal(&too_precise, 19, 4).is_err());
        // 12345.60 does not fit precision 5.
        let too_large = Value::Decimal {
            units: 1_234_560,
            scale: 2,
        };
        assert!(as_decimal(&too_large, 5, 2).is_err());
    }

    #[test]
    fn typed_timestamps_agree_with_the_text_path() {
        let format: Arc<str> = Arc::from("%Y-%m-%d %H:%M:%S%.6f");
        for micros in [1_757_500_000_123_456i64, 0, -86_400_000_000] {
            let typed = Value::Timestamp {
                micros,
                format: format.clone(),
            };
            let text = Value::String(typed.csv_string(""));
            assert_eq!(as_timestamp_micros(&typed).unwrap(), micros);
            assert_eq!(as_timestamp_micros(&text).unwrap(), micros);
            assert_eq!(
                as_date(&typed).unwrap(),
                as_date(&text).unwrap(),
                "date disagreement at {}",
                micros
            );
        }
    }

    #[test]
    fn parses_decimals_at_the_column_scale() {
        // Two decimal places into a scale-4 column pads with zeros.
        assert_eq!(as_decimal(&Value::String("45.12".into()), 19, 4).unwrap(), 451_200);
        assert_eq!(as_decimal(&Value::String("1.00".into()), 19, 4).unwrap(), 10_000);
        assert_eq!(as_decimal(&Value::String("-3.5".into()), 19, 4).unwrap(), -35_000);
        assert_eq!(as_decimal(&Value::String("7".into()), 19, 4).unwrap(), 70_000);
    }

    #[test]
    fn refuses_decimals_that_would_lose_data() {
        let too_precise = as_decimal(&Value::String("1.234567".into()), 19, 4);
        assert!(too_precise.is_err(), "excess decimal places must not be truncated");

        let too_large = as_decimal(&Value::String("12345.6".into()), 5, 2);
        assert!(too_large.is_err(), "value must fit the declared precision");

        assert!(as_decimal(&Value::String("abc".into()), 19, 4).is_err());
    }

    #[test]
    fn parses_dates_and_timestamps() {
        let epoch = as_date(&Value::String("1970-01-01".into())).unwrap();
        assert_eq!(epoch, 0);
        assert_eq!(as_date(&Value::String("1970-01-02".into())).unwrap(), 1);
        // A timestamp is accepted where a date is wanted.
        assert_eq!(as_date(&Value::String("1970-01-02 13:45:00".into())).unwrap(), 1);

        assert_eq!(
            as_timestamp_micros(&Value::String("1970-01-01 00:00:01".into())).unwrap(),
            1_000_000
        );
        // Microsecond precision survives, matching datetime(6).
        assert_eq!(
            as_timestamp_micros(&Value::String("1970-01-01 00:00:00.123456".into())).unwrap(),
            123_456
        );
        assert!(as_timestamp_micros(&Value::String("not a time".into())).is_err());
    }

    #[test]
    fn integer_range_is_enforced_per_column_width() {
        assert!(as_int(&Value::I64(200), i8::MIN as i128, i8::MAX as i128).is_err());
        assert!(as_int(&Value::I64(100), i8::MIN as i128, i8::MAX as i128).is_ok());
    }

    #[test]
    fn builds_a_record_batch_from_generated_values() {
        let tables = parse_schema(
            "CREATE TABLE t (id BIGINT NOT NULL, amount DECIMAL(19,4), ts DATETIME(6), note VARCHAR(64)) ENGINE=OLAP",
        )
        .unwrap();
        let columns = tables[0].columns.clone();
        let mut builder = BatchBuilder::new(columns, 8).unwrap();

        let row: Vec<(String, Value)> = vec![
            ("id".into(), Value::I64(7)),
            ("amount".into(), Value::String("45.12".into())),
            ("ts".into(), Value::String("2026-09-10 12:00:00.500000".into())),
            ("note".into(), Value::String("hello".into())),
        ];
        builder
            .append_row(|name| row.iter().find(|(key, _)| key == name).map(|(_, value)| value))
            .unwrap();

        // A null in a nullable column is fine.
        let with_null: Vec<(String, Value)> = vec![
            ("id".into(), Value::I64(8)),
            ("amount".into(), Value::Null),
            ("ts".into(), Value::Null),
            ("note".into(), Value::Null),
        ];
        builder
            .append_row(|name| {
                with_null.iter().find(|(key, _)| key == name).map(|(_, value)| value)
            })
            .unwrap();

        assert_eq!(builder.rows(), 2);
        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4);
        assert!(builder.is_empty(), "builder resets after finish");
    }

    #[test]
    fn missing_column_value_is_an_error() {
        let tables = parse_schema("CREATE TABLE t (a INT, b INT) ENGINE=OLAP").unwrap();
        let mut builder = BatchBuilder::new(tables[0].columns.clone(), 4).unwrap();
        let row = vec![("a".to_string(), Value::I64(1))];
        let err = builder
            .append_row(|name| row.iter().find(|(key, _)| key == name).map(|(_, value)| value))
            .expect_err("column b is missing");
        assert!(err.to_string().contains('b'), "{}", err);
    }
}

#[cfg(test)]
mod inspect {
    /// Prints the schema and first rows of a generated file, for eyeballing.
    #[test]
    #[ignore]
    fn dump() {
        let path = std::env::var("DUMP_PARQUET").expect("set DUMP_PARQUET");
        let file = std::fs::File::open(&path).expect("open");
        let builder =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let meta = builder.metadata().clone();
        println!("rows: {}", meta.file_metadata().num_rows());
        println!("row groups: {}", meta.num_row_groups());
        println!("compression: {:?}", meta.row_group(0).column(0).compression());
        println!("\nschema:");
        for field in builder.schema().fields() {
            println!(
                "  {:22} {:34} nullable={}",
                field.name(),
                format!("{:?}", field.data_type()),
                field.is_nullable()
            );
        }
        let mut reader = builder.with_batch_size(2).build().unwrap();
        let batch = reader.next().unwrap().unwrap();
        println!("\nfirst row:");
        for (index, field) in batch.schema().fields().iter().enumerate() {
            let column = batch.column(index);
            println!("  {:22} {}", field.name(), arrow::util::display::array_value_to_string(column, 0).unwrap());
        }
    }
}
