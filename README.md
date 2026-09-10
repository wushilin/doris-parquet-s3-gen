# doris-parquet-s3-gen

Generate synthetic data from a Doris `CREATE TABLE` statement and stream it to
S3 as Parquet. Nothing is staged on disk, and no whole file is held in memory.

```sh
cargo build --release

# 1. derive an editable field spec from your DDL
./target/release/doris-parquet-s3-gen --schema schema.sql --emit-spec spec.yaml

# 2. write a starter S3 config
./target/release/doris-parquet-s3-gen --emit-s3-config s3.toml

# 3. generate
./target/release/doris-parquet-s3-gen \
  --schema schema.sql --spec spec.yaml --s3-config s3.toml \
  --target-size 10GiB --file-size 2GiB
```

`./run.sh` wraps step 3 with preflight checks. `./run.sh --help` lists options.

## How it works

The Doris DDL supplies column types, order and nullability. The spec supplies
the generator for each column. Types map to native Parquet, so Doris reads the
files back without casting:

| Doris | Parquet |
|---|---|
| `datetime(6)` | `Timestamp(Microsecond)` |
| `decimal(19,4)` | `Decimal128(19,4)` |
| `largeint` | `Decimal128(38,0)` |
| `bigint` / `int` / `smallint` / `tinyint` | `Int64` / `Int32` / `Int16` / `Int8` |
| `date` | `Date32` |
| `varchar` / `char` / `string` / `json` | `Utf8` |

Generation and upload run in separate thread pools with a bounded queue
between them, so a row-group encode or a slow upload never stalls generation:

```
generators (CPU count)  ->  queue (30 batches)  ->  upload workers (8)
```

Each upload worker owns its own objects, encodes a row group at a time, and
streams the bytes out as S3 multipart parts. Closing a file completes the
upload, so an object appears atomically and Doris never sees a partial file.

## Pre-flight

`--emit-spec` turns a table into a full field spec, one entry per column, with
a generator chosen from the column type and name. For a 244-column table that
is 244 entries you can edit rather than write. It refuses to overwrite.

`--emit-s3-config` writes a commented TOML template. Unknown keys are rejected
at startup, so a typo fails immediately instead of silently taking a default.

## Compression

Parquet compresses internally, per column chunk, before anything is uploaded.
The bytes on the wire are already compressed, so transport compression on top
would gain nothing.

`compression_level` sets codec effort: zstd takes 1-22, gzip 0-9. The parquet
crate defaults zstd to 1, its weakest setting; this tool defaults to 3, which
is zstd's own default. When the network is the bottleneck, spending CPU here is
usually free speed because it shrinks what has to be uploaded.

The exception is high-entropy data. On a table dominated by UUIDs and random
numbers, levels 1 through 12 all produced 358-364 MiB from the same 4M rows,
a spread under 2%. Parquet dictionary-encodes low-cardinality columns before
zstd sees them, and random values have nothing left to squeeze. Raising the
level only pays when the data actually repeats. Measure before tuning.

### Unique ids: `sequence_string` vs `uuid`

`uuid` emits a real v4 UUID. The sixteen random bytes come from the
generator's own ChaCha12 stream, seeded from the OS once per thread, not from
`Uuid::new_v4`: that draws from the entropy pool on every call, which is a
`getrandom` syscall on any kernel without the vDSO entry point added in 6.11.
Three UUID columns meant three syscalls a row. Taking the bytes from the
already-seeded stream is worth about 31% on the whole pipeline, 922k to 1.20M
rows/s at four generator threads, and produces a v4 UUID by the same
construction.

The interesting difference is size, not speed. The same 4M rows are 358 MiB of
Parquet with `uuid` and 139 MiB with `sequence_string`, because dense ordered
strings compress and random ones do not. Which way that cuts depends entirely
on what bounds the run:

| | bytes/row | rows for 40 TB | hours at 4 threads |
|---|---|---|---|
| `uuid` | 93.8 | 4.3e11 | 98 |
| `sequence_string` | 36.4 | 1.1e12 | 312 |

Bounded by bytes, `uuid` wins three times over: it needs 2.6x fewer rows to
reach the same volume, and it is now the faster generator per row as well.
Bounded by rows, `sequence_string` wins instead, with 2.6x less to compress and
upload for the same row count.

`sequence_string` renders a counter into a fixed-width string instead:

```yaml
- name: invoiceid
  gen:
    type: sequence_string
    template: "inv0000000000000{}"   # default "{}"
    width: 36                                # default 36, a UUID's width
    start: 1                                 # default 1
    step: 1                                  # default 1
```

The counter is zero-padded to fill whatever the literal text leaves over, so
the output is always `width` characters and column widths and file sizes stay
where they were. All generator threads share one cursor and claim blocks of it,
so values are unique across threads; the atomic is touched once per block, not
once per row.

Reach for `uuid` when the run is bounded by a data volume, or when the test
depends on values being random, or when you are measuring compression,
compaction, or write amplification and need the pessimistic case. `sample.spec`
uses it for exactly the first reason. Reach for `sequence_string` when the run
is bounded by rows, a column only needs to be unique and the right size, and
you would rather not push the extra bytes.

### Where generation time goes

Randomness is not the expensive part. Measured by replacing one generator at a
time with a constant, 4M rows of `sample.spec` at four generator threads, best
of three, against a base of 8022 ms:

| generator | fields | cost | note |
|---|---|---|---|
| `datetime_around` | 3 | 2384 ms | 30%, and see the caveat below |
| `sequence_string` | 3 | 1131 ms | 14%, the zero-padding |
| `decimal_range` | 1 | 38 ms | 0.5% |
| `int_range` | 2 | none measurable | under the noise floor |
| `weighted_choice` | 3 | none measurable | under the noise floor |

The last two came out marginally *faster* with the generator than with a
constant, which is how a measurement says "below the noise", here about 1.5%.
A `weighted_choice` row is a float draw, a scan of a handful of weights, and a
`String` clone; a `constant` still pays the clone, so there is nothing to see.
A random draw is around 32 ns: `StdRng` is ChaCha12 in userspace, not a
syscall. Entropy has never been the thing to optimise here.

That ablation writes CSV, which forces the text rendering the Parquet path
skips, so it overstates `datetime_around` for real runs. It is the right shape
for comparing generators against each other, not for costing the pipeline.

So timestamps and decimals do not become text on the way to Parquet. A
generator emits `Value::Timestamp` (epoch microseconds) or `Value::Decimal`
(unscaled units and a scale), and the Parquet writer takes the number. The
strings are built only where something actually needs text: CSV output,
`template` fields, and the JavaScript bridge. Formatting a timestamp so the
writer could parse it straight back was costing more than the whole rest of the
row: on the full Parquet pipeline the change took 4M rows from about 435k to
about 1.09M rows/s.

Two traps in the other direction. `sequence` is classified `StatefulOrdered`,
which clamps generation to one thread, so swapping an `int_range` for it to
"save the random draw" costs about 4x. And `sequence_string` pays for every
digit it zero-pads: letting a literal prefix carry 16 of a 36-character value,
so only 20 digits are padded, is worth about 5% on the whole pipeline over
padding all 36. Twenty digits is also past the u64 counter range, so it cannot
overflow, while a UUID-shaped template leaves only twelve digits and 40 TB of
this data is about 1.1e12 rows. A run that would pass that ceiling is refused
before it starts rather than quietly emitting 37-character values partway
through.

`datetime_around` also stopped reading the clock per row. It refreshes
`Utc::now()` every few thousand rows and draws its offset in microseconds
rather than seconds, which keeps the sub-second digits as varied as they were
when every row called the clock. That matters more than it looks: a datetime
column that quietly became compressible would change what a load test measures.

## Sizing and limits

`--rows` is exact. `--target-size` is approximate, because a row's compressed
size is unknown until its row group is encoded. Generators project the observed
bytes-per-row across every row produced, which lands within a few percent and
gets tighter on larger runs. Expect roughly +0.5% at 10GiB.

`--file-size` rolls to a new object once a file passes the cap. Files can only
roll on a row group boundary, so the result is between the cap and the cap plus
one row group.

Peak memory is roughly `row_group_rows x columns x 24 bytes` per upload worker,
plus `part_size x max_concurrent_parts` for uploads in flight. The startup
banner prints an estimate.

S3 allows 10,000 parts per object, so `part_size` caps the largest file:
10MiB parts allow about 97GiB. A `--file-size` that would need more parts is
refused at startup.

## Reading the status display

```
 rows 12,847,392 / 50,000,000   26%  ████████░░░░░░░░░░░░░░░  ETA 1m28s
 gen  421,003 rows/s upload-bound  threads 14  queue 30/30  buffered 192 MiB
 up   6 done, 8 writers                            sent 15 GiB  rate 15 MiB/s
 file part-w00-000007.parquet 1.2 GiB/2.0 GiB
```

`upload-bound` means generators are blocked on a full queue because uploads
cannot keep up. That is backpressure, not a stall. `draining` means generation
finished and the workers are completing their files. There is no byte rate on
the generation line on purpose: bytes do not exist until a row group is
encoded, so the only honest byte rate is the upload one.

`sent` and `rate` count compressed Parquet bytes, the same bytes that travel
over the network, so the rate is directly comparable to your link speed.
`buffered` is different: it is the uncompressed row group held in memory, so
it describes memory pressure rather than network traffic.

If uploads are the bottleneck, more generator threads will not help. Raise
`max_concurrent_parts` and `--upload-threads`, or accept the link speed. The
queue provides backpressure automatically: when it fills, generators block, so
generation self-throttles to whatever the network sustains. No rate limiting
is needed or wanted.

## Field spec

The spec is YAML. Each field names a generator; fields can reference each other
and are ordered by a dependency graph. Generators include `uuid`,
`sequence_string`, `sequence`, `int_range`, `float_range`, `decimal_range`,
`datetime_around`, `datetime_range`, `choice`, `weighted_choice`, `template`,
`lorem`, `name`, `email`, `address`, `random_bytes`, `fluctuating`, and
`javascript`.

```yaml
version: 1
fields:
  - name: invoiceid
    gen:
      type: sequence_string
      template: "inv0000000000000{}"
  - name: xc_cdc_operation
    gen:
      type: weighted_choice
      values:
        - value: INSERT
          weight: 80
        - value: UPDATE
          weight: 20
  - name: amount
    gen:
      type: decimal_range
      min: "1.00"
      max: "99.99"
      scale: 2
```

Any generator takes `null_rate` to emit nulls at a given rate. `sequence`,
`fluctuating` and `javascript` are stateful and force a single generator
thread. See `sample.spec` for a worked example and the generator reference
below it.

## CSV output

Omitting both `--s3-config` and `--out-dir` writes CSV to stdout or to
`--output`, which is what this tool grew out of. Parquet output requires
`--schema`, because column types come from the DDL.

## Credentials

Credentials come from the standard AWS sources: `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY`, `AWS_PROFILE`, or an instance profile. Put them in
`[s3.credentials]` only if you must, and keep that file at mode 600.
