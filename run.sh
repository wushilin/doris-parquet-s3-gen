#!/usr/bin/env bash
#
# Generate Parquet from a Doris schema and stream it to S3.
#
#   ./run.sh                      10GiB to the bucket in s3.toml
#   ./run.sh -t 1GiB              a smaller run
#   ./run.sh --local /tmp/pq      write locally instead, no S3
#   ./run.sh -n                   print the command without running it
#
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

BIN="./target/release/doris-parquet-s3-gen"
SCHEMA="sample.sql"
SPEC="sample.spec"
S3_CONFIG="s3.local.toml"
TARGET_SIZE="10GiB"
FILE_SIZE="2GiB"
THREADS=""
UPLOAD_THREADS="8"
QUEUE_DEPTH="100"
LOCAL_DIR=""
DRY_RUN="no"

die() { printf 'error: %s\n' "$*" >&2; exit 1; }

usage() {
  sed -n '2,9p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
  cat <<'USAGE'

Options:
  -s, --schema FILE     Doris CREATE TABLE file      (default: sample.sql)
  -p, --spec FILE       field generation spec        (default: sample.spec)
  -c, --config FILE     S3 config TOML          (default: s3.local.toml)
  -t, --target SIZE     stop after this much Parquet (default: 10GiB)
  -f, --file-size SIZE  roll to a new object at      (default: 2GiB)
  -j, --threads N       generation threads           (default: CPU count)
  -u, --upload-threads N  upload workers             (default: 8)
  -q, --queue-depth N   batches buffered in the queue (default: 100)
  -o, --local DIR       write to DIR instead of S3
  -n, --dry-run         print the command and exit
  -h, --help            this text

Sizes accept K/M/G/T/P with optional i and B, for example 512M, 2GiB, 40TiB.
Every unit is a power of 1024, so GB and GiB mean the same thing.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -s|--schema)    SCHEMA="${2:?--schema needs a file}"; shift 2 ;;
    -p|--spec)      SPEC="${2:?--spec needs a file}"; shift 2 ;;
    -c|--config)    S3_CONFIG="${2:?--config needs a file}"; shift 2 ;;
    -t|--target)    TARGET_SIZE="${2:?--target needs a size}"; shift 2 ;;
    -f|--file-size) FILE_SIZE="${2:?--file-size needs a size}"; shift 2 ;;
    -j|--threads)   THREADS="${2:?--threads needs a number}"; shift 2 ;;
    -u|--upload-threads) UPLOAD_THREADS="${2:?--upload-threads needs a number}"; shift 2 ;;
    -q|--queue-depth)    QUEUE_DEPTH="${2:?--queue-depth needs a number}"; shift 2 ;;
    -o|--local)     LOCAL_DIR="${2:?--local needs a directory}"; shift 2 ;;
    -n|--dry-run)   DRY_RUN="yes"; shift ;;
    -h|--help)      usage; exit 0 ;;
    *)              die "unknown option '$1' (try --help)" ;;
  esac
done

# ---- preflight ------------------------------------------------------------

[[ -f "$SCHEMA" ]] || die "schema file '$SCHEMA' not found"
[[ -f "$SPEC" ]]   || die "spec file '$SPEC' not found"

if [[ ! -x "$BIN" ]]; then
  echo "building release binary..."
  cargo build --release
fi

DEST_ARGS=()
if [[ -n "$LOCAL_DIR" ]]; then
  DEST_ARGS=(--out-dir "$LOCAL_DIR")
  DEST_LABEL="$LOCAL_DIR"
else
  if [[ ! -f "$S3_CONFIG" ]]; then
    if [[ -f "s3.toml" ]]; then
      die "S3 config '$S3_CONFIG' not found. Copy the template and edit it:
    cp s3.toml $S3_CONFIG
Anything matching *.local.* is gitignored, so your bucket stays out of git."
    fi
    die "S3 config '$S3_CONFIG' not found; create one with:
    $BIN --emit-s3-config $S3_CONFIG"
  fi

  # Credentials may come from the environment, a profile, or the config file.
  if ! grep -q '^[[:space:]]*access_key_id' "$S3_CONFIG"; then
    if [[ -z "${AWS_ACCESS_KEY_ID:-}" && -z "${AWS_PROFILE:-}" ]]; then
      die "no credentials: set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY,
    set AWS_PROFILE, or add an [s3.credentials] section to $S3_CONFIG"
    fi
  fi

  BUCKET=$(sed -n 's/^[[:space:]]*bucket[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' "$S3_CONFIG" | head -1)
  PREFIX=$(sed -n 's/^[[:space:]]*prefix[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' "$S3_CONFIG" | head -1)
  [[ -n "$BUCKET" ]] || die "no bucket found in $S3_CONFIG"
  DEST_LABEL="s3://${BUCKET}/${PREFIX}"

  # Warn if the target prefix already holds objects, so a rerun does not
  # quietly mix new files in with old ones.
  if command -v aws >/dev/null 2>&1; then
    EXISTING=$(aws s3 ls "s3://${BUCKET}/${PREFIX}" 2>/dev/null | grep -c '\.parquet' || true)
    if [[ "${EXISTING:-0}" -gt 0 ]]; then
      echo "note: ${EXISTING} parquet object(s) already exist under ${DEST_LABEL}"
      echo "      new files use the same part-wNN-NNNNNN.parquet naming and may overwrite them."
      read -r -p "continue? [y/N] " reply
      [[ "$reply" =~ ^[Yy]$ ]] || { echo "aborted."; exit 1; }
    fi
  fi
fi

CMD=("$BIN"
  --schema "$SCHEMA"
  --spec "$SPEC"
  "${DEST_ARGS[@]}"
  --target-size "$TARGET_SIZE"
  --file-size "$FILE_SIZE"
  --upload-threads "$UPLOAD_THREADS"
  --queue-depth "$QUEUE_DEPTH")

# An unset thread count lets the binary default to the CPU count.
if [[ -n "$THREADS" ]]; then
  CMD+=(--threads "$THREADS")
fi

if [[ -z "$LOCAL_DIR" ]]; then
  CMD+=(--s3-config "$S3_CONFIG")
fi

# ---- run ------------------------------------------------------------------

echo "schema      $SCHEMA"
echo "spec        $SPEC"
echo "destination $DEST_LABEL"
echo "target      $TARGET_SIZE   files roll at $FILE_SIZE"
echo "threads     ${THREADS:-cpu count} generating, $UPLOAD_THREADS uploading, queue $QUEUE_DEPTH"
echo

if [[ "$DRY_RUN" == "yes" ]]; then
  printf '%q ' "${CMD[@]}"; echo
  exit 0
fi

START=$SECONDS
"${CMD[@]}"
echo "finished in $((SECONDS - START))s"

if [[ -z "$LOCAL_DIR" ]] && command -v aws >/dev/null 2>&1; then
  echo
  echo "objects now under ${DEST_LABEL}:"
  aws s3 ls "s3://${BUCKET}/${PREFIX}" --human-readable --summarize | tail -20
fi
