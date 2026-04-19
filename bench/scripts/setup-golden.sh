#!/usr/bin/env bash
# Generate golden results for JPointDB correctness tests.
#
# Steps:
#   1. Download first ROWS rows of ClickBench hits.tsv.gz (default 100k).
#   2. Load into a temporary DuckDB database using the ClickBench schema.
#   3. Run each of the 43 ClickBench queries, dumping results as TSV into
#      bench/golden/results/q##.tsv (committed to git — small files).
#
# Usage:
#   ROWS=50000 ./bench/scripts/setup-golden.sh
#   ./bench/scripts/setup-golden.sh        # default ROWS=100000

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(dirname "$SCRIPT_DIR")"
SAMPLE_TSV="$BENCH_DIR/sample/hits.tsv"
SCHEMA="$BENCH_DIR/golden/create.sql"
QUERIES="$BENCH_DIR/golden/queries.sql"
RESULTS_DIR="$BENCH_DIR/golden/results"
DB="$BENCH_DIR/sample/hits.duckdb"
ROWS="${ROWS:-100000}"
URL="https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz"

command -v duckdb >/dev/null || { echo "duckdb not found in PATH" >&2; exit 1; }
command -v curl   >/dev/null || { echo "curl not found in PATH" >&2; exit 1; }

mkdir -p "$(dirname "$SAMPLE_TSV")" "$RESULTS_DIR"

if [[ ! -f "$SAMPLE_TSV" ]]; then
    echo "==> downloading first $ROWS rows from $URL"
    # head closes its pipe early, producing SIGPIPE for curl/gunzip — that's expected.
    set +o pipefail
    curl -fsSL "$URL" 2>/dev/null | gunzip 2>/dev/null | head -n "$ROWS" > "$SAMPLE_TSV" || true
    set -o pipefail
    if [[ ! -s "$SAMPLE_TSV" ]]; then
        echo "download produced empty file" >&2
        exit 1
    fi
    actual=$(wc -l < "$SAMPLE_TSV")
    size=$(du -h "$SAMPLE_TSV" | cut -f1)
    echo "    got $actual rows, $size"
else
    echo "==> reusing existing sample $SAMPLE_TSV ($(wc -l < "$SAMPLE_TSV") rows)"
fi

rm -f "$DB"
TMP_SQL=$(mktemp)
trap 'rm -f "$TMP_SQL"' EXIT

cat "$SCHEMA" > "$TMP_SQL"
# NULLSTR='\N' matches the JPointDB convention: only the literal backslash-N
# marks NULL; empty fields are kept as empty strings for TEXT columns.
printf "COPY hits FROM '%s' (DELIMITER E'\\\\t', HEADER false, NULLSTR '\\\\N');\n" "$SAMPLE_TSV" >> "$TMP_SQL"

i=0
while IFS= read -r line || [[ -n "$line" ]]; do
    [[ -z "${line// /}" ]] && continue
    idx=$(printf "q%02d" "$i")
    out="$RESULTS_DIR/$idx.tsv"
    stripped="${line%;}"
    # COPY (query) TO 'file' (FORMAT CSV, DELIMITER '\t', HEADER TRUE) — TSV with header.
    printf "COPY (%s) TO '%s' (FORMAT CSV, DELIMITER E'\\\\t', HEADER TRUE);\n" \
        "$stripped" "$out" >> "$TMP_SQL"
    i=$((i + 1))
done < "$QUERIES"

echo "==> running $i queries against DuckDB"
duckdb "$DB" < "$TMP_SQL"

echo "==> golden results written to $RESULTS_DIR/"
ls -la "$RESULTS_DIR"
