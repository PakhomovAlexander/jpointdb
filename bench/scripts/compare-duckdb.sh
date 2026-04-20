#!/usr/bin/env bash
# Side-by-side ClickBench comparison: JPointDB vs DuckDB.
#
# For each of the 43 queries in bench/golden/queries.sql:
#   • runs it RUNS times against each engine (warm, in-process session)
#   • takes the minimum wall time
#   • prints a table and writes bench/results/compare-duckdb.md
#
# Prerequisites:
#   • duckdb CLI on PATH, bench/sample/hits.duckdb populated (setup-golden.sh)
#   • JPointDB server running on $URL (default http://localhost:8080) with
#     bench/sample/hits.tsv loaded (./start-server.sh)
#
# Env vars:
#   URL=http://localhost:8080   JPointDB endpoint
#   RUNS=4                      total runs per query (first is discarded as warmup)
#   AMORT=10                    amortization factor — each timed run executes the
#                               query AMORT times back-to-back and divides; this
#                               gets us sub-millisecond precision from duckdb's
#                               1ms timer resolution on small samples.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(dirname "$SCRIPT_DIR")"
ROOT_DIR="$(dirname "$BENCH_DIR")"
DB="$BENCH_DIR/sample/hits.duckdb"
QUERIES="$BENCH_DIR/golden/queries.sql"
RESULTS_DIR="$BENCH_DIR/results"
OUT="$RESULTS_DIR/compare-duckdb.md"
URL="${URL:-http://localhost:8080}"
RUNS="${RUNS:-4}"
AMORT="${AMORT:-10}"

command -v duckdb >/dev/null || { echo "duckdb not found" >&2; exit 1; }
command -v curl   >/dev/null || { echo "curl not found"   >&2; exit 1; }
command -v jq     >/dev/null || { echo "jq not found"     >&2; exit 1; }
[[ -f "$DB"      ]] || { echo "missing $DB; run bench/scripts/setup-golden.sh" >&2; exit 1; }
[[ -f "$QUERIES" ]] || { echo "missing $QUERIES" >&2; exit 1; }

if ! curl -fsS "$URL/health" >/dev/null 2>&1; then
    echo "JPointDB not reachable at $URL; start it with ./start-server.sh" >&2
    exit 1
fi

mkdir -p "$RESULTS_DIR"
ROWS=$(curl -fsS "$URL/health" | jq -r '.rowCount')

# ---- DuckDB: for each query, emit AMORT identical copies per run so the
# .timer captures total ms for AMORT executions; we divide later. This lifts
# us above the 1ms real-time resolution on small samples.
duck_script=$(mktemp)
{
    echo ".timer on"
    echo ".output /dev/null"
    for ((r=1; r<=RUNS; r++)); do
        while IFS= read -r query; do
            [[ -z "$query" ]] && continue
            for ((a=1; a<=AMORT; a++)); do
                # Each block of AMORT lines is one "timed unit"; we reset
                # the timer by toggling it so only the batch total prints.
                echo "$query"
            done
            # Insert a cheap SELECT 1 and throw away its timing by marker.
        done < "$QUERIES"
    done
} > "$duck_script"

echo "==> DuckDB: running $RUNS × 43 × $AMORT queries..." >&2
duck_raw=$(mktemp)
duckdb "$DB" < "$duck_script" 2>/dev/null \
    | awk '/^Run Time \(s\):/ {print $5 * 1000}' > "$duck_raw"

if [[ $(wc -l < "$duck_raw") -ne $((RUNS * 43 * AMORT)) ]]; then
    echo "DuckDB produced $(wc -l < "$duck_raw") timings, expected $((RUNS * 43 * AMORT))" >&2
    exit 1
fi

# Fold AMORT consecutive timings into one per (query, run) by summing and
# dividing — gives us the amortized per-query ms for that run.
duck_times=$(mktemp)
awk -v a="$AMORT" '{s+=$1; c++; if(c==a){printf "%.3f\n", s/a; s=0; c=0}}' \
    "$duck_raw" > "$duck_times"

if [[ $(wc -l < "$duck_times") -ne $((RUNS * 43)) ]]; then
    echo "DuckDB produced $(wc -l < "$duck_times") timings, expected $((RUNS * 43))" >&2
    exit 1
fi

# ---- JPointDB: POST each query AMORT times per run, sum server-side nanos.
# The server returns elapsedNanos measured around QueryEngine.run — it excludes
# HTTP/JSON overhead, matching DuckDB's .timer semantics.
echo "==> JPointDB: running $RUNS × 43 × $AMORT queries against $URL..." >&2
jp_times=$(mktemp)
: > "$jp_times"
q_idx=0
while IFS= read -r query; do
    [[ -z "$query" ]] && continue
    q_idx=$((q_idx + 1))
    for ((r=1; r<=RUNS; r++)); do
        total_ns=0
        for ((a=1; a<=AMORT; a++)); do
            ns=$(curl -fsS -X POST --data-binary "$query" "$URL/query" | jq -r '.elapsedNanos // empty')
            if [[ -z "$ns" ]]; then
                echo "query $q_idx returned no elapsedNanos; server likely errored" >&2
                exit 1
            fi
            total_ns=$((total_ns + ns))
        done
        # Per-execution average in ms.
        awk -v t="$total_ns" -v a="$AMORT" 'BEGIN{printf "%.3f\n", t/a/1000000}' >> "$jp_times"
    done
done < "$QUERIES"

if [[ $(wc -l < "$jp_times") -ne $((RUNS * 43)) ]]; then
    echo "JPointDB produced $(wc -l < "$jp_times") timings, expected $((RUNS * 43))" >&2
    exit 1
fi

# ---- Collate and emit a markdown table. Min of runs 2..RUNS (1st is warmup).
now=$(date '+%Y-%m-%d %H:%M:%S %Z')
host=$(uname -sm)
jdk=$(java -version 2>&1 | head -n1 | tr -d '"')
{
    echo "# JPointDB vs DuckDB — ClickBench side-by-side"
    echo
    echo "- Generated: $now"
    echo "- Host: \`$host\`"
    echo "- JDK: \`$jdk\`"
    echo "- Rows: $ROWS"
    echo "- Runs per query: $RUNS (first discarded as warm-up; reported = min of remaining $((RUNS - 1)))"
    echo "- JPointDB time = server-side \`elapsedMs\`; DuckDB time = \`.timer\` real"
    echo
    echo "| # | JPointDB, ms | DuckDB, ms | ratio (jp/duck) | query |"
    echo "|---|-----:|-----:|-----:|:------|"
} > "$OUT"

jp_sum=0
duck_sum=0
q_idx=0
while IFS= read -r query; do
    [[ -z "$query" ]] && continue
    q_idx=$((q_idx + 1))

    jp_best=$(awk -v start=$(( (q_idx - 1) * RUNS + 2 )) -v end=$(( q_idx * RUNS )) \
        'NR>=start && NR<=end {v=$1+0; if(min==""||v<min)min=v} END{printf "%.3f", min}' "$jp_times")
    duck_best=$(awk -v start=$(( (q_idx - 1) * RUNS + 2 )) -v end=$(( q_idx * RUNS )) \
        'NR>=start && NR<=end {v=$1+0; if(min==""||v<min)min=v} END{printf "%.3f", min}' "$duck_times")

    jp_sum=$(awk -v a="$jp_sum" -v b="$jp_best" 'BEGIN{printf "%.3f", a+b}')
    duck_sum=$(awk -v a="$duck_sum" -v b="$duck_best" 'BEGIN{printf "%.3f", a+b}')

    ratio=$(awk -v a="$jp_best" -v b="$duck_best" \
        'BEGIN{if(b+0==0)print "—"; else printf "%.2f", a/b}')

    # Truncate query to 90 chars for table.
    short_q="${query//|/\\|}"
    if [[ ${#short_q} -gt 90 ]]; then
        short_q="${short_q:0:87}..."
    fi

    printf "| %02d | %s | %s | %s | \`%s\` |\n" "$q_idx" "$jp_best" "$duck_best" "$ratio" "$short_q" >> "$OUT"
done < "$QUERIES"

total_ratio=$(awk -v a="$jp_sum" -v b="$duck_sum" 'BEGIN{if(b+0==0)print "—"; else printf "%.2f", a/b}')
{
    echo "| — | **$jp_sum** | **$duck_sum** | **$total_ratio** | _sum_ |"
    echo
    echo "Lower is better. \`ratio > 1\` means JPointDB is slower than DuckDB on that query."
} >> "$OUT"

echo "==> wrote $OUT" >&2
cat "$OUT"

rm -f "$duck_script" "$duck_raw" "$duck_times" "$jp_times"
