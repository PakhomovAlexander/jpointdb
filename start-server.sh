#!/usr/bin/env bash
# Start the JPointDB REST server. Defaults to loading bench/sample/hits.tsv
# with the ClickBench column names; override via args or env vars.
#
# Usage:
#   ./start-server.sh                          # defaults, port 8080
#   PORT=9000 ./start-server.sh                # change port via env
#   ./start-server.sh --data-dir path/to.jpdb  # any server flag → pass-through
#   ./start-server.sh --tsv my.tsv --column-names a,b,c --port 9000
#
# The first run converts the TSV into a columnar .jpdb directory next to it;
# subsequent runs just mmap the existing store.

set -euo pipefail
cd "$(dirname "$0")"

SERVER_BIN="server/build/install/server/bin/server"
if [[ ! -x "$SERVER_BIN" ]]; then
    echo "==> building server distribution (first run)..."
    ./gradlew -q :server:installDist
fi

# Pass-through mode: any explicit argument → forward verbatim.
if [[ $# -gt 0 ]]; then
    exec "$SERVER_BIN" "$@"
fi

PORT="${PORT:-8080}"
TSV="${TSV:-bench/sample/hits.tsv}"
DATA_DIR="${DATA_DIR:-}"

if [[ -n "$DATA_DIR" ]]; then
    exec "$SERVER_BIN" --data-dir "$DATA_DIR" --port "$PORT"
fi

if [[ ! -f "$TSV" ]]; then
    echo "No default sample at $TSV." >&2
    echo "Either:" >&2
    echo "  • run bench/scripts/setup-golden.sh to fetch a 100k-row ClickBench sample, or" >&2
    echo "  • set TSV=/path/to/file.tsv, or DATA_DIR=/path/to.jpdb, or pass flags directly." >&2
    exit 1
fi

NAMES=$(awk '/CREATE TABLE/{f=1;next} /^\)/{f=0} f && NF>1 {gsub(/^[ \t]+/,""); print $1}' \
    bench/golden/create.sql | paste -sd, -)

exec "$SERVER_BIN" \
    --tsv "$TSV" \
    --column-names "$NAMES" \
    --port "$PORT"
