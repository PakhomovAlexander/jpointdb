#!/usr/bin/env bash
# Thin wrapper around the JPointDB CLI.
#
# Usage:
#   ./cli.sh                                        # REPL, localhost:8080
#   ./cli.sh -c 'SELECT COUNT(*) FROM hits;'        # one-shot query
#   URL=http://host:9000 ./cli.sh                   # override server URL
#   ./cli.sh --url http://host:9000 -c 'SELECT 1 FROM t;'
#
# Default URL is http://localhost:${PORT:-8080}.

set -euo pipefail
cd "$(dirname "$0")"

CLI_BIN="cli/build/install/cli/bin/cli"
if [[ ! -x "$CLI_BIN" ]]; then
    echo "==> building cli distribution (first run)..."
    ./gradlew -q :cli:installDist
fi

# Determine default URL; respect $URL and $PORT if set.
DEFAULT_URL="${URL:-http://localhost:${PORT:-8080}}"

# If the user already passed --url, just forward everything. Otherwise prepend --url.
has_url=0
for arg in "$@"; do
    if [[ "$arg" == "--url" ]]; then has_url=1; break; fi
done

if [[ $has_url -eq 1 ]]; then
    exec "$CLI_BIN" "$@"
else
    exec "$CLI_BIN" --url "$DEFAULT_URL" "$@"
fi
