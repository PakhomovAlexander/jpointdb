# JPointDB

Personal analytical database in Java 25 — columnar storage, SQL engine, REST server, CLI, web UI. Built to chase [ClickBench](https://github.com/ClickHouse/ClickBench) performance with minimum third-party deps.

**Status:** 43/43 ClickBench queries pass against the DuckDB reference on a 100k-row `hits.tsv` sample. Stage 3 (vectorized/parallel execution) is still ahead — see [`BACKLOG.md`](BACKLOG.md).

## Stack

- **Java 25** — sealed interfaces, pattern matching, FFM (`MemorySegment` + `Arena.ofShared` for mmap), `jdk.incubator.vector` enabled (not used yet).
- **Gradle 9** multi-module build.
- **JSpecify** null-safety enforced via NullAway on main sources.
- **Runtime deps:** JLine (CLI), CodeMirror via CDN (web). That's it. No Spring, no Jackson, no Lombok.
- **Test/golden deps:** JUnit 5, DuckDB CLI as reference for correctness goldens.

## Modules

| Module | Purpose |
|--------|---------|
| `core`   | Storage, SQL parser/binder/executor, TSV converter, JSON |
| `server` | `com.sun.net.httpserver` REST server (`/health`, `/schema`, `/query`) + static web UI |
| `cli`    | JLine REPL with syntax highlighting, history, `\`-meta commands |
| `bench`  | ClickBench golden correctness tests (compared against DuckDB) |

## Quick start

```bash
# 1. Build and fetch a 100k-row ClickBench sample (requires duckdb CLI + curl).
./bench/scripts/setup-golden.sh

# 2. Start the server on localhost:8080 — converts sample/hits.tsv into a .jpdb on first run.
./start-server.sh

# 3a. Web UI
open http://localhost:8080

# 3b. Or CLI REPL
./cli.sh
```

One-shot query from CLI:
```bash
./cli.sh -c 'SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;'
```

## Build & verify

```bash
./gradlew check             # Spotless + Checkstyle + ErrorProne + NullAway + SpotBugs + tests + JaCoCo (~11s)
./gradlew :bench:test       # ClickBench goldens (needs bench/golden/results/ populated)
./gradlew pitest            # PIT mutation testing (slow; core baseline ~71% killed)
./gradlew dependencyCheckAnalyze  # OWASP — slow first run (~800MB NVD)
```

## Storage format

- Per-table directory `*.jpdb/` with `meta.json` + one file per column.
- Integer/double columns: raw primitive arrays + optional Arrow-style validity bitmap.
- String columns: DICT (global dictionary + i32 ids, magic `JPCS`) with RAW fallback (offsets + bytes) on cardinality overflow.
- Mmap via `Arena.ofShared` — read path is off-heap.

## Further reading

- [`BACKLOG.md`](BACKLOG.md) — everything deferred: Stage 3 performance (vector batches, SIMD, parallel scan, hash agg), engine features (native DATE/DATETIME, JOIN, windows), dev ergonomics.
- [`AGENTS.md`](AGENTS.md) — conventions and commands for AI agents working on this repo.
