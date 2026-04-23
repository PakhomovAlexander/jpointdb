# Hacking on JPointDB

A developer-oriented tour: how to run it, where the code lives, how a query
flows end-to-end, how to benchmark your own changes, and what to read first if
you want to understand the fast paths.

## Prerequisites

- **JDK 25** (via [SDKMAN](https://sdkman.io/) or system package). The build
  uses `--add-modules=jdk.incubator.vector` so the Vector API is on the path.
- **`duckdb` CLI** (≥ 1.5) — only needed for golden tests and the side-by-side
  benchmark. `brew install duckdb` / `apt install duckdb` / prebuilt binaries.
- **`curl`**, **`jq`**, **`awk`** — used by bench scripts.
- No other services. The server is just `com.sun.net.httpserver`; storage is
  mmapped files on your disk.

## Five-minute run

```bash
git clone git@github.com:PakhomovAlexander/jpointdb.git
cd jpointdb

# 1. Fetch the 1M-row ClickBench sample + build DuckDB golden results.
./bench/scripts/setup-golden.sh          # downloads ~75 MB sample, produces hits.tsv + q00..q42.tsv

# 2. Build the server dist and run it (first call converts TSV → columnar .jpdb ~once; ~3s on M-class CPU).
./start-server.sh
# ... "listening on http://localhost:8080"

# 3. Talk to it: web UI, CLI REPL, or raw HTTP.
open http://localhost:8080                            # dark-mode SQL editor
./cli.sh                                              # JLine REPL
curl -X POST http://localhost:8080/query \
    --data-binary 'SELECT COUNT(*) FROM hits'         # { "rows":[[1000000]], "elapsedMs": 0, ... }
```

The first `/query` call with a novel filter pattern (LIKE, REGEXP_REPLACE,
etc.) builds a per-dictionary cache that all subsequent calls share — this is
why the same query measured cold vs warm differs by 10-80 ms on Q29 / Q22.

## Repo layout

```
├── core/                  # storage + SQL engine (all the interesting code)
├── server/                # com.sun.net.httpserver wrapper + static web UI
├── cli/                   # JLine REPL
├── bench/                 # ClickBench golden tests + compare-with-duckdb script
├── start-server.sh        # convenience launcher for the bench sample
├── cli.sh                 # convenience launcher for the REPL
├── gradle/                # checkstyle, spotless, spotbugs, dependency-check configs
├── BACKLOG.md             # deferred work items
└── AGENTS.md              # conventions for AI agents editing this repo
```

Inside `core/src/main/java/io/jpointdb/core/`:

| Package       | What lives there |
|---------------|---|
| `convert`     | TSV → columnar bulk loader (`TsvConverter`, `FieldSink`) |
| `tsv`         | Zero-copy TSV scanner over mmapped bytes |
| `schema`      | `ColumnType` enum + `SchemaSniffer` (int32/int64/double/string inference) |
| `column`      | On-disk column formats — `I32/I64/F64Column`, `StringColumn` (DICT + RAW), `Dictionary`, `NullBitmap` |
| `table`       | `Table` = metadata + all opened columns (one `Arena.ofShared` per table) |
| `sql`         | Lexer → `SqlParser` → sealed `SqlAst` → `Binder` → sealed `BoundAst` → `DictBitsetRewriter` pass |
| `query`       | `QueryEngine.run()` — parse + bind + execute. All the performance work lives here |
| `json`        | Tiny handwritten JSON writer (no Jackson) |

Not in `core`:
- `server` — three endpoints (`/health`, `/schema`, `/query`) + embedded web UI.
- `cli` — REPL with `\` meta-commands, CodeMirror-style SQL completion via JLine.
- `bench` — `ClickBenchGoldenTest` JUnit 5 driver; shell scripts for DuckDB comparison.

## How a query flows

```
┌───────────┐  String        ┌──────────┐  SqlAst.Select   ┌─────────┐
│   HTTP    │ ─────────────► │  Parser  │ ───────────────► │ Binder  │
│ / JLine / │                │          │                  │         │
│   tests   │                └──────────┘                  └────┬────┘
└───────────┘                                                   │ BoundSelect
                                                                ▼
                                                         ┌──────────────────┐
                                                         │ DictBitsetRewrite│  (predicate pre-compute)
                                                         └────┬─────────────┘
                                                              ▼
                                                       ┌──────────────┐
                                                       │  Executor    │  dispatch to fast paths
                                                       └──────┬───────┘
                                                              ▼
                                                       ┌──────────────┐
                                                       │ QueryResult  │
                                                       └──────────────┘
```

### `Executor.executeAggregated` — the fast-path cascade

Sits at the top of every aggregate query. Each detector checks the shape of
`plan`; the first one that matches handles the query and returns. Files /
sections to read in order:

1. **`detectGrandTotalSum`** (`Executor.java` ~ L430)
   Algebraic fusion: a grand-total with only `SUM(col + literal)` aggs reduces
   to `SUM(col) + N*COUNT + offsets`. One vectorized pass.

2. **`tryDictMinMaxShortcut`** (~ L563)
   `SELECT MIN(col), MAX(col) FROM t` where `col` is DICT STRING — walk the
   dictionary, skip the row scan entirely. Correct because every dict entry
   was written on behalf of at least one non-null row.

3. **`detectGrandTotalVector` + `VectorBatchAgg`** (~ L645, separate file)
   Grand-total without `WHERE` of SUM/AVG/MIN/MAX over primitive columns →
   SIMD `jdk.incubator.vector` reductions. Q03 / Q04 / Q07 land here.

4. **`detectPrimitiveKey` + `executeAggregatedInline`** (~ L1165)
   The big one. Any `GROUP BY` whose keys are primitive-long-encodable
   (I32, I64, or DICT STRING dict-id, or `extract:*`/`date_trunc:*`/
   `regexp_replace` over DICT STRING, or "derived" expressions that are
   pure functions of the above) goes through `PrimitiveAggMap` — an
   open-addressing hash map with flat `long[cap * width]` keys and flat
   `long[][] / double[][]` agg state. **No boxed `Aggregator` per slot.**

5. **`executeAggregatedPrimitive`** (~ L2287)
   Same primitive GROUP BY key detection but with boxed `Aggregator[]` per
   slot — used when the inline path can't take the query (HAVING, complex
   ORDER BY, MIN/MAX over strings, etc.) but keys are still primitive.

6. **Generic path** (`scanAggChunk` + HashMap<List<Object>, Aggregator[]>)
   Boxed fallback. Correct but slow; any query that reaches here is a
   candidate for a new detector.

### Key data structures

- **`PrimitiveAggMap`** (~ 460 lines) — hand-rolled open-addressing map.
  Specialized `getOrCreateSlot1/2/3(long...)` for the common widths; generic
  `getOrCreateSlotN(long[])` for width ≥ 4. Agg state lives in parallel
  `long[][] longFields` / `double[][] doubleFields` indexed by slot.
  `merge(other)` and the `foldSlot` helper combine per-chunk maps into a
  radix-partitioned result.

- **`LongKeysAggMap`** — older, Object-valued variant for width 1/2 used by
  `executeAggregatedPrimitive` when the inline path can't take the query.

- **`LongHashSet`** — primitive-long open-addressing set backing
  `CountDistinctLong`. Handles the `k == 0` collision with an out-of-band
  flag.

- **`RowPredicate`** (inside `Executor.java`) — compiles a pure-primitive
  AND-chain WHERE (non-null int comparisons + precomputed `BoundDictBitsetMatch`
  leaves) into a `boolean test(long row)` method. Avoids `Boolean`-boxing
  per row.

- **`DictBitsetRewriter`** (`sql/`) — bind-time pass. Rewrites
  `col OP literal` / `col LIKE pattern` over DICT STRING columns into
  `BoundDictBitsetMatch` with a precomputed `boolean[dictSize]`. Bitsets are
  cached process-wide by `(Dictionary, op/kind, literal)`.

### Caches you'll want to know about

All cached at bind time, keyed by `Dictionary` identity (stable for the
column's lifetime):

| Cache | File | What it stores |
|---|---|---|
| `DictBitsetRewriter.BITSET_CACHE` | `sql/DictBitsetRewriter.java` | `boolean[dictSize]` per `(op, literal)` |
| `Executor.COMPUTED_DICT_CACHE` | `query/Executor.java` | `(long[] idMap, String[] newDict)` per transform |
| `Executor.STRLEN_CACHE` | `query/Executor.java` | `long[dictSize]` of codepoint counts |
| `LikeMatcher.CACHE` | `sql/LikeMatcher.java` | compiled matcher per pattern |
| `ScalarFns.PATTERN_CACHE` | `query/ScalarFns.java` | compiled regex per pattern |

They are **not** keyed on any ClickBench-specific identifier — any query
against any table that repeats a filter gets the same speed-up.

## Benchmarking

Two entry points.

### Correctness: `./gradlew :bench:test`

Runs `ClickBenchGoldenTest` which executes each of the 43 queries in
`bench/golden/queries.sql` against JPointDB and diffs the result against
`bench/golden/results/q*.tsv` (captured from DuckDB). Tolerant tie-breaks are
applied for queries whose ORDER BY is ambiguous.

### Performance: `./bench/scripts/compare-duckdb.sh`

Side-by-side timing vs DuckDB on the exact same 1 M-row sample. Requires the
server running on `:8080`.

```bash
./start-server.sh &                    # must already be listening
RUNS=6 AMORT=15 ./bench/scripts/compare-duckdb.sh
# writes bench/results/compare-duckdb.md with the per-query table
```

- `RUNS` — how many times each query is timed. First run is **discarded as
  warm-up**; reported number is `min` of the remaining `RUNS - 1`.
- `AMORT` — how many back-to-back executions per run, summed and divided.
  Lifts the measurement above DuckDB's `.timer` 1 ms resolution.
- JPointDB time = server-side `elapsedNanos` around `QueryEngine.run()`
  (parse + bind + execute). **Does not include** HTTP / JSON overhead.
- DuckDB time = `.timer` real clock around each executed statement.

### Profiling

```bash
# async-profiler on the running server
jps                                    # find the Main PID
async-profiler/profiler.sh -d 30 -f /tmp/jp-cpu.html <pid>
# fire a query in another shell while profiler runs
curl -sS -X POST http://localhost:8080/query --data-binary 'SELECT ...' > /dev/null
```

Hot loops worth knowing:
- `scanAggChunkInline` — the inline primitive GROUP BY inner loop.
- `PrimitiveAggMap.probe2/3` — open-addressing probe for 2/3-wide keys.
- `mergeChunksRadix` / `partitionChunkSlots` — post-scan radix merge.
- `selectTopKSlots` / `sortAllSlots` — slot-based top-k heap on primitive longs.

## Making a change — suggested first targets

Look at the bench table in the top-level README (or the freshly generated
`bench/results/compare-duckdb.md`) and pick a query where JPointDB is slower
than DuckDB. Read the query's shape, then:

1. **Trace the dispatch** in `Executor.executeAggregated` to find which
   detector handles it.
2. **If it lands in the generic path** (`scanAggChunk`) — that's a new fast
   path worth writing.
3. **If it already lands in `executeAggregatedInline`** — profile and look at
   the hot loop. Common levers:
   - Can the per-row state touch fewer cache lines? (see AVG/COUNT counter
     sharing in `detectSimpleAggs`)
   - Can a scalar call be precomputed per dict entry?
     (see `buildExtractMap` / `buildDateTruncMap` / `buildRegexReplaceMap`)
   - Can a filter leaf compile into a primitive predicate?
     (see `RowPredicate.compile`)

Run `./gradlew :bench:test` after every change — correctness first. Then the
side-by-side script to see the perf impact.

### Style & checks

- **No new dependencies** without discussion. The whole `runtimeClasspath`
  dependency list is in `build.gradle.kts` and currently has 2 entries
  (JLine for CLI, JSpecify for annotations).
- **`./gradlew check`** gates Spotless + Checkstyle + ErrorProne + NullAway +
  SpotBugs + JaCoCo (70 % threshold on `:core`) + tests. Must be green
  before commit.
- **JSpecify null-safety** on main sources — NullAway enforces it. Test code
  is exempted (tests do null-poking assertions).

## Repo conventions

- **Comments explain *why*, not *what*.** One-line only — no multi-paragraph
  docstrings. The existing fast-path comments are the model to copy.
- **Prefer editing over adding.** New files are rare; each one needs a clear
  home in the package structure.
- **Commit messages** lead with what the change optimizes / enables, follow
  with a one-sentence result line (e.g. "Q29 39 → 10 ms"). Look at recent
  `git log --oneline` for the pattern.
- **Correctness > speed.** If a fast path might be wrong on edge cases
  (nullable columns, empty dicts, NaN doubles), return `null` from the
  detector and let the generic path handle it.

## Further reading in the repo

- `BACKLOG.md` — what's deferred and why; good source of "next things to try".
- `AGENTS.md` — operational notes for AI coding assistants working on the
  repo, but most of it applies to humans too.
- Recent commit log — each optimization has a commit that explains the
  problem, the fix, and the measured impact. Read `git log` from the root
  of any of the optimization commits for context.
