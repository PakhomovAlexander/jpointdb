# JPointDB perf journey — session summary

Running ClickBench 43 queries on 1M rows of `hits.tsv`, row-at-a-time executor,
single JVM. DuckDB 1.5.1 used as reference.

## Progression

| step | JPointDB total | DuckDB total | ratio | Δ |
|------|-----:|-----:|-----:|-----:|
| baseline | 11 808 ms | 137 ms | 86× | — |
| (1) LIKE: cache matcher + String.contains fast paths | 7 853 ms | 117 ms | 67× | **−33.5 %** |
| (2) Dict String cache in DICT columns | 5 305 ms | 114 ms | 47× | **−32.4 %** |
| (3) Bind-time LikeMatcher resolution (refactor) | ~5 305 ms | — | — | ~noise |
| (4) AND/OR short-circuit in ExprEvaluator | **5 074 ms** | 114 ms | **45×** | **−4.4 %** |

Cumulative from baseline: **11 808 → 5 074 ms (−57 %)**, zero-change-to-executor-shape.

## What each fix did

### (1) LIKE — biggest single win
`ExprEvaluator.evalLike` compiled `Pattern` per row (85 % of Q21's CPU).
Added `LikeMatcher` interface with fast paths for `literal`, `literal%`,
`%literal`, `%literal%` → `String.equals/startsWith/endsWith/contains`,
each lowered to a SIMD intrinsic by HotSpot. Regex fallback caches by
pattern string. Q21 6.0×, Q23 4.5×.

### (2) Dict String cache
`StringColumn.valueAsString` in DICT mode built a fresh Java `String` for
every row (50 % of Q21's post-(1) CPU). `Dictionary` now holds a lazy
`String[size]`: first read per dict id decodes UTF-8 and stores, all
subsequent reads return the cached reference. Benign slot-write races
(String is immutable, `keyAsString(id)` is pure).
Strongest wins: Q38 Title 3.5×, Q34 URL 3.3×, Q28 AVG(STRLEN) 3.0×,
Q29 REGEXP_REPLACE 2.5×, Q43 DATE_TRUNC 2.1×.

### (3) Bind-time LikeMatcher
`LikeMatcher` moved to `io.jpointdb.core.sql` as a proper bind-time
artifact. `Binder` resolves the matcher when `pattern` is a literal;
`BoundLike` carries it; `ExprEvaluator.evalLike` uses the prebound
matcher without touching the `ConcurrentHashMap` on the hot path.
Clean refactor; perf win within noise.

### (4) AND/OR short-circuit
`evalBinary` used to evaluate both sides of AND/OR before folding
through `andOp`/`orOp`. Q37's WHERE has six AND'd predicates — most
rows fail on the first (`CounterID = 62`), but we were running all
six. Three-valued logic preserved (FALSE left short-circuits AND
even with NULL right). Q37 ×1.4, Q38 ×1.9, Q39 ×2.1, Q41-43 ×1.5-1.7.

## Where the remaining 5 074 ms goes

Per-query hotspots (from asprof after all fixes):

| query | ms | dominant frame | meaning |
|-------|---:|:--|:--|
| Q30 (90 SUMs) | 1 324 | 58 % evalBinary, 24 % isNullAt, 10 % numPlus boxing | 90 `ResolutionWidth` reads per row, no CSE |
| Q33 (heavy agg) | 1 126 | similar | row-at-a-time + boxing |
| Q29 REGEXP_REPLACE | 308 | (not profiled — regex intrinsics) | regex engine itself |
| Q19 EXTRACT | ~245 | 22 % TimSort, 20 % HashMap, 12.5 % compare, 3 % extract | GROUP BY + ORDER BY infra |
| Q40 CASE | 181 | evalBinary / compare | row-at-a-time |

The floor: every remaining hotspot is a symptom of row-at-a-time +
Object-boxed eval. The JIT-inlined hot loop is the dominant frame in
every flame. Further micro-fixes (CSE, native DATE/DATETIME, bind-time
predicate compilation) give ×1.2–1.5 each; the big jump needs
**vector batches**.

## Next (per `BACKLOG.md` Stage 3)

- **Vector batches** (4096 rows, typed primitive arrays + null mask) is
  the foundation. Kills boxing (7 % Long.valueOf, 10 % numPlus),
  lets `isNullAt` become a word-wise bit scan (24 % of Q30), lets
  `Aggregator.accept` become `acceptBatch(long[], int)`, unlocks
  `jdk.incubator.vector` SIMD for filters / aggregates.
  Expected: −60 % to −75 % of current 5 074 ms.
- **Open-addressing hash aggregator** with primitive `long` keys. Q19
  HashMap is 20 %, Q17/18 similar. Removes boxing and List-wrapping
  of composite keys.
- **Parallel scan** via ForkJoinPool. DuckDB uses all cores; we're
  single-threaded. This is how we close the absolute gap once the
  per-core cost is down.
- **Native DATE/DATETIME** (i32 days / i64 micros). Affects Q19, Q28,
  Q37-43 filter comparisons; ~5-10 % savings.
- **Common subexpression elimination**. Q30 reads `ResolutionWidth`
  90× per row; batches would implicitly fix this, but a pre-batch CSE
  pass helps too.

All current results and flamegraphs are committed under
`bench/results/`:

- `compare-duckdb.md` — latest per-query table
- `summary-1m.md` — baseline profile analysis
- `summary-after-like.md` — after fix (1)
- `summary-after-dict.md` — after fix (2)
- `summary-final.md` — this file
- `flame-1m.html` — baseline full-compare flame
- `flame-q21-like.html` — Q21 baseline (regex-dominated)
- `flame-q21-like-fixed.html` — Q21 after (1)
- `flame-q21-dict.html` — Q21 after (2)
