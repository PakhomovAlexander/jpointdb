# JPointDB vs DuckDB — ClickBench 1M summary + CPU profile

- Dataset: first 1,000,000 rows of ClickBench `hits.tsv`
- Host: `Darwin arm64`, JDK 25.0.1 LTS
- JPointDB: single-threaded, row-at-a-time executor, string columns in DICT mode
- DuckDB: 1.5.1, default (uses all cores)
- Methodology: each query run `RUNS=3` times, each timing unit amortized over `AMORT=5` back-to-back executions (sub-ms precision for DuckDB); the first measured run is discarded; reported = min of the remaining 2
- Profile: async-profiler 4.3 `cpu` event sampled at 1ms, attached to JPointDB server for the duration of one full compare pass

## Totals

| engine | sum of 43 queries (ms) |
|--------|-----------------------:|
| JPointDB | **11 808** |
| DuckDB   | **137** |
| **ratio** | **86×** slower |

JPointDB wins 0/43 at 1M (at 100k it won 2/43 — the 100k wins were dominated by fixed overhead, not query work).

## Top-10 queries by absolute JPointDB time (~85 % of the pie)

| # | JP ms | Duck ms | ratio | query shape |
|---|------:|--------:|------:|:------------|
| 33 | 1 976 | 2.4  | 823× | `MIN(URL), MIN(Title), COUNT(*), COUNT(DISTINCT UserID)` on filtered rows |
| 30 | 1 760 | 0.8  | 2 199× | `SUM(ResolutionWidth + N)` repeated 90× |
| 23 | 1 684 | 1.2  | 1 404× | `Title LIKE '%Google%' AND URL NOT LIKE '%.google.%'` + `MIN(URL), MIN(Title)` |
| 29 |   785 | 0.2  | 3 924× | `REGEXP_REPLACE(Referer, ...) GROUP BY k HAVING COUNT(*) > 100000` |
| 22 |   669 | 5.8  | 115× | `URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase` |
| 21 |   621 | 0.2  | 3 107× | `WHERE URL LIKE '%google%'` |
| 24 |   621 | 7.0  | 89× | `SELECT * WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10` |
| 38 |   470 | 1.6  | 294× | `Title` GROUP BY with filter |
| 40 |   460 | 11.6 | 40× | `CASE WHEN` over three int columns |
| 19 |   441 | 2.4  | 184× | `extract(minute FROM EventTime)` as a GROUP BY key |

Full table: [`bench/results/compare-duckdb.md`](compare-duckdb.md)

## CPU profile — where the JPointDB time goes

Total on-CPU samples: 84 897. Categorized:

| bucket | samples | share | what it is |
|--------|--------:|------:|:-----------|
| **regex/Pattern** | 18 406 | **21.7 %** | `Pattern.compile` inside `ExprEvaluator.evalLike` — **one compile per row** (not cached) |
| **String materialize** | 13 140 | **15.5 %** | `new String(bytes, UTF_8)` in `StringColumn.valueAsString`; every dict lookup decodes UTF-8 fresh |
| **TimSort / Arrays.sort** | 9 606 | **11.3 %** | ORDER BY, mostly string `compareTo` |
| **GC (young-gen copy)** | 5 847 | **6.9 %** | downstream of the String allocation storm |
| **HashMap.getNode** | 5 378 | **6.3 %** | `java.util.HashMap` in group aggregate, boxed Long keys |
| **Aggregator.accept** | 3 716 | **4.4 %** | per-row virtual dispatch into `SumLong`/`Count`/`MinMax` etc |
| **isNullAt** | 2 742 | **3.2 %** | null-bitmap bit test — per row, not batched |
| **boxing (Long.valueOf etc)** | 1 420 | **1.7 %** | boxing integers at the eval boundary |

Flamegraph (full, interactive): [`bench/results/flame-1m.html`](flame-1m.html)

### Top individual leaf frames

```
7616  io/jpointdb/core/query/ExprEvaluator.eval
6223  java/lang/String.checkIndex
5423  io/jpointdb/core/query/Executor.executeAggregated
5135  io/jpointdb/core/query/ExprEvaluator.compare
4918  java/lang/String.<init>
3937  java/util/regex/Pattern$CharPropertyGreedy.match
3350  java/lang/String.decodeUTF8_UTF16
3340  io/jpointdb/core/query/ExprEvaluator.readColumn
3244  java/util/TimSort.mergeLo
2374  java/lang/StringUTF16.charAt
2339  G1ParScanThreadState::do_copy_to_survivor_space
2328  java/util/HashMap.getNode
2235  io/jpointdb/core/query/ExprEvaluator.evalBinary
2187  io/jpointdb/core/column/AbstractColumn.isNullAt
2058  io/jpointdb/core/query/Aggregator$SumLong.accept
```

## Recommended attack order (by expected ROI)

1. **LIKE: cache Pattern + literal-only fast paths** — ~20 min of work.
   `ExprEvaluator.evalLike` currently does `Pattern.compile(likeToRegex(pat), Pattern.DOTALL).matcher(s).matches()` per row. Cache by the SQL pattern string. For `%literal%` / `literal%` / `%literal` / `literal` → use `String.contains` / `startsWith` / `endsWith` / `equals`, skip regex entirely.
   **Reach:** Q21-24, Q33 directly (~5.5 s of 11.8 s). Expect ×20-100 on those.

2. **String dict: return cached `String` per dict id instead of `new String(bytes)`** — a few hours.
   Today `StringColumn.valueAsString` decodes UTF-8 from the segment each call. In DICT mode, build a `String[]` lazily populated on first lookup (O(dictSize) once). Even better: for equality predicates and GROUP BY keys, compare dict ids directly and never decode.
   **Reach:** Q17, Q18, Q33, Q34, Q35, Q38 and all string GROUP BYs. 15 % of total CPU plus secondary GC relief.

3. **Vector batches** (4096 rows, typed arrays + null mask) — the big one, days of work, foundation for SIMD/parallel later.
   Kills `Long.valueOf` / boxing in evaluators, lets `isNullAt` become a word-wise bit-scan, lets `Aggregator.accept` become `acceptBatch(long[], int)`, unlocks `jdk.incubator.vector`.
   **Reach:** everything, ×3-5 base, more with SIMD.

4. **Open-addressing hash agg** with primitive long keys — couple of days.
   Replaces `java.util.HashMap` for group state. Removes boxing + better cache locality.
   **Reach:** all GROUP BY queries, ×2-3.

5. **Native `DATE` / `DATETIME` column types** (i32 days / i64 micros) — a day or two.
   Removes string parsing in `EXTRACT` and `DATE_TRUNC`; comparisons become primitive.
   **Reach:** Q19, Q43 primarily, ×5-10.

6. **Parallel scan** (ForkJoinPool over ~64K-row chunks) — once batches exist.
   DuckDB uses all cores by default; this is why the 100k→1M scaling got *worse* for us. Parallelism is where we close the absolute gap to DuckDB on our machine.

After (1), (2), and (5) alone we should see JPointDB sum drop from ~11.8 s to ~4-5 s. After vector batches and parallel scan on top, targeting sub-second total against DuckDB's ~150 ms is realistic.
