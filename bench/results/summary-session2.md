# JPointDB perf — session 2 summary

ClickBench 43 queries on 1M `hits.tsv`, single JVM, DuckDB 1.5.1 as reference.

Starting point (end of session 1): 5074 ms total, 45× DuckDB.

## Progression

| step | JPointDB total | DuckDB | ratio | Δ |
|------|-----:|-----:|-----:|-----:|
| session 1 final | 5074 ms | 114 | 45× | — |
| (1) Parallel scan via ForkJoinPool | 2379 | 117 | 20× | **−53 %** |
| (2) Open-addressing hash aggregator (1/2-col int) | 1959 | 117 | 17× | **−18 %** |
| (3) Top-k heap + deferred SELECT materialize | 1107 | 117 | 9.45× | **−43 %** |
| (4) Per-row column-read cache (CSE) | 1002 | 117 | 8.55× | **−10 %** |
| (5) Pre-size agg map to avoid merge rehashes | 975 | 118 | 8.25× | **−3 %** |
| (6) DICT STRING as primitive-key GROUP BY | 910 | 118 | 7.70× | **−7 %** |
| (7) Batch-fused grand-total SUM (Q30 algebra) | **801** | 113 | **7.1×** | **−12 %** |

Cumulative: 11 808 → 801 ms, **−93.2 %**, **14.7× speedup** from baseline.

## Fixes in detail

### (1) Parallel scan — biggest single jump of the session
Split row range across `ForkJoinPool.commonPool()` in ~16 chunks for tables
≥ 100 k rows. Each chunk builds its own per-chunk state; main thread does
sequential merge. Added `Aggregator.merge(Aggregator)` to every aggregator
kind so chunks can be folded back. Q29 REGEXP 7×, Q30 4.4×, Q40 2×.

### (2) Open-addressing hash aggregator
`LongKeysAggMap`: primitive-long keys (1 or 2 wide), linear probing,
load 0.7. Replaces `HashMap<List<Object>, Aggregator[]>` when GROUP BY is
one or two `BoundColumn` of I32/I64 — sidesteps `Arrays.asList`, boxing,
and `List.hashCode`/`equals` traversal per row. Q33 1229 → 865 ms (−30 %).

### (3) Top-k min-heap + deferred SELECT
Restructured the post-aggregation path:

```
groups → HAVING + ORDER BY vals → selectTopK → apply offset → materialize SELECT
```

For a query with 1 M groups and `LIMIT 10`, SELECT items are evaluated for
~10 winners instead of all 1 M. Q33 865 → 204 ms (−76 %). Q19, Q34, Q35
all picked up big wins too.

### (4) Per-row column-read cache
`ExprEvaluator` caches the decoded value per column by row id (sentinel
`Long.MIN_VALUE` for "not cached"). Q30 reads `ResolutionWidth` in all
90 SUMs and Q36 reads `ClientIP` in 4 GROUP BY exprs — now only one
segment read per row. Q30 270 → 177 ms, Q36 32 → 26 ms.

### (5) Pre-size agg map
`LongKeysAggMap(width, capacityHint)` plus using `sum(chunk.size)` as the
hint for the merged map. Q33's sequential merge of 1 M unique keys no
longer pays ~15 power-of-two rehashes.

### (6) DICT STRING GROUP BY
`KeyKind.DICT_STRING` in the primitive path. `StringColumn.idAt(row)`
feeds the primitive hash directly; `Dictionary.stringAt(id)` converts
back once per *winner* at materialize time. Q34 URL 55 → 22 ms, Q37 URL
30 → 23 ms.

### (7) Batch-fused grand-total SUM
`detectGrandTotalSum`: matches every agg being `SUM(col)` or
`SUM(col + lit)` over the same non-nullable I32 column. Exploits the
identity `SUM(col + k) = SUM(col) + count * k` to collapse 90 sweeps of
the column into one. `I32Column.readInts` reads 4096 ints at once via
`MemorySegment.copy`; HotSpot auto-vectorizes the horizontal add.
**Q30 178 → 0.37 ms, now faster than DuckDB** (0.4 ms).

## Where the remaining 801 ms goes

| query | ms | notes |
|-------|---:|:--|
| Q33 (WatchID, ClientIP, 1 M unique groups, LIMIT 10 OFFSET 10 000) | 209 | sequential hash merge, boxing on materialize |
| Q19 (UserID, extract(minute), SearchPhrase — 3-col group) | 82 | hits generic path; no width-3 primitive |
| Q40 (5-col GROUP BY with CASE) | 60 | generic path |
| Q35 (literal + URL — generic path because literal isn't BoundColumn) | 51 | could rewrite to drop the degenerate key col |
| Q29 (REGEXP_REPLACE) | 40 | regex engine itself |
| Q36 (ClientIP, -1, -2, -3 — 4-col arithmetic GROUP BY) | 29 | generic path |
| Q37-38 (URL/Title filter) | 25 | generic post-agg — filter-then-group |

Tried but didn't land (kept the code reverted):
- Parallel tree-reduction of chunk maps — overhead outweighed the win
  for Q33 even with pre-sized destinations.
- Lazy boxing of primitive group keys — neutral; the allocation was
  already cheap under JIT.

## What's next

- **Vector batches (4096 rows, typed primitive arrays + null mask).** Most
  remaining cost is per-row Object boxing in accept/eval and per-row
  hash lookup. Q30 already proved the batch pattern is enormously faster
  when the aggregate is algebraically separable; generalising that into
  a proper batch executor is the structural fix for Q33 and Q19.
- **Width-3+ primitive key path.** Q19/Q36/Q40 all fall through to the
  generic HashMap path purely because the GROUP BY has 3+ columns. A
  `long[]`-flat key with open addressing would pick them up.
- **Parallel post-scan top-k.** `Q33`'s top-k heap over 1 M entries is
  still single-threaded; partial heaps per chunk merged sequentially
  would cut it.

Commits this session (newest first):
```
93676ed Batch-fused grand-total SUM over I32 column (Q30 pattern)
1e907ef Extend primitive-key GROUP BY to DICT-encoded STRING columns
2d1a12d Pre-size LongKeysAggMap to skip merge-time rehashes
c34a5ed Per-row column-read cache in ExprEvaluator
6a2c45e Top-k min-heap + deferred SELECT materialize for aggregate queries
28017c2 Open-addressing hash aggregator for integer GROUP BY
48c4a84 Parallel scan via ForkJoinPool: fan out row range across cores
```

Current results in `bench/results/compare-duckdb.md`.
