# LIKE optimization — before / after

Fix: `ExprEvaluator.evalLike` now caches a `LikeMatcher` per SQL pattern.
For `literal`, `literal%`, `%literal`, `%literal%` (no `_` wildcards, no
embedded `%`) it short-circuits to `String.equals` / `startsWith` /
`endsWith` / `contains` — which HotSpot lowers to byte-level SIMD
intrinsics on Latin-1 strings. The fallback regex path compiles once per
distinct pattern and is reused across rows.

## ClickBench 1M totals

| | JPointDB sum | DuckDB sum | ratio |
|---|-----:|-----:|-----:|
| before | 11 808 ms | 137 ms | 86× |
| **after** | **7 853 ms** | 117 ms | **67×** |
| delta | **−3 955 ms (−33.5 %)** | — | — |

## Per-query effect on LIKE queries

| # | before ms | after ms | speedup |
|---|-----:|-----:|-----:|
| Q21 `WHERE URL LIKE '%google%'` | 621 | 103 | **6.0×** |
| Q22 `URL LIKE ... GROUP BY SearchPhrase` | 669 | 141 | 4.7× |
| Q23 `Title LIKE ... AND URL NOT LIKE ...` (two LIKEs) | 1 684 | 378 | 4.5× |
| Q24 `SELECT * WHERE URL LIKE ... ORDER BY EventTime LIMIT 10` | 621 | 103 | 6.0× |

LIKE queries alone: **3 595 ms → 725 ms (×5.0)**. They were 30 % of the
total; now they're under 10 %.

## CPU profile for Q21 before / after

Same workload (60× `SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'`,
1M rows), same sampler.

| bucket | before | after |
|--------|------:|------:|
| `java.util.regex.Pattern` | **85.0 %** | **0.0 %** |
| String materialize from dict | 11.0 % | **50.1 %** |
| SIMD intrinsics (`count_positives`, `StringLatin1`, byte copy) | — | 12.6 % |
| `ExprEvaluator.eval` dispatch / other | 4.0 % | 37.3 % |

Flamegraphs:
- before: [`bench/results/flame-q21-like.html`](flame-q21-like.html)
- after: [`bench/results/flame-q21-like-fixed.html`](flame-q21-like-fixed.html)

## What's next

Q21's new dominant cost — 50 % in `StringColumn.valueAsString` →
`new String(bytes, UTF_8)` — is fix #2 in the plan: cache decoded strings
per dict id in DICT mode, or compare dict ids directly for equality and
`contains` on a literal. Expected impact: another ~40 % off the queries
that read string columns (Q17, Q18, Q22-24, Q25-27, Q33-39).
