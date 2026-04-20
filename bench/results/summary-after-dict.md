# Dict String cache — before / after

Fix: `Dictionary` now holds a lazily-populated `String[]` of size `dict.size()`.
`StringColumn.valueAsString` in DICT mode calls `dict.stringAt(id)`, which
decodes UTF-8 once per dict id and returns the cached reference on every
subsequent read. Data races on slot writes are benign — `keyAsString(id)`
is pure and `String` is immutable.

## ClickBench 1M totals

| stage | JPointDB sum | DuckDB sum | ratio |
|-------|-----:|-----:|-----:|
| baseline | 11 808 ms | 137 ms | 86× |
| after LIKE fix | 7 853 ms | 117 ms | 67× |
| **after dict cache** | **5 305 ms** | 114 ms | **47×** |
| delta vs baseline | **−55.1 %** | — | — |

## Per-query effect (dict cache only, not counting LIKE fix)

String-heavy queries — biggest wins:

| # | pre-dict ms | post-dict ms | speedup | query |
|---|-----:|-----:|-----:|-------|
| 38 | 470 | 135 | ×3.5 | `Title` GROUP BY with filter |
| 34 | 310 | 93 | ×3.3 | `URL` GROUP BY |
| 35 | 341 | 106 | ×3.2 | `1, URL` GROUP BY |
| 25 | 97 | 32 | ×3.0 | `SearchPhrase` ORDER BY EventTime |
| 26 | 98 | 31 | ×3.2 | `SearchPhrase` ORDER BY SearchPhrase |
| 27 | 90 | 33 | ×2.7 | `SearchPhrase` ORDER BY EventTime, SearchPhrase |
| 28 | 192 | 65 | ×3.0 | AVG(STRLEN(URL)) HAVING COUNT > 100000 |
| 29 | 785 | 308 | ×2.5 | REGEXP_REPLACE(Referer, …) GROUP BY |
| 37 | 404 | 162 | ×2.5 | URL GROUP BY with date filter |
| 40 | 460 | 173 | ×2.7 | CASE WHEN (Src = '' …) |
| 43 | 278 | 133 | ×2.1 | DATE_TRUNC + GROUP BY |

LIKE queries still benefit (URL is string-typed):

| # | pre-dict | post-dict | speedup |
|---|-----:|-----:|-----:|
| 21 | 103 | 55 | ×1.9 |
| 22 | 141 | 78 | ×1.8 |
| 23 | 378 | 153 | ×2.5 |
| 24 | 103 | 55 | ×1.9 |

Non-string queries mostly unchanged (Q30 arithmetic: 1760 → 1244, gets a
bit from EventDate reads).

## Q21 profile, three points in time

| bucket | baseline | after LIKE | after dict |
|--------|------:|------:|------:|
| `java.util.regex.Pattern` | **85.0 %** | 0.0 % | 0.0 % |
| `new String(bytes, UTF_8)` materialization | 11.0 % | **50.1 %** | **8.3 %** |
| `ExprEvaluator.eval` (JIT-inlined hot loop) | ~4 % | 37 % | **80 %** |
| `likeMatcherFor` ConcurrentHashMap lookup | — | 4.2 % | 6.2 % |

The remaining 80 % in `ExprEvaluator.eval` is the row-at-a-time dispatch
loop itself: virtual calls into readColumn → stringAt → contains intrinsic,
all inlined into one C2 compilation. This is the floor for the current
executor shape. Pushing below requires vector batches.

Flamegraph progression:
- [`flame-q21-like.html`](flame-q21-like.html) — baseline, regex-dominated
- [`flame-q21-like-fixed.html`](flame-q21-like-fixed.html) — after LIKE fix
- [`flame-q21-dict.html`](flame-q21-dict.html) — after dict cache

## What's left to do in hot paths without vector batches

1. **Bind-time `LikeMatcher` resolution** — move `LikeMatcher` into `BoundLike`
   at binder stage when pattern is a literal, eliminating the per-row
   `ConcurrentHashMap.get`. Saves the ~6 % still spent there. ~30 min.
2. **Native DATE/DATETIME** — Q19 `extract(minute FROM EventTime)` still
   245 ms, Q43 DATE_TRUNC 133 ms, Q28/38 date filters. All parse ISO
   strings every row. Native i32-days/i64-micros eliminates this. Large
   payoff on ~8 queries.
3. **Open-addressing hash aggregator** with primitive long keys — Q16/17
   UserID GROUP BY, Q34/35 URL GROUP BY. Current `java.util.HashMap`
   with boxed `Long` keys is visible in the JIT loop.
