# Q33 оптимизация: сессия 3 — сводная таблица

1 M строк `hits.tsv`, DuckDB 1.5.1 referee, min-of-3 per run, 5 fresh-JVM runs.

## Итог

| фаза | total | Q33 | замечание |
|---|---:|---:|---|
| baseline | 800 ms | 207.6 ms | sequential re-probe, boxed accept |
| +opt-1 | 802 ms | 206.5 ms | `occupiedSlots[]` скрывает `probe2` в iteration |
| +opt-2 | 805 ms | 197.9 ms | `Aggregator.acceptLong` + 2 inline primitive ops |
| +opt-3 final | **722 ms** | **172.6 ms** | + precompiled `OrderResolver`s |
| ∆ total | **−78 ms (−9.8%)** | **−35 ms (−17%)** | |
| — | — | — | — |
| +ZGC (revert) | 835 ms | 171.0 ms | Q33 выигрывает, прочие теряют — откат |
| +inline primstate (revert) | 888 ms | 300.8 ms | regressed — переносит аллоки на materialize, не устраняет |

## Per-query на горячих запросах

| query | baseline | +opt-1 | +opt-2 | +ZGC | +inline | +opt-3 final |
|---|---:|---:|---:|---:|---:|---:|
| Q3 (3 простых agg, grand total) | 5.4 | 3.4 | 3.6 | 3.7 | 3.5 | **3.2** |
| Q16 (UserID GROUP BY) | 7.1 | 6.6 | 7.0 | 6.9 | 6.8 | **5.8** |
| Q19 (3-col, extract minute) | 82.0 | 86.3 | 115.2 | 109.3 | 76.9 | **82.6** |
| Q21 (LIKE '%google%') | 7.6 | 8.0 | 7.5 | 8.0 | 7.5 | **7.5** |
| Q30 (90 SUM) | 0.4 | 0.3 | 0.3 | 0.3 | 0.3 | **0.3** |
| **Q33 (1M group, LIMIT 10)** | 207.6 | 206.5 | 197.9 | **171.0** | 300.8 | **172.6** |
| Q34 (URL GROUP BY) | 24.3 | 19.2 | 19.2 | 23.3 | 23.7 | **18.1** |
| Q35 (literal + URL) | 49.9 | 55.5 | 52.9 | 63.1 | 48.3 | **51.0** |
| Q36 (ClientIP, -1, -2, -3) | 27.8 | 27.2 | 26.4 | 34.8 | 27.4 | **25.9** |
| Q37 (URL filter) | 24.0 | 24.2 | 21.4 | 24.2 | 23.9 | **19.0** |
| Q40 (TraficSrc/Search/Adv 5-col) | 59.4 | 58.6 | 58.0 | 66.4 | 56.3 | **54.3** |
| **TOTAL** | **800** | **802** | **805** | **835** | **888** | **722** |

## Flamegraph деградации хот-методов (Q33, ~20s профилирования)

| метод | baseline (5075) | +opt-1+opt-2 (4508) | +opt-3 (5093) |
|---|---:|---:|---:|
| `LongKeysAggMap.probe2` | **13.3 %** (676) | 8.2 % (370) | 6.7 % (339) |
| `Executor.acceptAggs` (boxed) | **10.9 %** (552) | 1.2 % (53) | 0 % |
| `LongKeysAggMap.forEachKey2` | 10.2 % (520) | 9.5 % (428) | 9.7 % (492) |
| `LongKeysAggMap.getOrCreate2` | 6.7 % (341) | 8.1 % (363) | 8.6 % (440) |
| `LongKeysAggMap.merge` | 5.3 % (271) | 8.5 % (383) | 7.1 % (361) |
| `Aggregator.create` / create lambda | 3.7 % (189) | 7.1 % (322) | 8.3 % (422) |
| `SumLong.accept` | 1.5 % (78) | 0 % | 0 % |
| `Executor.findMatch` (ORDER BY path) | 0 % | **1.8 %** (79) | **0 %** |

## Применённые оптимизации

**opt-1 · `occupiedSlots[]` iteration** ([commit 459f3f2](..)):
LongKeysAggMap хранил insertion order в `orderKeys[]` (плоский массив ключей).
`forEachKey` и `merge` re-probing'ом находили slot для каждого ключа —
~1M вызовов `probe2` на запрос Q33. Заменил на `int[] occupiedSlots`,
записываемый при insert; iteration сразу идёт в `values[slot]`.
`probe2` самплы −45 %.

**opt-2 · primitive `acceptLong`/`acceptDouble`** ([commit 3f2d35f](..)):
Добавил fast-path методы в Aggregator подклассы (CountStar, Count, SumLong,
SumDouble, Avg, MinMax), принимающие примитивы. `resolvePrimitiveArgAccepts`
в `scanAggChunkPrimitive` строит per-agg лямбду, читающую из I32/I64/F64
колонки напрямую и вызывающую `acceptLong/Double` — минуя `Long.valueOf`
и `((Number)v).longValue()`. Boxed `acceptAggs` почти полностью вытеснен.

**opt-3 · precompiled ORDER BY resolvers** ([commit 61b7a66](..)):
`finalizeAggregated` вычислял ORDER BY values через `evalPostAgg`, которая
делает `findMatch(groupExprs)` + `sameAgg(aggs)` линейными поисками.
Для Q33 это 3 × 1M × 5 ≈ 15M compare-calls. Теперь `compileOrderResolver`
разрешает каждое ORDER BY-выражение один раз в замыкание: `key.get(N)` или
`states[N].result()` — O(1) вместо O(orderBy × (groupExprs + aggs))
на группу. `findMatch` из flame исчез.

## Отброшенные опции

**Generational ZGC** (`-XX:+UseZGC -Xms1g -Xmx4g`):
Q33 улучшился на 18 ms (171 vs 189), но GC barrier overhead на коротких
запросах Q19/Q35/Q36/Q40 добавил суммарно 40+ ms → **net +30 ms** на total.
ClickBench — workload с короткими запросами и всплесками аллокаций; G1
справляется лучше в среднем.

**Inline primitive state (`PrimitiveAggMap`)**:
Заменил `Aggregator[][] values` на flat `long[]`/`double[]` per-field
индексированные слотами. Scan-loop получил прямой `longFields[idx][slot]++`
без создания Aggregator объектов (убрал 3M аллокаций). НО: `finalizeAggregated`
создавал `PrecomputedAggregator` wrapper для КАЖДОЙ группы, чтобы совместимо
передать в `evalPostAgg`. Это перенесло те же 3M аллокаций со scan на
materialize. **Q33 вырос до 301 ms.** Откачен; для реального выигрыша нужен
специализированный `finalizeAggregated` с top-k по slot-индексам — это
отдельный большой рефакторинг.

**Parallel tree-merge карт**:
`mergeChunksParallel` с pre-sized destinations — 16→8→4→2→1 merge phases.
Overhead ForkJoinPool submission + cache-ping пересилил выигрыш на Q33
(1M ключей): Q33 174 → 234 ms. Откачен.

**Heap pre-touch (`-Xms2g -XX:+AlwaysPreTouch`)**:
Стабилизировал измерения, но поднял medium total ~60 ms из-за памяти,
трогаемой JVM-ом при старте.

## Выводы

1. **Основная оставшаяся стоимость Q33** — 8M аллокаций per-запрос
   (3M Aggregator + 1M Aggregator[] + 2M boxed Long + 1M Object[] +
   1M Arrays.asList + 1M GroupEntry). Убрать их требует структурной
   замены `finalizeAggregated` на slot-based top-k, а не только scan-loop.

2. **ZGC не win для workload с короткими запросами** — barrier overhead
   делает медианный total хуже, даже если Q33 становится ровнее.

3. **Noise floor** нашей setup'ы: ~60-90 ms на total ClickBench. Любая
   оптимизация меньше ~30 ms на Q33 тонет в run-to-run jitter.

4. **Финальное ускорение Q33 в этой сессии: −35 ms (−17 %)**, total
   −78 ms (−10 %). Дальнейшие 30-40 % на Q33 возможны только структурным
   переходом на vector-batched aggregators.

Artifacts:
- `bench/results/compare-duckdb-baseline.md`
- `bench/results/compare-duckdb-opt1.md`
- `bench/results/compare-duckdb-opt2.md`
- `bench/results/compare-duckdb-zgc.md` (откат)
- `bench/results/compare-duckdb-opt3-inline.md` (откат)
- `bench/results/compare-duckdb-final-v2.md` (финал)
- `bench/results/flame-q33-s2.html` (до)
- `bench/results/flame-q33-final.html` (после opt-1+opt-2)
- `bench/results/flame-q33-v3.html` (после opt-1+opt-2+opt-3)
- `bench/results/alloc-q33.html` (аллокационный профиль)

Commits:
```
61b7a66 Pre-compile ORDER BY resolvers in finalizeAggregated
3f2d35f Primitive-typed acceptLong/acceptDouble on simple aggregators
459f3f2 Refactor LongKeysAggMap iteration: occupiedSlots[] instead of orderKeys[]+re-probe
```
