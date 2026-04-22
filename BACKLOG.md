# JPointDB — Backlog

Единый список всего отложенного. Источники:
1. **Quality Tier 2** — инструменты, которые пока ломаются об Java 25 bytecode.
2. **Quality Tier 3** — низкоприоритетные линтеры.
3. **Stage 3 — performance** — всё что нужно для «обгоняем большинство БД ClickBench».
4. **Отложенные дыры движка** — вскрытые при разработке, не блокируют 43/43 ClickBench.
5. **Удобства dev-цикла** — скрипты, docs, observability.

---

## 🟢 Tier 3 Quality Gates

### SonarCloud / SonarQube — вне scope
- Service, не local-tool. Имеет смысл подключить, если проект станет публичным в OSS. Покрытие уже собирается JaCoCo и может быть заlifted в Sonar когда понадобится.

### Checker Framework — избыточен
- Более мощный, чем NullAway, но в ~5× больше церемонии. Включать только если обнаружатся реальные инварианты, которые нельзя выразить иначе (например, нульness-трекинг с дженериками, unit-типы для размерностей).

### JDepend / Structure101 / jqassistant — архитектурные
- Имеет смысл при ≥10 модулях. Сейчас 4 (core/server/cli/bench) — правила "нет циклов между модулями" обеспечены структурой Gradle.

---

## ✅ Уже подключено (бывший Tier 2)

| Инструмент | Версия | Как запускать | Заметка |
|-----------|--------|---------------|---------|
| **SpotBugs** | 4.9.3 (+ ASM 9.8 forced) | в `./gradlew check` | Confidence=HIGH, `gradle/spotbugs-exclude.xml` исключает тесты |
| **PIT mutation testing** | 1.21.0 | `./gradlew pitest` (долго, не в check) | Текущий baseline core: **71% killed, 87% test strength** |
| **OWASP Dependency-Check** | 11.1.1 | `./gradlew dependencyCheckAnalyze` (долго — качает NVD ≈800 МБ в первый раз; не в check) | CVSS ≥ 7 → fail; `gradle/dependency-check-suppressions.xml` для false positives. С NVD API key быстрее. |
| **NullAway** | 0.12.3 (над ErrorProne) | в `./gradlew check` (main sources only) | JSpecify-mode, `AnnotatedPackages=io.jpointdb`. Записано ~60 JSpecify-аннотаций `@Nullable` в главных файлах. Отключено для testCompile — тесты много null-pokают. Любой новый NPE-сценарий в main ломает build. |

---

## 🔴 Stage 3 — Performance (следующая большая тема)

### Quick-wins из анализа разрыва с DuckDB (сессия 5)

После opt-4 (inline primitive state + slot top-k) total 649 ms / 119 ms DuckDB = **5.44×**. Разобрал per-query, отсортировал по ratio. Следующие точечные правки, каждая даёт >3% на total:

- [ ] **Width≥3 inline primitive GROUP BY** — Q17 (15×), Q19 (25×), Q40 (5×) все попадают в generic HashMap path. Обобщить `LongKeysAggMap` на N-колоночный `long[]`-ключ. Потенциал: −80 ms.
- [ ] **Нативный DATE / DATETIME (i32 days / i64 micros)** — сейчас EventDate/EventTime хранятся как STRING. Q07 (MIN/MAX EventDate 16×), Q31 (23×), Q37/38/41/43 все платят string-compare за фильтр `WHERE date BETWEEN ... AND ...`. Потенциал: −40 ms.
- [ ] **Radix-partitioned hash агрегатор** — Q33 всё ещё 58× slower (140 ms на 1M уникальных групп). DuckDB разбивает по хешу ключа на партиции → каждое ядро работает со своей без merge. Потенциал: −100 ms на Q33.
- [x] ~~**Literal в GROUP BY → игнорить при shape detection**~~ — Done в коммите `11c4581`. Q35 48 → 11 ms. Total −56 ms. `PrimitiveKeyShape` теперь с `exprToKey[]` и `literalValues[]` — ключ строится из column-components, литералы спличиваются на выходе.
- [ ] **Zone-maps (page-level min/max)** — при conversion записывать min/max на каждый 4K-row блок в meta. `MIN/MAX(col)` за O(pages) вместо O(rows). Q07 (16×). Потенциал: −3 ms.
- [ ] **STRING ORDER BY → radix-sort на UTF-8 байтах** — Q25/Q27 (`ORDER BY EventTime LIMIT 10`) ~8-9× slower. `String.compareTo` на 19-символьных timestamp'ах vs int64-micros radix у DuckDB. Потенциал: −30 ms.

Суммарный потенциал: **−290 ms** → **~360 ms total** ≈ **3× DuckDB**. Подробный анализ разрыва — в `bench/results/summary-session3.md` и коммит `e6a8e90`.

### Векторизация исполнителя
- [ ] **Vector batches** (`Batch` с типизированными примитивными массивами + null-mask, размер 4096 строк).
- [ ] **Vector API / SIMD** через `jdk.incubator.vector`: фильтры по i32/i64/f64, агрегаты `sum`/`count`/`min`/`max`, compare-mask для строкового поиска разделителей.
- [ ] Переписать `Executor` с row-at-a-time на pull-based с батчами. `BoundExpr` → `VectorizedExpr` с подходящими реализациями.
- [ ] Off-heap через `Arena.ofShared` для промежуточных буферов (исключить GC из hot path).

### Параллелизм
- [ ] **ForkJoinPool для scan/filter**: разбивать колонку на блоки (по ~64K строк), пускать параллельно, мерджить result-set.
- [ ] Параллельный aggregate: `partial_agg` per-thread + reduce.
- [ ] Учёт NUMA для >1 socket машин (если актуально).

### Hash-агрегат
- [ ] Open-addressing hash-table с линейным probing, спец-версии под i32/i64/(i64,i64) ключи.
- [ ] Inline-строчные ключи (до 8 байт в машинном слове).
- [ ] Ring-buffer spilling для очень больших GROUP BY.

### Специализация горячих путей
- [ ] Замена `Object`-evaluator на code-gen (или руками расписанные per-type варианты) — убрать boxing в аккумуляторах.
- [ ] Inline-обработка `COUNT(*)` без eval-вызова.
- [ ] `LIKE '%literal%'` → `String.contains`/boyer-moore вместо `Pattern.compile`.
- [ ] `REGEXP_REPLACE` с литерал-паттерном → lazy compile, кеш уже есть.

### Storage-уровень
- [ ] **Переписать offsets для RAW-строк в off-heap streamable**: сейчас держим в `long[]` в памяти целиком, для 100M строк это 800МБ heap.
- [ ] **Поблочный dict**: 1-й проход может строить per-block словарь для экономии памяти во время конвертации (в обмен на more expensive read).
- [ ] Сжатие колонок: RLE для постоянных/почти-постоянных, Delta для монотонных i64 (UserID), LZ4 для строк при raw-encoding.
- [ ] Bloom-фильтры per-block для «negatively-selective» WHERE.
- [ ] Индекс min/max per-block (предикат-skip).
- [ ] Быстрый хеш в словаре (xxHash3 вместо `31*h+b`).

### IO / форматы
- [ ] `hits.jpdb` → опция compressed-format с LZ4/Zstd декодированием на лету.
- [ ] Parquet reader/writer — для interop с ClickBench-экосистемой.

### Профилирование
- [ ] async-profiler интеграция + `bench/scripts/profile.sh`.
- [ ] JMH-микробенчи для hot paths (tsv scan, dict build, filter eval).

---

## 🟡 Движок — отложенные фичи

### Типы
- [ ] Нативный **DATE** (i32 = days since epoch) и **DATETIME** (i64 = micros since epoch).  
  Сейчас — STRING, `DATE_TRUNC/EXTRACT` парсят строку. Нативные типы дадут 10-20× ускорение.
- [ ] **DECIMAL(p,s)** — если в реальных данных понадобятся точные десятичные.
- [ ] **BOOL** (сейчас не распознаётся sniffer-ом).
- [ ] **ARRAY** / **MAP** — далёкий план.

### SQL
- [ ] **JOIN** (hash-join для начала). ClickBench JOIN не использует, но для реального применения обязательно.
- [ ] **Subqueries** в `FROM`/`WHERE`.
- [ ] **UNION / INTERSECT / EXCEPT**.
- [ ] **Window functions** (`OVER`, `RANK`, `LAG`).
- [ ] **SELECT DISTINCT** на уровне statement.
- [ ] **ARRAY literals** и `any()`/`all()`.
- [ ] **Auto-expand `SELECT *`** при `JOIN` (сейчас один from-table, всё просто).

### Data model
- [ ] Множественные таблицы в одном `.jpdb`-каталоге (сейчас один `Table.open(dir)` = одна таблица).
- [ ] Append после конвертации (сейчас `.jpdb` immutable).
- [ ] Alter column: rename / drop / type cast.

---

## 🟡 REST / CLI / Web

- [ ] **Authentication / auth** (сейчас нет).
- [ ] **/query** стриминговый ответ (NDJSON) для больших result-set.
- [ ] **CORS** заголовки (если web UI станет внешним).
- [ ] **CLI**: `\timing` on/off, `\export` в TSV/JSON, `\watch` (повтор запроса каждые N сек).
- [ ] **Web UI**: tree-view план исполнения (explain), история запросов, pin queries.
- [ ] **Explain** команда — показывает физический план + оценки.

---

## 🟡 Tests

- [ ] **JMH-бенчи** — `bench/src/main/java/...Bench.java` под ClickBench.
- [ ] **Property-based** для парсера/lexer (jqwik).
- [ ] **Fuzzing** byte-level TSV scanner (jazzer).
- [ ] **Full ClickBench sample** — скрипт для автопрогона на 100M строк с таймингами.

---

## 🟢 Dev ergonomics

- [ ] **`./gradlew bench`** — wired к ClickBench-раннеру, чтобы одной командой получать table queries-taimings.
- [ ] **Devcontainer / nix** shell c Java 25 + DuckDB — чтобы setup-golden.sh работал «из коробки».
- [ ] **GitHub Actions** CI: `./gradlew check` на каждый PR, кеш Gradle.
- [ ] **Release automation** — сборка distzip с server+cli в один артефакт.
- [ ] **README.md** — сейчас нет; добавить краткое руководство + architecture.md.

---

## Что НЕ попало в backlog (принципиально)

- Миграция с row-at-a-time на иммедиейт SIMD без батчей — не имеет смысла, батчи прежде.
- Перевод на off-heap hash table сразу с открытой адресацией — хочется сначала замерить что узкое место hash, а не boxing.
- Собственный regex engine — стандартный `java.util.regex` достаточно быстр для ClickBench Q28.
- Поддержка non-Java 25 JVM — tradeoff: теряем Vector API и FFM, не стоит.
