# Agent notes for JPointDB

Practical guide for AI agents (Claude Code, Codex, etc.) picking up this repo. Read [`README.md`](README.md) first for the elevator pitch; this file is about *how to work here*.

## Project goals

- Personal analytical DB in **Java 25**. Beat most ClickBench entries (performance is Stage 3 — not done yet).
- **Definition of Done** for correctness: 43/43 ClickBench queries match DuckDB on a 100k-row sample. Current state: passing.
- **Minimum third-party dependencies.** Runtime: JLine only. Tests: JUnit 5 + DuckDB CLI (external tool, not a Java dep).

## Hard rules

- **Java 25 only.** Toolchain is pinned; no polyfills for older JVMs.
- **No new runtime deps without discussion.** The stack is deliberately thin.
- **JSpecify null-safety on main sources.** NullAway runs in `./gradlew check` — breaking it breaks the build. Tests are exempt.
- **Russian for human conversation with the owner; English for all code, commits, identifiers, comments.**
- **Don't add comments that restate code.** Comments explain *why*, not *what*.
- **Don't write docs (*.md) unless asked.** `README.md`, `AGENTS.md`, `BACKLOG.md` are the only ones.

## Layout

```
core/    storage, SQL parser/binder/executor, TSV convert, JSON
server/  REST (HttpServer) + static web UI at src/main/resources/web/
cli/     JLine REPL
bench/   ClickBench goldens — DuckDB reference results committed under bench/golden/results/
gradle/  checkstyle.xml, eclipse-formatter.xml, spotbugs-exclude.xml, dependency-check-suppressions.xml
```

## Commands

```bash
./gradlew check                     # full local gate (Spotless, Checkstyle, ErrorProne+NullAway, SpotBugs, tests, JaCoCo)
./gradlew :core:test                # fast inner loop
./gradlew :bench:test               # ClickBench correctness (requires bench/golden/results/ populated first)
./gradlew pitest                    # PIT mutation testing; long
./gradlew dependencyCheckAnalyze    # OWASP; long, downloads NVD

./bench/scripts/setup-golden.sh     # fetch hits.tsv + regenerate DuckDB golden results
./start-server.sh                   # mmap sample and serve on :8080
./cli.sh                            # JLine REPL against :8080
```

## Tooling versions — context for debugging

- **Spotless** uses Eclipse JDT 4.33. google-java-format and palantir both crash on Java 25's `Log$DeferredDiagnosticHandler` API change — don't swap the formatter.
- **JaCoCo** 0.8.13 minimum (earlier can't parse class-file major 69).
- **SpotBugs** 4.9.3 with `org.ow2.asm:9.8` forced via `resolutionStrategy` — bundled ASM is too old for Java 25 bytecode.
- **PIT** 1.21.0 (bundles ASM 9.9). Config passes `--add-modules=jdk.incubator.vector` so the vector-API smoke test survives.
- **NullAway** 0.12.3 in JSpecify mode, `AnnotatedPackages=io.jpointdb`. Disabled for `compileTestJava`.

## Conventions worth preserving

- **`@Nullable` is TYPE_USE** — on arrays, write `long @Nullable []`, not `@Nullable long[]`. NullAway won't infer otherwise.
- **SQL engine** is hand-rolled: `Tokenizer` → `SqlParser` (recursive descent) → `Binder` → `BoundAst` (sealed) → `Executor` (row-at-a-time).
  - Special forms in the parser: `EXTRACT(field FROM expr)`, `DATE_TRUNC(precision, ts)`.
  - `REGEXP_REPLACE` translates Perl-style `\1` to Java's `$1`, caches compiled patterns.
- **Storage:** columns are one file each inside `*.jpdb/`. Strings default to DICT; converter falls back to RAW on cardinality overflow. Validity bitmap is separate, Arrow-style (1 = valid).
- **CLI meta-commands** (`\s`, `\c`, `\q`): JLine's `DefaultParser` treats `\` as escape — we clear it via `parser.setEscapeChars(null)`. Locked in by `CliE2ETest`.
- **ClickBench goldens with `ORDER BY COUNT(*) DESC LIMIT N`** need deterministic tie-breakers. Six queries in `bench/golden/queries.sql` have a secondary key appended (q11/q17/q18/q22/q31/q32). Don't remove them without replacing the tie-break.
- **Empty TSV field vs NULL:** `\N` = NULL, `` (empty) = empty string. DuckDB golden loader uses `NULLSTR '\N'` to match.

## Before you change the SQL engine

- Add a golden-equivalent test first. For anything that changes result rows, regenerate the affected `bench/golden/results/q##.tsv` via DuckDB (`bench/scripts/setup-golden.sh`), don't hand-edit.
- Keep `BoundAst` sealed — exhaustive `switch` is how the executor stays honest.

## What's next

See [`BACKLOG.md`](BACKLOG.md). Highlights:
- **Stage 3 performance** (the big one): vectorized batches (4096 rows) + `jdk.incubator.vector` SIMD for filters/aggregates, parallel scan via ForkJoinPool, open-addressing hash aggregator, off-heap hot-path buffers.
- **Native DATE/DATETIME** (today strings are parsed each time — 10–20× speedup available).
- **JOIN** (not in ClickBench, needed for anything real).
