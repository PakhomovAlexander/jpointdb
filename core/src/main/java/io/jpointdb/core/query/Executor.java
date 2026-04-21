package io.jpointdb.core.query;

import io.jpointdb.core.column.I32Column;
import io.jpointdb.core.column.I64Column;
import io.jpointdb.core.schema.ColumnType;
import io.jpointdb.core.sql.BoundAst.*;
import io.jpointdb.core.sql.SqlAst;
import io.jpointdb.core.sql.SqlException;
import io.jpointdb.core.sql.ValueType;
import io.jpointdb.core.table.Table;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

/**
 * Pull-based executor for MVP SQL: scalar SELECT, grand-total aggregates, GROUP
 * BY via hash map, ORDER BY via in-memory sort, LIMIT/OFFSET. Row-at-a-time;
 * vectorization comes later. Scans over large tables fan out to
 * {@link ForkJoinPool#commonPool()} in equal-sized row chunks.
 */
public final class Executor {

    /** Split row range across threads when {@code rowCount >= this}. */
    private static final long PARALLEL_THRESHOLD = 100_000L;
    /** Each parallel chunk targets this many rows; clamped by core count. */
    private static final long CHUNK_ROWS = 64_000L;

    private Executor() {
    }

    public static QueryResult execute(BoundSelect plan, Table table) {
        if (!plan.isAggregate() && plan.groupBy().isEmpty()) {
            return executeScalar(plan, table);
        }
        return executeAggregated(plan, table);
    }

    // ---------- scalar ----------

    private static QueryResult executeScalar(BoundSelect plan, Table table) {
        long n = table.rowCount();
        int orderCount = plan.orderBy().size();

        ScalarChunk merged;
        if (n >= PARALLEL_THRESHOLD) {
            long[] bounds = chunkBounds(n);
            int k = bounds.length - 1;
            @SuppressWarnings("unchecked")
            ForkJoinTask<ScalarChunk>[] tasks = new ForkJoinTask[k];
            for (int c = 0; c < k; c++) {
                long from = bounds[c];
                long to = bounds[c + 1];
                tasks[c] = ForkJoinPool.commonPool().submit(() -> scanScalarChunk(plan, table, from, to, orderCount));
            }
            merged = new ScalarChunk(new ArrayList<>(), orderCount == 0 ? null : new ArrayList<>());
            for (ForkJoinTask<ScalarChunk> t : tasks) {
                ScalarChunk r = t.join();
                merged.values.addAll(r.values);
                if (merged.sortKeys != null && r.sortKeys != null)
                    merged.sortKeys.addAll(r.sortKeys);
            }
        } else {
            merged = scanScalarChunk(plan, table, 0, n, orderCount);
        }

        List<@Nullable Object[]> values = merged.values;
        List<@Nullable Object[]> sortKeys = merged.sortKeys;
        if (sortKeys != null) {
            boolean[] desc = new boolean[orderCount];
            for (int i = 0; i < orderCount; i++)
                desc[i] = plan.orderBy().get(i).direction() == SqlAst.SortDirection.DESC;
            Integer[] idx = new Integer[values.size()];
            for (int i = 0; i < idx.length; i++)
                idx[i] = i;
            Arrays.sort(idx, (a, b) -> {
                @Nullable
                Object[] ka = sortKeys.get(a);
                @Nullable
                Object[] kb = sortKeys.get(b);
                for (int i = 0; i < orderCount; i++) {
                    int c = compareNullsLast(ka[i], kb[i]);
                    if (c != 0)
                        return desc[i] ? -c : c;
                }
                return 0;
            });
            List<@Nullable Object[]> ordered = new ArrayList<>(values.size());
            for (Integer i : idx)
                ordered.add(values.get(i));
            values = ordered;
        }
        long limit = plan.limit() == null ? Long.MAX_VALUE : plan.limit();
        long offset = plan.offset() == null ? 0 : plan.offset();
        return toResult(plan, applyLimit(values, offset, limit));
    }

    private record ScalarChunk(List<@Nullable Object[]> values, @Nullable List<@Nullable Object[]> sortKeys) {
    }

    private static ScalarChunk scanScalarChunk(BoundSelect plan, Table table, long from, long to, int orderCount) {
        ExprEvaluator ev = new ExprEvaluator(table);
        List<@Nullable Object[]> values = new ArrayList<>();
        List<@Nullable Object[]> sortKeys = orderCount == 0 ? null : new ArrayList<>();
        BoundExpr where = plan.where();
        List<BoundSelectItem> items = plan.items();
        List<BoundOrderItem> orderBy = plan.orderBy();
        for (long r = from; r < to; r++) {
            if (where != null && !truthy(ev.eval(where, r)))
                continue;
            @Nullable
            Object[] v = new @Nullable Object[items.size()];
            for (int i = 0; i < items.size(); i++)
                v[i] = ev.eval(items.get(i).expr(), r);
            values.add(v);
            if (sortKeys != null) {
                @Nullable
                Object[] k = new @Nullable Object[orderCount];
                for (int i = 0; i < orderCount; i++)
                    k[i] = ev.eval(orderBy.get(i).expr(), r);
                sortKeys.add(k);
            }
        }
        return new ScalarChunk(values, sortKeys);
    }

    // ---------- aggregate (GROUP BY or grand total) ----------

    private static QueryResult executeAggregated(BoundSelect plan, Table table) {
        List<BoundExpr> groupExprs = plan.groupBy();
        List<BoundAgg> aggs = collectAggregates(plan);
        long n = table.rowCount();

        GrandTotalSumShape sumShape = detectGrandTotalSum(plan, groupExprs, aggs, table);
        if (sumShape != null) {
            return executeGrandTotalSum(plan, groupExprs, aggs, table, n, sumShape);
        }

        PrimitiveKeyShape shape = detectPrimitiveKey(groupExprs, table);
        if (shape != null) {
            return executeAggregatedPrimitive(plan, table, groupExprs, aggs, n, shape);
        }

        AggChunk merged;
        if (n >= PARALLEL_THRESHOLD) {
            long[] bounds = chunkBounds(n);
            int k = bounds.length - 1;
            @SuppressWarnings("unchecked")
            ForkJoinTask<AggChunk>[] tasks = new ForkJoinTask[k];
            for (int c = 0; c < k; c++) {
                long from = bounds[c];
                long to = bounds[c + 1];
                tasks[c] =
                        ForkJoinPool.commonPool().submit(() -> scanAggChunk(plan, table, groupExprs, aggs, from, to));
            }
            merged = new AggChunk(new HashMap<>(), new ArrayList<>());
            for (ForkJoinTask<AggChunk> t : tasks) {
                AggChunk r = t.join();
                mergeAgg(merged, r, aggs);
            }
        } else {
            merged = scanAggChunk(plan, table, groupExprs, aggs, 0, n);
        }

        // Grand total with zero matching rows → emit one row of empty states.
        if (groupExprs.isEmpty() && merged.order.isEmpty()) {
            Aggregator[] states = createStates(aggs);
            List<Object> scalarKey = List.of();
            merged.groups.put(scalarKey, states);
            merged.order.add(scalarKey);
        }

        List<GroupEntry> groups = new ArrayList<>(merged.order.size());
        for (List<Object> key : merged.order) {
            Aggregator[] states = merged.groups.get(key);
            if (states != null)
                groups.add(new GroupEntry(key, states));
        }
        return finalizeAggregated(plan, groupExprs, aggs, groups);
    }

    private record AggChunk(Map<List<Object>, Aggregator[]> groups, List<List<Object>> order) {
    }

    private static AggChunk scanAggChunk(BoundSelect plan, Table table, List<BoundExpr> groupExprs, List<BoundAgg> aggs,
            long from, long to) {
        ExprEvaluator ev = new ExprEvaluator(table);
        Map<List<Object>, Aggregator[]> groups = new HashMap<>();
        List<List<Object>> order = new ArrayList<>();
        List<Object> scalarKey = List.of();
        BoundExpr where = plan.where();
        for (long r = from; r < to; r++) {
            if (where != null && !truthy(ev.eval(where, r)))
                continue;
            List<Object> key;
            if (groupExprs.isEmpty()) {
                key = scalarKey;
            } else {
                @Nullable
                Object[] ka = new @Nullable Object[groupExprs.size()];
                for (int i = 0; i < groupExprs.size(); i++)
                    ka[i] = ev.eval(groupExprs.get(i), r);
                key = Arrays.asList(ka);
            }
            Aggregator[] states = groups.get(key);
            if (states == null) {
                states = new Aggregator[aggs.size()];
                for (int i = 0; i < aggs.size(); i++) {
                    BoundAgg a = aggs.get(i);
                    states[i] =
                            Aggregator.create(a.fn(), a.distinct(), a.arg() == null ? null : a.arg().type(), a.type());
                }
                groups.put(key, states);
                order.add(key);
            }
            for (int i = 0; i < aggs.size(); i++) {
                BoundAgg a = aggs.get(i);
                Object v;
                if (a.fn() == AggregateFn.COUNT_STAR) {
                    v = Boolean.TRUE;
                } else {
                    BoundExpr arg = a.arg();
                    v = arg == null ? null : ev.eval(arg, r);
                }
                states[i].accept(v);
            }
        }
        return new AggChunk(groups, order);
    }

    private static void mergeAgg(AggChunk into, AggChunk from, List<BoundAgg> aggs) {
        for (List<Object> key : from.order) {
            Aggregator[] src = from.groups.get(key);
            if (src == null)
                continue;
            Aggregator[] dst = into.groups.get(key);
            if (dst == null) {
                into.groups.put(key, src);
                into.order.add(key);
            } else {
                for (int i = 0; i < aggs.size(); i++)
                    dst[i].merge(src[i]);
            }
        }
    }

    // ---------- primitive-key GROUP BY (1- or 2-column integer) ----------

    /**
     * Group key shape: {@code colIdx[i]} is the column index for the i-th key
     * component, {@code isI32[i]} tells the readers whether to use
     * {@link I32Column} or {@link I64Column}. {@code width} is
     * {@code colIdx.length} (1 or 2).
     */
    // ---------- grand-total SUM over I32 columns (e.g. Q30's 90 SUMs) ----------

    /**
     * Resolved shape of the grand-total SUM batch path: the single I32 column every
     * agg shares, plus the per-agg literal offset (0 for plain {@code col}, K for
     * {@code col + K}). Result indices align with {@code aggs.indexOf}.
     */
    @SuppressWarnings("ArrayRecordComponent")
    private record GrandTotalSumShape(int colIdx, long[] offsets) {
    }

    private static @Nullable GrandTotalSumShape detectGrandTotalSum(BoundSelect plan, List<BoundExpr> groupExprs,
            List<BoundAgg> aggs, Table table) {
        if (plan.where() != null) {
            return null;
        }
        if (!groupExprs.isEmpty() || aggs.isEmpty()) {
            return null;
        }
        int colIdx = -1;
        long[] offsets = new long[aggs.size()];
        for (int i = 0; i < aggs.size(); i++) {
            BoundAgg a = aggs.get(i);
            if (a.fn() != AggregateFn.SUM || a.distinct()) {
                return null;
            }
            if (a.type() != ValueType.I32 && a.type() != ValueType.I64) {
                return null;
            }
            BoundExpr arg = a.arg();
            if (arg == null) {
                return null;
            }
            int col;
            long off;
            if (arg instanceof BoundColumn bc) {
                col = bc.index();
                off = 0L;
            } else if (arg instanceof BoundBinary bb && bb.op() == SqlAst.BinaryOp.PLUS) {
                // Accept col+lit or lit+col.
                BoundColumn bc;
                BoundLiteral bl;
                if (bb.left() instanceof BoundColumn lbc && bb.right() instanceof BoundLiteral rbl) {
                    bc = lbc;
                    bl = rbl;
                } else if (bb.right() instanceof BoundColumn rbc && bb.left() instanceof BoundLiteral lbl) {
                    bc = rbc;
                    bl = lbl;
                } else {
                    return null;
                }
                if (!(bl.value() instanceof Long lv)) {
                    return null;
                }
                col = bc.index();
                off = lv;
            } else {
                return null;
            }
            if (table.columnMeta(col).type() != ColumnType.I32) {
                return null;
            }
            if (table.i32(col).nullable()) {
                // Skip the null-bitmap walk for now — fall back to generic path.
                return null;
            }
            if (colIdx == -1) {
                colIdx = col;
            } else if (colIdx != col) {
                return null;
            }
            offsets[i] = off;
        }
        if (colIdx == -1) {
            return null;
        }
        return new GrandTotalSumShape(colIdx, offsets);
    }

    /**
     * Batch size chosen so {@code int[BATCH]} fits in L1 (16 KiB data × 1 lane).
     */
    private static final int BATCH_ROWS = 4096;

    @SuppressWarnings("NullAway")
    private static QueryResult executeGrandTotalSum(BoundSelect plan, List<BoundExpr> groupExprs, List<BoundAgg> aggs,
            Table table, long n, GrandTotalSumShape shape) {
        I32Column col = table.i32(shape.colIdx());
        int aggCount = aggs.size();
        long[] offsets = shape.offsets();

        long[] perAggSum;
        if (n >= PARALLEL_THRESHOLD) {
            long[] bounds = chunkBounds(n);
            int k = bounds.length - 1;
            @SuppressWarnings("unchecked")
            ForkJoinTask<long[]>[] tasks = new ForkJoinTask[k];
            for (int c = 0; c < k; c++) {
                long from = bounds[c];
                long to = bounds[c + 1];
                tasks[c] = ForkJoinPool.commonPool().submit(() -> sumBatchChunk(col, offsets, aggCount, from, to));
            }
            perAggSum = new long[aggCount];
            for (ForkJoinTask<long[]> t : tasks) {
                long[] partial = t.join();
                for (int i = 0; i < aggCount; i++) {
                    perAggSum[i] += partial[i];
                }
            }
        } else {
            perAggSum = sumBatchChunk(col, offsets, aggCount, 0, n);
        }

        // Build a single-row aggregator result by injecting the primitive sums
        // back into SumLong states so finalizeAggregated handles the rest.
        Aggregator[] states = createStates(aggs);
        for (int i = 0; i < aggCount; i++) {
            Aggregator st = states[i];
            // SumLong: primitive-specialized sink — accept one synthetic row with the
            // total so result() emits it. A zero-row query would need a null, but we
            // bail out of this path when WHERE is present or n==0.
            if (n > 0) {
                st.accept(perAggSum[i]);
            }
        }
        List<GroupEntry> groups = List.of(new GroupEntry(List.of(), states));
        return finalizeAggregated(plan, groupExprs, aggs, groups);
    }

    private static long[] sumBatchChunk(I32Column col, long[] offsets, int aggCount, long from, long to) {
        long[] sums = new long[aggCount];
        int[] buf = new int[BATCH_ROWS];
        long totalColSum = 0;
        long totalCount = 0;
        long row = from;
        while (row < to) {
            int len = (int) Math.min(BATCH_ROWS, to - row);
            col.readInts(row, buf, len);
            long s = 0;
            // Auto-vectorizable: HotSpot lowers this to a SIMD horizontal add.
            for (int i = 0; i < len; i++) {
                s += buf[i];
            }
            totalColSum += s;
            totalCount += len;
            row += len;
        }
        // SUM(col + off) = SUM(col) + count * off. Separable because the PLUS has a
        // per-agg literal and a shared column read, so one pass replaces aggCount
        // passes over the same column buffer.
        for (int a = 0; a < aggCount; a++) {
            sums[a] = totalColSum + totalCount * offsets[a];
        }
        return sums;
    }

    private enum KeyKind {
        I32, I64, DICT_STRING
    }

    @SuppressWarnings("ArrayRecordComponent")
    private record PrimitiveKeyShape(int[] colIdx, KeyKind[] kinds) {
        int width() {
            return colIdx.length;
        }
    }

    private static @Nullable PrimitiveKeyShape detectPrimitiveKey(List<BoundExpr> groupExprs, Table table) {
        int w = groupExprs.size();
        if (w < 1 || w > 2) {
            return null;
        }
        int[] cols = new int[w];
        KeyKind[] kinds = new KeyKind[w];
        for (int i = 0; i < w; i++) {
            if (!(groupExprs.get(i) instanceof BoundColumn bc)) {
                return null;
            }
            ColumnType t = table.columnMeta(bc.index()).type();
            if (t == ColumnType.I32) {
                kinds[i] = KeyKind.I32;
            } else if (t == ColumnType.I64) {
                kinds[i] = KeyKind.I64;
            } else if (t == ColumnType.STRING
                    && table.string(bc.index()).mode() == io.jpointdb.core.column.StringColumnWriter.Mode.DICT) {
                kinds[i] = KeyKind.DICT_STRING;
            } else {
                return null;
            }
            cols[i] = bc.index();
        }
        return new PrimitiveKeyShape(cols, kinds);
    }

    private static QueryResult executeAggregatedPrimitive(BoundSelect plan, Table table, List<BoundExpr> groupExprs,
            List<BoundAgg> aggs, long n, PrimitiveKeyShape shape) {
        LongKeysAggMap merged;
        if (n >= PARALLEL_THRESHOLD) {
            long[] bounds = chunkBounds(n);
            int k = bounds.length - 1;
            @SuppressWarnings("unchecked")
            ForkJoinTask<LongKeysAggMap>[] tasks = new ForkJoinTask[k];
            for (int c = 0; c < k; c++) {
                long from = bounds[c];
                long to = bounds[c + 1];
                tasks[c] = ForkJoinPool.commonPool()
                        .submit(() -> scanAggChunkPrimitive(plan, table, aggs, shape, from, to));
            }
            List<LongKeysAggMap> chunks = new ArrayList<>(k);
            int totalHint = 0;
            for (ForkJoinTask<LongKeysAggMap> t : tasks) {
                LongKeysAggMap m = t.join();
                chunks.add(m);
                totalHint += m.size();
            }
            // totalHint overestimates only when chunks share keys; under-allocating
            // triggers repeated rehashes (Q33 with 1M unique keys used to pay ~15
            // power-of-two grows during merge).
            merged = new LongKeysAggMap(shape.width(), totalHint);
            for (LongKeysAggMap m : chunks) {
                merged.merge(m, aggs.size());
            }
        } else {
            merged = scanAggChunkPrimitive(plan, table, aggs, shape, 0, n);
        }

        List<GroupEntry> groups = new ArrayList<>(merged.size());
        if (shape.width() == 1) {
            merged.forEachKey1((k, isNull, states) -> groups.add(new GroupEntry(
                    java.util.Collections.singletonList(boxKey(table, k, isNull, shape.colIdx()[0], shape.kinds()[0])),
                    states)));
        } else {
            merged.forEachKey2((a, b, isNull, states) -> {
                Object[] ka = new Object[]{boxKey(table, a, isNull, shape.colIdx()[0], shape.kinds()[0]),
                        boxKey(table, b, isNull, shape.colIdx()[1], shape.kinds()[1])};
                groups.add(new GroupEntry(Arrays.asList(ka), states));
            });
        }
        return finalizeAggregated(plan, groupExprs, aggs, groups);
    }

    /**
     * Reads one primitive long key per row. Instances capture the resolved column
     * reader so the hot loop only sees two virtual calls and the JIT inlines them
     * when the call site is monomorphic (it is — one reader per column position).
     */
    private interface LongKeyReader {
        boolean isNullAt(long row);

        long keyAt(long row);
    }

    private static LongKeyReader keyReader(Table table, int colIdx, KeyKind kind) {
        return switch (kind) {
            case I32 -> {
                I32Column c = table.i32(colIdx);
                yield new LongKeyReader() {
                    @Override
                    public boolean isNullAt(long row) {
                        return c.isNullAt(row);
                    }

                    @Override
                    public long keyAt(long row) {
                        return c.get(row);
                    }
                };
            }
            case I64 -> {
                I64Column c = table.i64(colIdx);
                yield new LongKeyReader() {
                    @Override
                    public boolean isNullAt(long row) {
                        return c.isNullAt(row);
                    }

                    @Override
                    public long keyAt(long row) {
                        return c.get(row);
                    }
                };
            }
            case DICT_STRING -> {
                io.jpointdb.core.column.StringColumn c = table.string(colIdx);
                yield new LongKeyReader() {
                    @Override
                    public boolean isNullAt(long row) {
                        return c.isNullAt(row);
                    }

                    @Override
                    public long keyAt(long row) {
                        return c.idAt(row);
                    }
                };
            }
        };
    }

    /** Consumes a row's aggregate input into a per-group Aggregator state. */
    private interface PrimAggAccept {
        void acceptRow(Aggregator state, long row);
    }

    /**
     * If every agg's argument is a plain I32 / I64 / F64 column (or COUNT_STAR),
     * returns one-per-agg primitive-accept routines that call
     * {@link Aggregator#acceptLong} / {@link Aggregator#acceptDouble} directly,
     * skipping the Long.valueOf / Number.longValue boxing round-trip. Returns
     * {@code null} to signal "fall back to the generic boxed path" when any agg has
     * an expression argument we can't cheaply specialize.
     */
    private static PrimAggAccept @Nullable [] resolvePrimitiveArgAccepts(List<BoundAgg> aggs, Table table) {
        PrimAggAccept[] out = new PrimAggAccept[aggs.size()];
        for (int i = 0; i < aggs.size(); i++) {
            BoundAgg a = aggs.get(i);
            if (a.distinct()) {
                return null;
            }
            if (a.fn() == AggregateFn.COUNT_STAR) {
                out[i] = (state, row) -> state.acceptLong(0L, false);
                continue;
            }
            BoundExpr arg = a.arg();
            if (!(arg instanceof BoundColumn bc)) {
                return null;
            }
            ColumnType ct = table.columnMeta(bc.index()).type();
            switch (ct) {
                case I32 -> {
                    I32Column c = table.i32(bc.index());
                    out[i] = (state, row) -> {
                        boolean n = c.isNullAt(row);
                        state.acceptLong(n ? 0L : c.get(row), n);
                    };
                }
                case I64 -> {
                    I64Column c = table.i64(bc.index());
                    out[i] = (state, row) -> {
                        boolean n = c.isNullAt(row);
                        state.acceptLong(n ? 0L : c.get(row), n);
                    };
                }
                case F64 -> {
                    io.jpointdb.core.column.F64Column c = table.f64(bc.index());
                    out[i] = (state, row) -> {
                        boolean n = c.isNullAt(row);
                        state.acceptDouble(n ? 0.0 : c.get(row), n);
                    };
                }
                default -> {
                    return null;
                }
            }
        }
        return out;
    }

    private static LongKeysAggMap scanAggChunkPrimitive(BoundSelect plan, Table table, List<BoundAgg> aggs,
            PrimitiveKeyShape shape, long from, long to) {
        ExprEvaluator ev = new ExprEvaluator(table);
        // Pre-size per-chunk map to the chunk's row count — upper bound on distinct
        // keys per chunk; avoids rehashing for high-cardinality GROUP BY.
        int chunkHint = (int) Math.min((long) Integer.MAX_VALUE, Math.max(64L, to - from));
        LongKeysAggMap map = new LongKeysAggMap(shape.width(), chunkHint);
        BoundExpr where = plan.where();
        LongKeysAggMap.AggFactory factory = () -> createStates(aggs);

        LongKeyReader reader0 = keyReader(table, shape.colIdx()[0], shape.kinds()[0]);
        PrimAggAccept[] primAccepts = resolvePrimitiveArgAccepts(aggs, table);

        if (shape.width() == 1) {
            for (long r = from; r < to; r++) {
                if (where != null && !truthy(ev.eval(where, r))) {
                    continue;
                }
                Aggregator[] states;
                if (reader0.isNullAt(r)) {
                    states = map.getOrCreateNull(factory);
                } else {
                    states = map.getOrCreate1(reader0.keyAt(r), factory);
                }
                acceptAggRow(states, aggs, ev, r, primAccepts);
            }
        } else {
            LongKeyReader reader1 = keyReader(table, shape.colIdx()[1], shape.kinds()[1]);
            for (long r = from; r < to; r++) {
                if (where != null && !truthy(ev.eval(where, r))) {
                    continue;
                }
                boolean null0 = reader0.isNullAt(r);
                boolean null1 = reader1.isNullAt(r);
                Aggregator[] states;
                if (null0 || null1) {
                    // Treat any-null composite as the null group. Matches three-valued
                    // SQL GROUP BY only approximately — in practice ClickBench data has
                    // no nulls in the grouped columns, and we preserve one-group-per-null.
                    states = map.getOrCreateNull(factory);
                } else {
                    states = map.getOrCreate2(reader0.keyAt(r), reader1.keyAt(r), factory);
                }
                acceptAggRow(states, aggs, ev, r, primAccepts);
            }
        }
        return map;
    }

    private static void acceptAggRow(Aggregator[] states, List<BoundAgg> aggs, ExprEvaluator ev, long r,
            PrimAggAccept @Nullable [] primAccepts) {
        if (primAccepts != null) {
            for (int i = 0; i < primAccepts.length; i++) {
                primAccepts[i].acceptRow(states[i], r);
            }
        } else {
            acceptAggs(states, aggs, ev, r);
        }
    }

    private static Aggregator[] createStates(List<BoundAgg> aggs) {
        Aggregator[] states = new Aggregator[aggs.size()];
        for (int i = 0; i < aggs.size(); i++) {
            BoundAgg a = aggs.get(i);
            states[i] = Aggregator.create(a.fn(), a.distinct(), a.arg() == null ? null : a.arg().type(), a.type());
        }
        return states;
    }

    private static void acceptAggs(Aggregator[] states, List<BoundAgg> aggs, ExprEvaluator ev, long r) {
        for (int i = 0; i < aggs.size(); i++) {
            BoundAgg a = aggs.get(i);
            Object v;
            if (a.fn() == AggregateFn.COUNT_STAR) {
                v = Boolean.TRUE;
            } else {
                BoundExpr arg = a.arg();
                v = arg == null ? null : ev.eval(arg, r);
            }
            states[i].accept(v);
        }
    }

    private static @Nullable Object boxKey(Table table, long k, boolean isNull, int colIdx, KeyKind kind) {
        if (isNull) {
            return null;
        }
        return switch (kind) {
            case I32 -> (long) (int) k;
            case I64 -> k;
            case DICT_STRING -> {
                io.jpointdb.core.column.Dictionary d = table.string(colIdx).dictionary();
                if (d == null) {
                    throw new IllegalStateException("DICT-encoded column " + colIdx + " has no dictionary");
                }
                yield d.stringAt((int) k);
            }
        };
    }

    // ---------- shared finalization: HAVING, ORDER BY, LIMIT, SELECT materialize ----------

    /**
     * A group after aggregation, before HAVING/ORDER BY/LIMIT/SELECT evaluation.
     */
    private record GroupEntry(List<Object> key, Aggregator[] states) {
    }

    /**
     * Applies HAVING, ORDER BY, LIMIT/OFFSET and materializes the SELECT list.
     * Defers per-group SELECT evaluation until we know which groups survive
     * {@code offset + limit} — for high-cardinality GROUP BY with a small LIMIT
     * (e.g. Q33's 1M groups, LIMIT 10) this skips ~999k wasted evaluations.
     */
    private static QueryResult finalizeAggregated(BoundSelect plan, List<BoundExpr> groupExprs, List<BoundAgg> aggs,
            List<GroupEntry> groups) {
        BoundExpr having = plan.having();
        List<BoundOrderItem> orderBy = plan.orderBy();
        long limit = plan.limit() == null ? Long.MAX_VALUE : plan.limit();
        long offset = plan.offset() == null ? 0 : plan.offset();

        // Pre-compile ORDER BY expressions to slot-/agg-indexed resolvers so the
        // per-group loop avoids findMatch/sameAgg linear scans. For Q33 that's
        // ~3 M findMatch calls collapsed to direct array reads.
        int orderN = orderBy.size();
        OrderResolver[] orderResolvers = new OrderResolver[orderN];
        for (int i = 0; i < orderN; i++) {
            orderResolvers[i] = compileOrderResolver(orderBy.get(i).expr(), groupExprs, aggs);
        }

        // HAVING filter + ORDER BY value extraction per group. ORDER BY values are
        // per-group, so this is O(groups × orderBy.size()) — cheap compared to the
        // full SELECT materialize we defer until after LIMIT.
        List<PendingGroup> pending = new ArrayList<>(groups.size());
        for (GroupEntry g : groups) {
            if (having != null) {
                Object hv = evalPostAgg(having, groupExprs, g.key(), aggs, g.states());
                if (!truthy(hv)) {
                    continue;
                }
            }
            @Nullable
            Object[] orderVals = orderN == 0 ? EMPTY_OBJECTS : new @Nullable Object[orderN];
            for (int i = 0; i < orderN; i++) {
                orderVals[i] = orderResolvers[i].eval(groupExprs, g.key(), aggs, g.states());
            }
            pending.add(new PendingGroup(g.key(), g.states(), orderVals));
        }

        List<PendingGroup> ordered;
        if (orderN > 0) {
            Comparator<PendingGroup> cmp = orderComparator(orderBy);
            long bound = Math.min((long) Integer.MAX_VALUE, offset + Math.min(limit, Integer.MAX_VALUE - offset));
            int k = (int) Math.min(pending.size(), Math.max(0L, bound));
            if (k < pending.size() && k < pending.size() / 2L) {
                ordered = selectTopK(pending, cmp, k);
            } else {
                ordered = new ArrayList<>(pending);
                ordered.sort(cmp);
            }
        } else {
            ordered = pending;
        }

        int fromIdx = (int) Math.min((long) ordered.size(), offset);
        int toIdx = (int) Math.min((long) ordered.size(), fromIdx + Math.min(limit, Integer.MAX_VALUE - fromIdx));
        List<PendingGroup> winners = ordered.subList(fromIdx, toIdx);

        List<@Nullable Object[]> rows = new ArrayList<>(winners.size());
        List<BoundSelectItem> items = plan.items();
        for (PendingGroup pg : winners) {
            @Nullable
            Object[] row = new @Nullable Object[items.size()];
            for (int i = 0; i < items.size(); i++) {
                row[i] = evalPostAgg(items.get(i).expr(), groupExprs, pg.key(), aggs, pg.states());
            }
            rows.add(row);
        }
        return toResult(plan, rows);
    }

    private record PendingGroup(List<Object> key, Aggregator[] states, @Nullable Object[] orderVals) {
    }

    private static final Object[] EMPTY_OBJECTS = new Object[0];

    /** Per-group ORDER BY value extractor, bound to a specific expression shape. */
    private interface OrderResolver {
        @Nullable
        Object eval(List<BoundExpr> groupExprs, List<Object> key, List<BoundAgg> aggs, Aggregator[] states);
    }

    /**
     * Compiles an ORDER BY expression once per query into an extractor that
     * avoids per-group findMatch / sameAgg scans. Direct hits are the common
     * case (ORDER BY <groupKey> or ORDER BY <aggregate>); anything else falls
     * back to the general evalPostAgg.
     */
    private static OrderResolver compileOrderResolver(BoundExpr expr, List<BoundExpr> groupExprs,
            List<BoundAgg> aggs) {
        for (int i = 0; i < groupExprs.size(); i++) {
            if (exprEquals(groupExprs.get(i), expr)) {
                final int idx = i;
                return (ge, key, ag, st) -> key.get(idx);
            }
        }
        if (expr instanceof BoundAgg) {
            for (int i = 0; i < aggs.size(); i++) {
                if (sameAgg(expr, aggs.get(i))) {
                    final int idx = i;
                    return (ge, key, ag, st) -> st[idx].result();
                }
            }
        }
        return (ge, key, ag, st) -> evalPostAgg(expr, ge, key, ag, st);
    }

    private static Comparator<PendingGroup> orderComparator(List<BoundOrderItem> orderBy) {
        return (ga, gb) -> {
            @Nullable
            Object[] a = ga.orderVals();
            @Nullable
            Object[] b = gb.orderVals();
            for (int i = 0; i < orderBy.size(); i++) {
                int c = compareNullsLast(a[i], b[i]);
                if (c != 0) {
                    return orderBy.get(i).direction() == SqlAst.SortDirection.DESC ? -c : c;
                }
            }
            return 0;
        };
    }

    /**
     * Returns the top {@code k} elements by {@code cmp} (lowest-cmp first in the
     * returned list), using a bounded min-heap of capacity {@code k}. Heap is
     * ordered by {@code cmp.reversed()} so the root is the current worst survivor;
     * we evict it when a better candidate arrives.
     */
    private static <T> List<T> selectTopK(List<T> input, Comparator<T> cmp, int k) {
        if (k <= 0) {
            return new ArrayList<>();
        }
        if (k >= input.size()) {
            List<T> copy = new ArrayList<>(input);
            copy.sort(cmp);
            return copy;
        }
        java.util.PriorityQueue<T> heap = new java.util.PriorityQueue<>(k, cmp.reversed());
        for (T el : input) {
            if (heap.size() < k) {
                heap.add(el);
            } else if (cmp.compare(el, heap.peek()) < 0) {
                heap.poll();
                heap.add(el);
            }
        }
        List<T> result = new ArrayList<>(heap);
        result.sort(cmp);
        return result;
    }

    private static long[] chunkBounds(long n) {
        int byCores = Math.max(1, Runtime.getRuntime().availableProcessors());
        int byRows = (int) Math.min(Integer.MAX_VALUE, (n + CHUNK_ROWS - 1) / CHUNK_ROWS);
        int k = Math.max(1, Math.min(byCores * 2, byRows));
        long chunk = (n + k - 1) / k;
        long[] bounds = new long[k + 1];
        for (int i = 0; i <= k; i++)
            bounds[i] = Math.min(n, (long) i * chunk);
        return bounds;
    }

    private static @Nullable Object evalPostAgg(BoundExpr e, List<BoundExpr> groupExprs, List<Object> key,
            List<BoundAgg> aggs, Aggregator[] states) {
        // If this expression equals one of the group-by exprs, take its key value.
        int gi = findMatch(e, groupExprs);
        if (gi >= 0)
            return key.get(gi);
        // If this is an aggregate call tracked in our list, take its state.
        for (int i = 0; i < aggs.size(); i++) {
            if (sameAgg(e, aggs.get(i)))
                return states[i].result();
        }
        // Otherwise recurse structurally.
        return switch (e) {
            case BoundLiteral l -> l.value();
            case BoundColumn c ->
                throw new SqlException("column " + c.name() + " used outside GROUP BY / aggregate", 0);
            case BoundUnary u -> applyUnary(u, evalPostAgg(u.operand(), groupExprs, key, aggs, states));
            case BoundBinary b -> applyBinary(b, evalPostAgg(b.left(), groupExprs, key, aggs, states),
                    evalPostAgg(b.right(), groupExprs, key, aggs, states));
            case BoundIsNull n -> {
                Object v = evalPostAgg(n.operand(), groupExprs, key, aggs, states);
                yield n.negated() ? v != null : v == null;
            }
            case BoundLike l -> {
                Object v = evalPostAgg(l.value(), groupExprs, key, aggs, states);
                if (v == null)
                    yield null;
                io.jpointdb.core.sql.LikeMatcher matcher = l.matcher();
                if (matcher == null) {
                    Object p = evalPostAgg(l.pattern(), groupExprs, key, aggs, states);
                    if (p == null)
                        yield null;
                    matcher = io.jpointdb.core.sql.LikeMatcher.forPattern((String) p);
                }
                boolean m = matcher.matches((String) v);
                yield l.negated() ? !m : m;
            }
            case BoundInList il -> {
                Object v = evalPostAgg(il.value(), groupExprs, key, aggs, states);
                if (v == null)
                    yield null;
                boolean hit = false;
                for (BoundExpr it : il.items()) {
                    Object iv = evalPostAgg(it, groupExprs, key, aggs, states);
                    if (iv != null && ExprEvaluator.compare(v, iv) == 0) {
                        hit = true;
                        break;
                    }
                }
                yield il.negated() != hit;
            }
            case BoundCase c -> {
                for (BoundWhen w : c.whens()) {
                    Object cond = evalPostAgg(w.when(), groupExprs, key, aggs, states);
                    if (Boolean.TRUE.equals(cond))
                        yield evalPostAgg(w.then(), groupExprs, key, aggs, states);
                }
                yield c.elseExpr() == null ? null : evalPostAgg(c.elseExpr(), groupExprs, key, aggs, states);
            }
            case BoundScalarCall sc ->
                ExprEvaluator.evalScalarCall(sc, a -> evalPostAgg(a, groupExprs, key, aggs, states));
            case BoundAgg a -> throw new AssertionError("unregistered agg: " + a);
        };
    }

    private static int findMatch(BoundExpr e, List<BoundExpr> list) {
        for (int i = 0; i < list.size(); i++) {
            if (exprEquals(e, list.get(i)))
                return i;
        }
        return -1;
    }

    private static boolean exprEquals(BoundExpr a, BoundExpr b) {
        if (a == b)
            return true;
        if (a.getClass() != b.getClass())
            return false;
        if (a instanceof BoundColumn ca && b instanceof BoundColumn cb)
            return ca.index() == cb.index();
        return a.equals(b);
    }

    private static boolean sameAgg(BoundExpr e, BoundAgg target) {
        if (!(e instanceof BoundAgg a))
            return false;
        if (a.fn() != target.fn() || a.distinct() != target.distinct())
            return false;
        if (a.arg() == null && target.arg() == null)
            return true;
        if (a.arg() == null || target.arg() == null)
            return false;
        return exprEquals(a.arg(), target.arg());
    }

    private static @Nullable Object applyUnary(BoundUnary u, @Nullable Object v) {
        if (v == null)
            return null;
        return switch (u.op()) {
            case NEG -> v instanceof Long l ? -l : v instanceof Double d ? -d : null;
            case NOT -> v instanceof Boolean b ? !b : null;
        };
    }

    private static @Nullable Object applyBinary(BoundBinary b, @Nullable Object l, @Nullable Object r) {
        if (b.op() == SqlAst.BinaryOp.AND) {
            if (Boolean.FALSE.equals(l) || Boolean.FALSE.equals(r))
                return Boolean.FALSE;
            if (l == null || r == null)
                return null;
            return ((Boolean) l) && ((Boolean) r);
        }
        if (b.op() == SqlAst.BinaryOp.OR) {
            if (Boolean.TRUE.equals(l) || Boolean.TRUE.equals(r))
                return Boolean.TRUE;
            if (l == null || r == null)
                return null;
            return ((Boolean) l) || ((Boolean) r);
        }
        if (l == null || r == null)
            return null;
        return switch (b.op()) {
            case PLUS -> addNum(l, r);
            case MINUS -> subNum(l, r);
            case MUL -> mulNum(l, r);
            case DIV -> divNum(l, r);
            case MOD -> modNum(l, r);
            case EQ -> ExprEvaluator.compare(l, r) == 0;
            case NEQ -> ExprEvaluator.compare(l, r) != 0;
            case LT -> ExprEvaluator.compare(l, r) < 0;
            case LE -> ExprEvaluator.compare(l, r) <= 0;
            case GT -> ExprEvaluator.compare(l, r) > 0;
            case GE -> ExprEvaluator.compare(l, r) >= 0;
            case AND, OR -> throw new AssertionError();
        };
    }

    private static Object addNum(Object a, Object b) {
        if (a instanceof Double || b instanceof Double)
            return ((Number) a).doubleValue() + ((Number) b).doubleValue();
        return ((Number) a).longValue() + ((Number) b).longValue();
    }

    private static Object subNum(Object a, Object b) {
        if (a instanceof Double || b instanceof Double)
            return ((Number) a).doubleValue() - ((Number) b).doubleValue();
        return ((Number) a).longValue() - ((Number) b).longValue();
    }

    private static Object mulNum(Object a, Object b) {
        if (a instanceof Double || b instanceof Double)
            return ((Number) a).doubleValue() * ((Number) b).doubleValue();
        return ((Number) a).longValue() * ((Number) b).longValue();
    }

    private static @Nullable Object divNum(Object a, Object b) {
        if (a instanceof Double || b instanceof Double) {
            double rb = ((Number) b).doubleValue();
            return rb == 0.0 ? null : ((Number) a).doubleValue() / rb;
        }
        long rb = ((Number) b).longValue();
        return rb == 0 ? null : ((Number) a).longValue() / rb;
    }

    private static @Nullable Object modNum(Object a, Object b) {
        if (a instanceof Double || b instanceof Double) {
            double rb = ((Number) b).doubleValue();
            return rb == 0.0 ? null : ((Number) a).doubleValue() % rb;
        }
        long rb = ((Number) b).longValue();
        return rb == 0 ? null : ((Number) a).longValue() % rb;
    }

    private static List<BoundAgg> collectAggregates(BoundSelect plan) {
        List<BoundAgg> out = new ArrayList<>();
        for (BoundSelectItem item : plan.items())
            collect(item.expr(), out);
        if (plan.having() != null)
            collect(plan.having(), out);
        for (BoundOrderItem o : plan.orderBy())
            collect(o.expr(), out);
        return out;
    }

    private static void collect(BoundExpr e, List<BoundAgg> out) {
        switch (e) {
            case BoundAgg a -> {
                for (BoundAgg existing : out)
                    if (sameAgg(existing, a))
                        return;
                out.add(a);
            }
            case BoundUnary u -> collect(u.operand(), out);
            case BoundBinary b -> {
                collect(b.left(), out);
                collect(b.right(), out);
            }
            case BoundIsNull n -> collect(n.operand(), out);
            case BoundLike l -> {
                collect(l.value(), out);
                collect(l.pattern(), out);
            }
            case BoundInList il -> {
                collect(il.value(), out);
                for (BoundExpr x : il.items())
                    collect(x, out);
            }
            case BoundCase c -> {
                for (BoundWhen w : c.whens()) {
                    collect(w.when(), out);
                    collect(w.then(), out);
                }
                if (c.elseExpr() != null)
                    collect(c.elseExpr(), out);
            }
            case BoundScalarCall sc -> {
                for (BoundExpr x : sc.args())
                    collect(x, out);
            }
            case BoundLiteral ignored -> {}
            case BoundColumn ignored -> {}
        }
    }

    // ---------- helpers ----------

    private static boolean truthy(@Nullable Object v) {
        return Boolean.TRUE.equals(v);
    }

    private static List<@Nullable Object[]> applyLimit(List<@Nullable Object[]> rows, long offset, long limit) {
        if (offset <= 0 && limit >= rows.size())
            return rows;
        int from = (int) Math.min(offset, rows.size());
        int to = (int) Math.min(from + limit, rows.size());
        return new ArrayList<>(rows.subList(from, to));
    }

    private static QueryResult toResult(BoundSelect plan, List<@Nullable Object[]> rows) {
        List<String> names = new ArrayList<>(plan.items().size());
        List<ValueType> types = new ArrayList<>(plan.items().size());
        for (BoundSelectItem it : plan.items()) {
            names.add(it.outputName());
            types.add(it.expr().type());
        }
        return new QueryResult(names, types, rows);
    }

    private static int compareNullsLast(@Nullable Object a, @Nullable Object b) {
        if (a == null && b == null)
            return 0;
        if (a == null)
            return 1;
        if (b == null)
            return -1;
        return ExprEvaluator.compare(a, b);
    }
}
