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
        long limit = plan.limit() == null ? Long.MAX_VALUE : plan.limit();
        long offset = plan.offset() == null ? 0 : plan.offset();
        long bound = (limit == Long.MAX_VALUE) ? Long.MAX_VALUE : offset + limit;

        // Top-k fast path: when the query has ORDER BY + small LIMIT and no
        // DISTINCT post-processing, keep only the (offset+limit) best
        // candidates in a bounded heap per chunk, merge K winners across
        // chunks. Avoids the O(N) materialize + O(N log N) sort of the
        // general path — crucial for queries like Q25/Q27 where String
        // compareTo on EventTime dominates.
        if (orderCount > 0 && bound <= 1000 && bound > 0 && n > 0) {
            return executeScalarTopK(plan, table, n, orderCount, (int) bound, (int) offset, (int) limit);
        }

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
        return toResult(plan, applyLimit(values, offset, limit));
    }

    /**
     * One scalar row that's made it into a top-k heap: SELECT values + ORDER BY
     * keys.
     */
    private record ScalarTopKEntry(@Nullable Object[] values, @Nullable Object[] sortKeys) {
    }

    /**
     * Bounded-heap top-k for scalar ORDER BY + LIMIT. Each chunk keeps its own
     * {@code k}-element heap; we concatenate the chunks' winners and sort them once
     * at the end. K is small (bench shows 10-1000), so the sort of
     * {@code chunks × k} items is trivial.
     */
    private static QueryResult executeScalarTopK(BoundSelect plan, Table table, long n, int orderCount, int k,
            int offset, long limit) {
        boolean[] desc = new boolean[orderCount];
        for (int i = 0; i < orderCount; i++) {
            desc[i] = plan.orderBy().get(i).direction() == SqlAst.SortDirection.DESC;
        }
        List<ScalarTopKEntry> merged;
        if (n >= PARALLEL_THRESHOLD) {
            long[] bounds = chunkBounds(n);
            int nc = bounds.length - 1;
            @SuppressWarnings("unchecked")
            ForkJoinTask<List<ScalarTopKEntry>>[] tasks = new ForkJoinTask[nc];
            for (int c = 0; c < nc; c++) {
                long from = bounds[c];
                long to = bounds[c + 1];
                tasks[c] = ForkJoinPool.commonPool()
                        .submit(() -> scanScalarChunkTopK(plan, table, from, to, orderCount, k, desc));
            }
            merged = new ArrayList<>(nc * k);
            for (ForkJoinTask<List<ScalarTopKEntry>> t : tasks) {
                merged.addAll(t.join());
            }
        } else {
            merged = scanScalarChunkTopK(plan, table, 0, n, orderCount, k, desc);
        }
        merged.sort((a, b) -> {
            @Nullable
            Object[] ka = a.sortKeys();
            @Nullable
            Object[] kb = b.sortKeys();
            for (int i = 0; i < orderCount; i++) {
                int c = compareNullsLast(ka[i], kb[i]);
                if (c != 0) {
                    return desc[i] ? -c : c;
                }
            }
            return 0;
        });
        int fromIdx = Math.min(merged.size(), offset);
        int toIdx = (int) Math.min((long) merged.size(), fromIdx + Math.min(limit, Integer.MAX_VALUE - fromIdx));
        List<@Nullable Object[]> rows = new ArrayList<>(toIdx - fromIdx);
        for (int i = fromIdx; i < toIdx; i++) {
            rows.add(merged.get(i).values());
        }
        return toResult(plan, rows);
    }

    private static List<ScalarTopKEntry> scanScalarChunkTopK(BoundSelect plan, Table table, long from, long to,
            int orderCount, int k, boolean[] desc) {
        ExprEvaluator ev = new ExprEvaluator(table);
        BoundExpr where = plan.where();
        List<BoundSelectItem> items = plan.items();
        List<BoundOrderItem> orderBy = plan.orderBy();
        // Min-heap ordered so the current WORST candidate sits at the root:
        // new candidates replace it only when they rank strictly better.
        java.util.Comparator<ScalarTopKEntry> worstFirst = (a, b) -> {
            @Nullable
            Object[] ka = a.sortKeys();
            @Nullable
            Object[] kb = b.sortKeys();
            for (int i = 0; i < orderCount; i++) {
                int c = compareNullsLast(ka[i], kb[i]);
                if (c != 0) {
                    return desc[i] ? c : -c; // reversed: worst first
                }
            }
            return 0;
        };
        java.util.PriorityQueue<ScalarTopKEntry> heap = new java.util.PriorityQueue<>(k, worstFirst);
        for (long r = from; r < to; r++) {
            if (where != null && !truthy(ev.eval(where, r))) {
                continue;
            }
            @Nullable
            Object[] keys = new @Nullable Object[orderCount];
            for (int i = 0; i < orderCount; i++) {
                keys[i] = ev.eval(orderBy.get(i).expr(), r);
            }
            if (heap.size() < k) {
                @Nullable
                Object[] v = new @Nullable Object[items.size()];
                for (int i = 0; i < items.size(); i++) {
                    v[i] = ev.eval(items.get(i).expr(), r);
                }
                heap.add(new ScalarTopKEntry(v, keys));
            } else {
                // Better than current worst?
                ScalarTopKEntry worst = heap.peek();
                @Nullable
                Object[] wk = worst.sortKeys();
                boolean better = false;
                for (int i = 0; i < orderCount; i++) {
                    int c = compareNullsLast(keys[i], wk[i]);
                    if (c != 0) {
                        better = desc[i] ? c > 0 : c < 0;
                        break;
                    }
                }
                if (better) {
                    @Nullable
                    Object[] v = new @Nullable Object[items.size()];
                    for (int i = 0; i < items.size(); i++) {
                        v[i] = ev.eval(items.get(i).expr(), r);
                    }
                    heap.poll();
                    heap.add(new ScalarTopKEntry(v, keys));
                }
            }
        }
        return new ArrayList<>(heap);
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

        QueryResult dictMinMax = tryDictMinMaxShortcut(plan, groupExprs, aggs, table, n);
        if (dictMinMax != null) {
            return dictMinMax;
        }

        VectorBatchAgg[] vbagg = detectGrandTotalVector(plan, groupExprs, aggs, table);
        if (vbagg != null) {
            return executeGrandTotalVector(plan, aggs, n, vbagg);
        }

        PrimitiveKeyShape shape = detectPrimitiveKey(groupExprs, table);
        if (shape != null) {
            SimpleAggShape simple = detectSimpleAggs(aggs, table);
            if (simple != null) {
                SlotOrderShape orderShape = detectSlotOrder(plan, groupExprs, aggs, simple, shape);
                if (orderShape != null && plan.having() == null) {
                    return executeAggregatedInline(plan, table, groupExprs, aggs, n, shape, simple, orderShape);
                }
            }
            // LongKeysAggMap only supports widths 1/2 and has no notion of
            // derived groupExprs; either of those falls through to the generic
            // boxed-key path when the inline fast path can't take it.
            if (shape.width() <= 2 && shape.derivedExprs() == null) {
                return executeAggregatedPrimitive(plan, table, groupExprs, aggs, n, shape);
            }
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

    // ---------- grand-total MIN/MAX via dict walk (skips the 1 M-row scan) ----------

    /**
     * Q07-style {@code SELECT MIN(col), MAX(col) FROM hits} over a DICT-encoded
     * STRING column: since every stored dict entry corresponds to at least one row,
     * MIN / MAX of the column equals MIN / MAX over the dict itself. For EventDate
     * (~30 entries) this is instantaneous; skipped for large dicts where the walk
     * would outweigh the per-row scan.
     *
     * <p>
     * Returns {@code null} to signal "not our shape — fall through".
     */
    private static @Nullable QueryResult tryDictMinMaxShortcut(BoundSelect plan, List<BoundExpr> groupExprs,
            List<BoundAgg> aggs, Table table, long n) {
        if (plan.where() != null || !groupExprs.isEmpty() || aggs.isEmpty()) {
            return null;
        }
        // All aggs must be MIN or MAX over the same DICT-encoded STRING column,
        // dict small enough that one walk beats a full scan.
        int sharedCol = -1;
        for (BoundAgg a : aggs) {
            if (a.distinct()) {
                return null;
            }
            if (a.fn() != AggregateFn.MIN && a.fn() != AggregateFn.MAX) {
                return null;
            }
            BoundExpr arg = a.arg();
            if (!(arg instanceof BoundColumn bc)) {
                return null;
            }
            if (table.columnMeta(bc.index()).type() != ColumnType.STRING) {
                return null;
            }
            io.jpointdb.core.column.StringColumn sc = table.string(bc.index());
            if (sc.mode() != io.jpointdb.core.column.StringColumnWriter.Mode.DICT) {
                return null;
            }
            if (sharedCol != -1 && sharedCol != bc.index()) {
                return null; // different columns per agg would need separate walks.
            }
            sharedCol = bc.index();
        }
        if (sharedCol == -1) {
            return null;
        }
        io.jpointdb.core.column.StringColumn sc = table.string(sharedCol);
        io.jpointdb.core.column.Dictionary dict = sc.dictionary();
        if (dict == null) {
            return null;
        }
        int dsize = dict.size();
        if (dsize == 0 || dsize > 1000) {
            return null;
        }
        if (n == 0) {
            // Empty table → null result from MIN/MAX.
            Aggregator[] states = new Aggregator[aggs.size()];
            for (int i = 0; i < aggs.size(); i++) {
                states[i] = new PrecomputedAggregator(null);
            }
            List<GroupEntry> groups = List.of(new GroupEntry(List.of(), states));
            return finalizeAggregated(plan, groupExprs, aggs, groups);
        }
        String min = dict.stringAt(0);
        String max = min;
        for (int i = 1; i < dsize; i++) {
            String s = dict.stringAt(i);
            if (s.compareTo(min) < 0) {
                min = s;
            }
            if (s.compareTo(max) > 0) {
                max = s;
            }
        }
        String finalMin = min;
        String finalMax = max;
        Aggregator[] states = new Aggregator[aggs.size()];
        for (int i = 0; i < aggs.size(); i++) {
            states[i] = new PrecomputedAggregator(aggs.get(i).fn() == AggregateFn.MIN ? finalMin : finalMax);
        }
        List<GroupEntry> groups = List.of(new GroupEntry(List.of(), states));
        return finalizeAggregated(plan, groupExprs, aggs, groups);
    }

    // ---------- general grand-total via VectorBatchAgg (SIMD) ----------

    /**
     * Matches grand-total queries whose every agg is a COUNT_STAR, or SUM / AVG /
     * MIN / MAX over a non-nullable I32 / I64 / F64 BoundColumn. Returns one
     * {@link VectorBatchAgg} per agg (templates, not per-chunk state), or
     * {@code null} to punt to the general path.
     *
     * <p>
     * Q30's algebraic fusion ({@link #detectGrandTotalSum}) runs first and wins on
     * its narrow shape; this path catches the broader Q03 / Q04 / Q07-ish cases.
     */
    private static VectorBatchAgg @Nullable [] detectGrandTotalVector(BoundSelect plan, List<BoundExpr> groupExprs,
            List<BoundAgg> aggs, Table table) {
        if (plan.where() != null || !groupExprs.isEmpty() || aggs.isEmpty()) {
            return null;
        }
        VectorBatchAgg[] out = new VectorBatchAgg[aggs.size()];
        for (int i = 0; i < aggs.size(); i++) {
            BoundAgg a = aggs.get(i);
            if (a.distinct()) {
                return null;
            }
            if (a.fn() == AggregateFn.COUNT_STAR) {
                out[i] = new VectorBatchAgg.CountStar();
                continue;
            }
            BoundExpr arg = a.arg();
            if (!(arg instanceof BoundColumn bc)) {
                return null;
            }
            ColumnType ct = table.columnMeta(bc.index()).type();
            VectorBatchAgg op;
            switch (ct) {
                case I32 -> {
                    I32Column c = table.i32(bc.index());
                    if (c.nullable()) {
                        return null;
                    }
                    op = switch (a.fn()) {
                        case SUM -> new VectorBatchAgg.SumI32(c);
                        case AVG -> new VectorBatchAgg.AvgI32(c);
                        case MIN -> new VectorBatchAgg.MinMaxI32(c, true);
                        case MAX -> new VectorBatchAgg.MinMaxI32(c, false);
                        default -> null;
                    };
                }
                case I64 -> {
                    I64Column c = table.i64(bc.index());
                    if (c.nullable()) {
                        return null;
                    }
                    op = switch (a.fn()) {
                        case SUM -> new VectorBatchAgg.SumI64(c);
                        case AVG -> new VectorBatchAgg.AvgI64(c);
                        case MIN -> new VectorBatchAgg.MinMaxI64(c, true);
                        case MAX -> new VectorBatchAgg.MinMaxI64(c, false);
                        default -> null;
                    };
                }
                case F64 -> {
                    io.jpointdb.core.column.F64Column c = table.f64(bc.index());
                    if (c.nullable()) {
                        return null;
                    }
                    op = switch (a.fn()) {
                        case SUM -> new VectorBatchAgg.SumF64(c);
                        case AVG -> new VectorBatchAgg.AvgF64(c);
                        default -> null; // MIN/MAX on F64 left for a follow-up
                    };
                }
                default -> {
                    return null;
                }
            }
            if (op == null) {
                return null;
            }
            out[i] = op;
        }
        return out;
    }

    private static QueryResult executeGrandTotalVector(BoundSelect plan, List<BoundAgg> aggs, long n,
            VectorBatchAgg[] templates) {
        int aggCount = aggs.size();
        VectorBatchAgg[] merged;
        if (n >= PARALLEL_THRESHOLD) {
            long[] bounds = chunkBounds(n);
            int k = bounds.length - 1;
            @SuppressWarnings("unchecked")
            ForkJoinTask<VectorBatchAgg[]>[] tasks = new ForkJoinTask[k];
            for (int c = 0; c < k; c++) {
                long from = bounds[c];
                long to = bounds[c + 1];
                tasks[c] = ForkJoinPool.commonPool().submit(() -> {
                    VectorBatchAgg[] local = new VectorBatchAgg[aggCount];
                    for (int j = 0; j < aggCount; j++) {
                        local[j] = templates[j].forkChunk();
                        local[j].acceptRange(from, to);
                    }
                    return local;
                });
            }
            merged = new VectorBatchAgg[aggCount];
            for (int j = 0; j < aggCount; j++) {
                merged[j] = templates[j].forkChunk();
            }
            for (ForkJoinTask<VectorBatchAgg[]> t : tasks) {
                VectorBatchAgg[] part = t.join();
                for (int j = 0; j < aggCount; j++) {
                    merged[j].mergeFrom(part[j]);
                }
            }
        } else {
            merged = new VectorBatchAgg[aggCount];
            for (int j = 0; j < aggCount; j++) {
                merged[j] = templates[j].forkChunk();
                merged[j].acceptRange(0, n);
            }
        }

        // Emit a single-row result. Reuse PrecomputedAggregator to keep the
        // finalize path shape-agnostic.
        Aggregator[] states = new Aggregator[aggCount];
        for (int j = 0; j < aggCount; j++) {
            states[j] = new PrecomputedAggregator(merged[j].result());
        }
        List<GroupEntry> groups = List.of(new GroupEntry(List.of(), states));
        return finalizeAggregated(plan, plan.groupBy(), aggs, groups);
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
        I32, I64, DICT_STRING,
        /**
         * {@code extract(<field> FROM <DICT-encoded STRING column>)} — a scalar call
         * whose dict-id-to-long mapping is precomputed once per query, so the hot
         * scan reduces to {@code map[col.idAt(row)]} — a pure array load.
         */
        DICT_STRING_EXTRACT_LONG
    }

    /**
     * Resolved primitive GROUP BY shape. The SQL {@code GROUP BY} list can mix
     * {@link BoundColumn} (real key columns) and {@link BoundLiteral} (constant
     * values that produce a single partition — semantically a no-op).
     *
     * <ul>
     * <li>{@code colIdx} / {@code kinds} describe the actual hash-key columns (size
     * = 1 or 2 = hash width).</li>
     * <li>{@code exprToKey[i]} = index into {@code colIdx} for the i-th
     * {@code groupExprs} entry, or -1 if it's a literal.</li>
     * <li>{@code literalValues[i]} = the boxed literal value if
     * {@code exprToKey[i] == -1}; {@code null} otherwise.</li>
     * </ul>
     *
     * <p>
     * Q35 {@code GROUP BY 1, URL} becomes width-1 DICT_STRING on URL with
     * {@code exprToKey = [-1, 0]} and {@code literalValues = [1L, null]}.
     */
    @SuppressWarnings("ArrayRecordComponent")
    private record PrimitiveKeyShape(int[] colIdx, KeyKind[] kinds, int[] exprToKey, @Nullable Object[] literalValues,
            long @Nullable [][] dictMaps, BoundExpr @Nullable [] derivedExprs) {
        int width() {
            return colIdx.length;
        }

        int exprCount() {
            return exprToKey.length;
        }
    }

    // ---------- inline primitive state path: PrimitiveAggMap + slot-based top-k ----------

    /**
     * How to update a slot's state for one row, and how to produce its final value.
     */
    private interface InlineAccept {
        void acceptRow(long[][] longFields, double[][] doubleFields, int slot, long row);

        @Nullable
        Object result(PrimitiveAggMap map, int slot);
    }

    /** Resolved inline-primitive agg shape: one {@link InlineAccept} per agg. */
    @SuppressWarnings("ArrayRecordComponent")
    private record SimpleAggShape(PrimitiveAggMap.AggOp[] mapOps, int longFields, int doubleFields,
            InlineAccept[] accepts) {
    }

    /**
     * Returns a shape iff every agg is one of: COUNT_STAR, or SUM/AVG over a
     * non-nullable I32/I64/F64 BoundColumn. Distinct or any richer expression
     * rejects — {@code null} signals "use the Aggregator-per-slot path".
     */
    private static @Nullable SimpleAggShape detectSimpleAggs(List<BoundAgg> aggs, Table table) {
        if (aggs.isEmpty()) {
            return null;
        }
        PrimitiveAggMap.AggOp[] mapOps = new PrimitiveAggMap.AggOp[aggs.size()];
        InlineAccept[] accepts = new InlineAccept[aggs.size()];
        int lf = 0;
        int df = 0;
        for (int i = 0; i < aggs.size(); i++) {
            BoundAgg a = aggs.get(i);
            if (a.distinct()) {
                return null;
            }
            if (a.fn() == AggregateFn.COUNT_STAR) {
                final int longIdx = lf++;
                mapOps[i] = new PrimitiveAggMap.AggOp(PrimitiveAggMap.AggOp.Kind.COUNT_STAR, longIdx, -1);
                accepts[i] = new InlineAccept() {
                    @Override
                    public void acceptRow(long[][] longFields, double[][] doubleFields, int slot, long row) {
                        longFields[longIdx][slot]++;
                    }

                    @Override
                    public @Nullable Object result(PrimitiveAggMap map, int slot) {
                        return map.longField(longIdx)[slot];
                    }
                };
                continue;
            }
            if (a.fn() != AggregateFn.SUM && a.fn() != AggregateFn.AVG) {
                return null;
            }
            BoundExpr arg = a.arg();
            if (!(arg instanceof BoundColumn bc)) {
                return null;
            }
            ColumnType ct = table.columnMeta(bc.index()).type();
            boolean isAvg = a.fn() == AggregateFn.AVG;
            if (ct == ColumnType.I32) {
                I32Column c = table.i32(bc.index());
                if (c.nullable()) {
                    return null;
                }
                if (!isAvg) {
                    final int longIdx = lf++;
                    mapOps[i] = new PrimitiveAggMap.AggOp(PrimitiveAggMap.AggOp.Kind.SUM_LONG, longIdx, -1);
                    accepts[i] = new InlineAccept() {
                        @Override
                        public void acceptRow(long[][] longFields, double[][] doubleFields, int slot, long row) {
                            longFields[longIdx][slot] += c.get(row);
                        }

                        @Override
                        public @Nullable Object result(PrimitiveAggMap map, int slot) {
                            return map.longField(longIdx)[slot];
                        }
                    };
                } else {
                    final int doubleIdx = df++;
                    final int longIdx = lf++;
                    mapOps[i] = new PrimitiveAggMap.AggOp(PrimitiveAggMap.AggOp.Kind.AVG_LONG, longIdx, doubleIdx);
                    accepts[i] = new InlineAccept() {
                        @Override
                        public void acceptRow(long[][] longFields, double[][] doubleFields, int slot, long row) {
                            doubleFields[doubleIdx][slot] += c.get(row);
                            longFields[longIdx][slot]++;
                        }

                        @Override
                        public @Nullable Object result(PrimitiveAggMap map, int slot) {
                            long cnt = map.longField(longIdx)[slot];
                            return cnt == 0 ? null : map.doubleField(doubleIdx)[slot] / cnt;
                        }
                    };
                }
            } else if (ct == ColumnType.I64) {
                I64Column c = table.i64(bc.index());
                if (c.nullable()) {
                    return null;
                }
                if (!isAvg) {
                    final int longIdx = lf++;
                    mapOps[i] = new PrimitiveAggMap.AggOp(PrimitiveAggMap.AggOp.Kind.SUM_LONG, longIdx, -1);
                    accepts[i] = new InlineAccept() {
                        @Override
                        public void acceptRow(long[][] longFields, double[][] doubleFields, int slot, long row) {
                            longFields[longIdx][slot] += c.get(row);
                        }

                        @Override
                        public @Nullable Object result(PrimitiveAggMap map, int slot) {
                            return map.longField(longIdx)[slot];
                        }
                    };
                } else {
                    final int doubleIdx = df++;
                    final int longIdx = lf++;
                    mapOps[i] = new PrimitiveAggMap.AggOp(PrimitiveAggMap.AggOp.Kind.AVG_LONG, longIdx, doubleIdx);
                    accepts[i] = new InlineAccept() {
                        @Override
                        public void acceptRow(long[][] longFields, double[][] doubleFields, int slot, long row) {
                            doubleFields[doubleIdx][slot] += c.get(row);
                            longFields[longIdx][slot]++;
                        }

                        @Override
                        public @Nullable Object result(PrimitiveAggMap map, int slot) {
                            long cnt = map.longField(longIdx)[slot];
                            return cnt == 0 ? null : map.doubleField(doubleIdx)[slot] / cnt;
                        }
                    };
                }
            } else if (ct == ColumnType.F64) {
                io.jpointdb.core.column.F64Column c = table.f64(bc.index());
                if (c.nullable()) {
                    return null;
                }
                final int doubleIdx = df++;
                if (!isAvg) {
                    mapOps[i] = new PrimitiveAggMap.AggOp(PrimitiveAggMap.AggOp.Kind.SUM_DOUBLE, -1, doubleIdx);
                    accepts[i] = new InlineAccept() {
                        @Override
                        public void acceptRow(long[][] longFields, double[][] doubleFields, int slot, long row) {
                            doubleFields[doubleIdx][slot] += c.get(row);
                        }

                        @Override
                        public @Nullable Object result(PrimitiveAggMap map, int slot) {
                            return map.doubleField(doubleIdx)[slot];
                        }
                    };
                } else {
                    final int longIdx = lf++;
                    mapOps[i] = new PrimitiveAggMap.AggOp(PrimitiveAggMap.AggOp.Kind.AVG_DOUBLE, longIdx, doubleIdx);
                    accepts[i] = new InlineAccept() {
                        @Override
                        public void acceptRow(long[][] longFields, double[][] doubleFields, int slot, long row) {
                            doubleFields[doubleIdx][slot] += c.get(row);
                            longFields[longIdx][slot]++;
                        }

                        @Override
                        public @Nullable Object result(PrimitiveAggMap map, int slot) {
                            long cnt = map.longField(longIdx)[slot];
                            return cnt == 0 ? null : map.doubleField(doubleIdx)[slot] / cnt;
                        }
                    };
                }
            } else {
                return null;
            }
        }
        return new SimpleAggShape(mapOps, lf, df, accepts);
    }

    /** Reads one ORDER BY component as a {@code long} from a slot's state/key. */
    private interface SlotLongExtractor {
        long extract(PrimitiveAggMap map, int slot);
    }

    /**
     * Resolved primitive ORDER BY layout: per-item extractor + direction flag, plus
     * a precompiled sign vector so the comparator is a pure primitive loop.
     */
    @SuppressWarnings("ArrayRecordComponent")
    private record SlotOrderShape(SlotLongExtractor[] extractors, boolean[] desc) {
    }

    /**
     * Decides if every ORDER BY expression reads a primitive long: either a
     * group-key column slot, or a SUM_LONG/COUNT_STAR agg result. AVG / DOUBLE /
     * expressions force the general path.
     */
    private static @Nullable SlotOrderShape detectSlotOrder(BoundSelect plan, List<BoundExpr> groupExprs,
            List<BoundAgg> aggs, SimpleAggShape agg, PrimitiveKeyShape key) {
        List<BoundOrderItem> orderBy = plan.orderBy();
        if (orderBy.isEmpty()) {
            return null;
        }
        SlotLongExtractor[] extractors = new SlotLongExtractor[orderBy.size()];
        boolean[] desc = new boolean[orderBy.size()];
        for (int i = 0; i < orderBy.size(); i++) {
            BoundOrderItem item = orderBy.get(i);
            desc[i] = item.direction() == SqlAst.SortDirection.DESC;
            SlotLongExtractor ext = resolveSlotOrderExtractor(item.expr(), groupExprs, aggs, agg, key);
            if (ext == null) {
                return null;
            }
            extractors[i] = ext;
        }
        return new SlotOrderShape(extractors, desc);
    }

    private static @Nullable SlotLongExtractor resolveSlotOrderExtractor(BoundExpr expr, List<BoundExpr> groupExprs,
            List<BoundAgg> aggs, SimpleAggShape agg, PrimitiveKeyShape key) {
        for (int i = 0; i < groupExprs.size(); i++) {
            if (exprEquals(groupExprs.get(i), expr)) {
                int keyIdx = key.exprToKey()[i];
                // Literal-backed group expr: sort order is constant across all groups.
                // Bail out — falling back to the full materialize path is simpler than
                // plumbing a constant extractor through the primitive heap.
                if (keyIdx < 0) {
                    return null;
                }
                final int component = keyIdx;
                return (m, s) -> m.keyAt(s, component);
            }
        }
        if (expr instanceof BoundAgg be) {
            for (int i = 0; i < aggs.size(); i++) {
                if (sameAgg(expr, aggs.get(i))) {
                    PrimitiveAggMap.AggOp op = agg.mapOps()[i];
                    // Only long-producing aggs work with a primitive long comparator.
                    if (op.kind == PrimitiveAggMap.AggOp.Kind.COUNT_STAR
                            || op.kind == PrimitiveAggMap.AggOp.Kind.SUM_LONG) {
                        final int longIdx = op.longField;
                        return (m, s) -> m.longField(longIdx)[s];
                    }
                    return null;
                }
            }
        }
        return null;
    }

    /**
     * Full primitive-inline pipeline for Q33-shaped queries:
     *
     * <pre>
     *     scan → PrimitiveAggMap (flat long/double state, no Aggregator objects)
     *     merge chunks
     *     slot-based top-k on primitive long ORDER BY vals (no boxing)
     *     materialize only K winners (boxing + evalPostAgg for ~10 rows)
     * </pre>
     */
    /**
     * For high-cardinality GROUP BY, merge chunks radix-style in parallel
     * partitions.
     */
    private static final int RADIX_THRESHOLD = 200_000;

    private static int nextPowerOfTwoPartitions(int chunkCount) {
        // Prefer P = #cores rounded to power-of-2 (for mask arithmetic).
        int cores = Math.max(1, Runtime.getRuntime().availableProcessors());
        int target = Math.min(cores, chunkCount);
        int p = 1;
        while (p * 2 <= target) {
            p *= 2;
        }
        return Math.max(2, p);
    }

    /**
     * Partition all chunk entries by {@code hash(key) & (P-1)} into {@code P}
     * independent result maps, each built in parallel. Every key lands in exactly
     * one partition so the per-partition merge needs no cross-thread
     * synchronization; downstream top-k then runs per partition in parallel.
     *
     * <p>
     * Unit of work per partition thread: walk {@code T × chunk_size} source slots,
     * test partition membership with a masked hash, insert the ~1/P that match.
     * Dominant cost is {@code T × chunk_size} hash calls and {@code N/P}
     * destination-map inserts.
     */
    private static PrimitiveAggMap[] mergeChunksRadix(List<PrimitiveAggMap> chunks, int p, PrimitiveKeyShape keyShape,
            SimpleAggShape aggShape, int totalHint) {
        int width = keyShape.width();
        int estPerPart = Math.max(64, (totalHint + p - 1) / p);
        int pMask = p - 1;
        PrimitiveAggMap[] result = new PrimitiveAggMap[p];
        @SuppressWarnings("unchecked")
        ForkJoinTask<PrimitiveAggMap>[] tasks = new ForkJoinTask[p];
        for (int i = 0; i < p; i++) {
            final int part = i;
            tasks[i] = ForkJoinPool.commonPool().submit(() -> {
                PrimitiveAggMap out = new PrimitiveAggMap(width, estPerPart, aggShape.mapOps(), aggShape.longFields(),
                        aggShape.doubleFields());
                out.ensureCapacity(estPerPart);
                if (width == 1) {
                    for (PrimitiveAggMap chunk : chunks) {
                        int[] slots = chunk.occupiedSlots();
                        int nn = chunk.nonNullSize();
                        for (int j = 0; j < nn; j++) {
                            int srcSlot = slots[j];
                            long k = chunk.keyAt(srcSlot, 0);
                            if ((int) (PrimitiveAggMap.hashKey(k) & pMask) != part) {
                                continue;
                            }
                            int dstSlot = out.getOrCreateSlot1(k);
                            out.foldSlotFrom(dstSlot, chunk, srcSlot);
                        }
                    }
                } else if (width == 2) {
                    for (PrimitiveAggMap chunk : chunks) {
                        int[] slots = chunk.occupiedSlots();
                        int nn = chunk.nonNullSize();
                        for (int j = 0; j < nn; j++) {
                            int srcSlot = slots[j];
                            long a = chunk.keyAt(srcSlot, 0);
                            long b = chunk.keyAt(srcSlot, 1);
                            if ((int) (PrimitiveAggMap.hashKey(a, b) & pMask) != part) {
                                continue;
                            }
                            int dstSlot = out.getOrCreateSlot2(a, b);
                            out.foldSlotFrom(dstSlot, chunk, srcSlot);
                        }
                    }
                } else { // width == 3
                    for (PrimitiveAggMap chunk : chunks) {
                        int[] slots = chunk.occupiedSlots();
                        int nn = chunk.nonNullSize();
                        for (int j = 0; j < nn; j++) {
                            int srcSlot = slots[j];
                            long a = chunk.keyAt(srcSlot, 0);
                            long b = chunk.keyAt(srcSlot, 1);
                            long c = chunk.keyAt(srcSlot, 2);
                            if ((int) (PrimitiveAggMap.hashKey(a, b, c) & pMask) != part) {
                                continue;
                            }
                            int dstSlot = out.getOrCreateSlot3(a, b, c);
                            out.foldSlotFrom(dstSlot, chunk, srcSlot);
                        }
                    }
                }
                return out;
            });
        }
        for (int i = 0; i < p; i++) {
            result[i] = tasks[i].join();
        }
        return result;
    }

    /**
     * Top-k over P independent partitions. Each partition picks its own local top-k
     * in parallel; the P × K candidates then go through a second-level top-k to
     * produce the global top-K, and only those winners get boxed + materialized.
     */
    private static QueryResult finalizeRadixInline(BoundSelect plan, List<BoundExpr> groupExprs, List<BoundAgg> aggs,
            PrimitiveKeyShape keyShape, SimpleAggShape aggShape, SlotOrderShape orderShape,
            PrimitiveAggMap[] partitions, Table table) {
        long limit = plan.limit() == null ? Long.MAX_VALUE : plan.limit();
        long offset = plan.offset() == null ? 0 : plan.offset();
        long bound = Math.min((long) Integer.MAX_VALUE, offset + Math.min(limit, Integer.MAX_VALUE - offset));

        int totalSize = 0;
        for (PrimitiveAggMap m : partitions) {
            totalSize += m.nonNullSize();
        }
        int k = (int) Math.min((long) totalSize, Math.max(0L, bound));

        // Parallel: each partition computes its local top-k (or full sort if
        // partition is smaller than k).
        int p = partitions.length;
        @SuppressWarnings("unchecked")
        ForkJoinTask<int[]>[] topTasks = new ForkJoinTask[p];
        int perPartK = k;
        for (int i = 0; i < p; i++) {
            final PrimitiveAggMap part = partitions[i];
            topTasks[i] = ForkJoinPool.commonPool().submit(() -> {
                int sz = part.nonNullSize();
                int localK = Math.min(sz, perPartK);
                if (localK == 0) {
                    return new int[0];
                }
                if (localK >= sz) {
                    return sortAllSlots(part, orderShape);
                }
                return selectTopKSlots(part, orderShape, localK);
            });
        }

        // Collect per-partition winners — each carries (partition index, slot id).
        int[][] partWinners = new int[p][];
        int globalCount = 0;
        for (int i = 0; i < p; i++) {
            partWinners[i] = topTasks[i].join();
            globalCount += partWinners[i].length;
        }

        // Build a combined candidate list and run a final top-k / sort.
        SlotLongExtractor[] extractors = orderShape.extractors();
        boolean[] desc = orderShape.desc();
        int orderN = extractors.length;
        // Flatten candidates into (mapIdx, slot, orderVals) parallel arrays.
        int[] candMap = new int[globalCount];
        int[] candSlot = new int[globalCount];
        long[] candOrder =
                new long[(long) globalCount * orderN > Integer.MAX_VALUE ? Integer.MAX_VALUE : globalCount * orderN];
        int w = 0;
        for (int i = 0; i < p; i++) {
            int[] slots = partWinners[i];
            PrimitiveAggMap m = partitions[i];
            for (int s : slots) {
                candMap[w] = i;
                candSlot[w] = s;
                for (int o = 0; o < orderN; o++) {
                    candOrder[w * orderN + o] = extractors[o].extract(m, s);
                }
                w++;
            }
        }

        // Sort the P × K candidates by order vals using the same comparator.
        Integer[] idx = new Integer[globalCount];
        for (int i = 0; i < globalCount; i++) {
            idx[i] = i;
        }
        Arrays.sort(idx, (a, b) -> compareForTopK(candOrder, a * orderN, candOrder, b * orderN, orderN, desc));

        int fromIdx = (int) Math.min((long) globalCount, offset);
        int toIdx = (int) Math.min((long) globalCount, fromIdx + Math.min(limit, Integer.MAX_VALUE - fromIdx));
        List<@Nullable Object[]> rows = new ArrayList<>(toIdx - fromIdx);
        List<BoundSelectItem> items = plan.items();
        for (int i = fromIdx; i < toIdx; i++) {
            int id = idx[i];
            int mapIdx = candMap[id];
            int slot = candSlot[id];
            PrimitiveAggMap m = partitions[mapIdx];
            List<Object> key = buildKeyFromSlot(m, slot, keyShape, table);
            Aggregator[] states = synthesizeStates(m, slot, aggShape.accepts());
            @Nullable
            Object[] row = new @Nullable Object[items.size()];
            for (int j = 0; j < items.size(); j++) {
                row[j] = evalPostAgg(items.get(j).expr(), groupExprs, key, aggs, states);
            }
            rows.add(row);
        }
        return toResult(plan, rows);
    }

    private static QueryResult executeAggregatedInline(BoundSelect plan, Table table, List<BoundExpr> groupExprs,
            List<BoundAgg> aggs, long n, PrimitiveKeyShape keyShape, SimpleAggShape aggShape,
            SlotOrderShape orderShape) {
        PrimitiveAggMap[] partitions = null;
        PrimitiveAggMap merged;
        if (n >= PARALLEL_THRESHOLD) {
            long[] bounds = chunkBounds(n);
            int kBounds = bounds.length - 1;
            @SuppressWarnings("unchecked")
            ForkJoinTask<PrimitiveAggMap>[] tasks = new ForkJoinTask[kBounds];
            for (int c = 0; c < kBounds; c++) {
                long from = bounds[c];
                long to = bounds[c + 1];
                tasks[c] = ForkJoinPool.commonPool()
                        .submit(() -> scanAggChunkInline(plan, table, keyShape, aggShape, from, to));
            }
            List<PrimitiveAggMap> chunks = new ArrayList<>(kBounds);
            int totalHint = 0;
            boolean anyNull = false;
            for (ForkJoinTask<PrimitiveAggMap> t : tasks) {
                PrimitiveAggMap m = t.join();
                chunks.add(m);
                totalHint += m.size();
                anyNull |= m.hasNull();
            }
            // Radix-parallel merge for big result sets — avoids the O(totalHint)
            // sequential merge through a single map. Null groups disable it
            // (null is in a separate slot per map, doesn't belong to any bucket).
            if (totalHint >= RADIX_THRESHOLD && !anyNull) {
                int p = nextPowerOfTwoPartitions(kBounds);
                partitions = mergeChunksRadix(chunks, p, keyShape, aggShape, totalHint);
                merged = partitions[0]; // only used for hasNull; we take radix path below
            } else {
                merged = new PrimitiveAggMap(keyShape.width(), totalHint, aggShape.mapOps(), aggShape.longFields(),
                        aggShape.doubleFields());
                merged.ensureCapacity(totalHint);
                for (PrimitiveAggMap m : chunks) {
                    merged.merge(m);
                }
            }
        } else {
            merged = scanAggChunkInline(plan, table, keyShape, aggShape, 0, n);
        }

        // Radix path: run top-k per partition in parallel, merge P × K candidates.
        if (partitions != null) {
            return finalizeRadixInline(plan, groupExprs, aggs, keyShape, aggShape, orderShape, partitions, table);
        }

        // Null-group primitive heap is tricky (null keys don't compare as longs);
        // if any row produced a null group, fall back to the generic materialize.
        if (merged.hasNull()) {
            return fallbackFinalize(plan, groupExprs, aggs, keyShape, aggShape, merged, table);
        }

        long limit = plan.limit() == null ? Long.MAX_VALUE : plan.limit();
        long offset = plan.offset() == null ? 0 : plan.offset();
        long bound = Math.min((long) Integer.MAX_VALUE, offset + Math.min(limit, Integer.MAX_VALUE - offset));
        int k = (int) Math.min((long) merged.nonNullSize(), Math.max(0L, bound));

        int[] winnerSlots;
        if (k >= merged.nonNullSize()) {
            // Everyone makes it — full sort by order vals.
            winnerSlots = sortAllSlots(merged, orderShape);
        } else {
            winnerSlots = selectTopKSlots(merged, orderShape, k);
        }

        int fromIdx = (int) Math.min((long) winnerSlots.length, offset);
        int toIdx = (int) Math.min((long) winnerSlots.length, fromIdx + Math.min(limit, Integer.MAX_VALUE - fromIdx));

        List<@Nullable Object[]> rows = new ArrayList<>(toIdx - fromIdx);
        List<BoundSelectItem> items = plan.items();
        for (int i = fromIdx; i < toIdx; i++) {
            int slot = winnerSlots[i];
            List<Object> key = buildKeyFromSlot(merged, slot, keyShape, table);
            Aggregator[] states = synthesizeStates(merged, slot, aggShape.accepts());
            @Nullable
            Object[] row = new @Nullable Object[items.size()];
            for (int j = 0; j < items.size(); j++) {
                row[j] = evalPostAgg(items.get(j).expr(), groupExprs, key, aggs, states);
            }
            rows.add(row);
        }
        return toResult(plan, rows);
    }

    private static List<Object> buildKeyFromSlot(PrimitiveAggMap map, int slot, PrimitiveKeyShape shape, Table table) {
        int n = shape.exprCount();
        // Fast path: single column, no literal slots (covers most existing
        // one- and two-col GROUP BY shapes without paying for the general loop).
        if (n == 1) {
            int keyIdx = shape.exprToKey()[0];
            Object v = resolveKeyValue(shape, table, map, slot, 0, keyIdx);
            return java.util.Collections.singletonList(v);
        }
        @Nullable
        Object[] values = new @Nullable Object[n];
        for (int i = 0; i < n; i++) {
            values[i] = resolveKeyValue(shape, table, map, slot, i, shape.exprToKey()[i]);
        }
        return Arrays.asList(values);
    }

    private static @Nullable Object resolveKeyValue(PrimitiveKeyShape shape, Table table, PrimitiveAggMap map, int slot,
            int exprIdx, int keyIdx) {
        if (keyIdx == -1) {
            return shape.literalValues()[exprIdx];
        }
        if (keyIdx == -2) {
            BoundExpr[] derived = shape.derivedExprs();
            if (derived == null) {
                throw new IllegalStateException("derived expr marker without derivedExprs");
            }
            BoundExpr d = derived[exprIdx];
            return evalFromKeys(d, shape, table, map, slot);
        }
        return boxKey(table, map.keyAt(slot, keyIdx), false, shape.colIdx()[keyIdx], shape.kinds()[keyIdx]);
    }

    /**
     * Evaluate a derived group expression against the primitive hash key slot.
     * BoundColumn refs resolve to the stored raw key (boxed via {@link #boxKey});
     * every other node recurses structurally. Mirrors
     * {@link ExprEvaluator#evalScalarCall}'s fn shape so we don't depend on row
     * reads — a derived expr by construction doesn't need them.
     */
    private static @Nullable Object evalFromKeys(BoundExpr e, PrimitiveKeyShape shape, Table table, PrimitiveAggMap map,
            int slot) {
        return switch (e) {
            case BoundLiteral l -> l.value();
            case BoundColumn c -> {
                int ki = findDirectKeyIdx(shape, c.index());
                if (ki < 0) {
                    throw new IllegalStateException("derived BoundColumn " + c.name() + " not in key");
                }
                yield boxKey(table, map.keyAt(slot, ki), false, shape.colIdx()[ki], shape.kinds()[ki]);
            }
            case BoundUnary u -> applyUnary(u, evalFromKeys(u.operand(), shape, table, map, slot));
            case BoundBinary b -> applyBinary(b, evalFromKeys(b.left(), shape, table, map, slot),
                    evalFromKeys(b.right(), shape, table, map, slot));
            case BoundIsNull n -> {
                Object v = evalFromKeys(n.operand(), shape, table, map, slot);
                yield n.negated() ? v != null : v == null;
            }
            case BoundCase c -> {
                Object result = null;
                for (BoundWhen w : c.whens()) {
                    Object cond = evalFromKeys(w.when(), shape, table, map, slot);
                    if (Boolean.TRUE.equals(cond)) {
                        result = evalFromKeys(w.then(), shape, table, map, slot);
                        break;
                    }
                }
                if (result == null && c.elseExpr() != null) {
                    result = evalFromKeys(c.elseExpr(), shape, table, map, slot);
                }
                yield result;
            }
            case BoundScalarCall sc ->
                ExprEvaluator.evalScalarCall(sc, a -> evalFromKeys(a, shape, table, map, slot));
            case BoundLike l -> {
                Object v = evalFromKeys(l.value(), shape, table, map, slot);
                Object p = evalFromKeys(l.pattern(), shape, table, map, slot);
                if (v == null || p == null) {
                    yield null;
                }
                boolean match = io.jpointdb.core.sql.LikeMatcher.forPattern((String) p).matches((String) v);
                yield l.negated() ? !match : match;
            }
            case BoundInList il -> {
                Object v = evalFromKeys(il.value(), shape, table, map, slot);
                if (v == null) {
                    yield null;
                }
                boolean found = false;
                for (BoundExpr item : il.items()) {
                    Object iv = evalFromKeys(item, shape, table, map, slot);
                    if (iv != null && ExprEvaluator.compare(v, iv) == 0) {
                        found = true;
                        break;
                    }
                }
                yield il.negated() != found;
            }
            case BoundAgg a -> throw new AssertionError("aggregate in derived group expr");
            case BoundDictBitsetMatch m -> throw new AssertionError("dict-bitset in derived group expr");
        };
    }

    private static int findDirectKeyIdx(PrimitiveKeyShape shape, int columnIdx) {
        int[] cols = shape.colIdx();
        KeyKind[] kinds = shape.kinds();
        for (int i = 0; i < cols.length; i++) {
            // Only direct-column key slots count — DICT_STRING_EXTRACT_LONG slots
            // reuse the same column but hold a derived long, not the raw value.
            if (cols[i] == columnIdx && kinds[i] != KeyKind.DICT_STRING_EXTRACT_LONG) {
                return i;
            }
        }
        return -1;
    }

    private static Aggregator[] synthesizeStates(PrimitiveAggMap map, int slot, InlineAccept[] accepts) {
        Aggregator[] out = new Aggregator[accepts.length];
        for (int i = 0; i < accepts.length; i++) {
            out[i] = new PrecomputedAggregator(accepts[i].result(map, slot));
        }
        return out;
    }

    /** Stateless wrapper that just reports a precomputed boxed result. */
    private static final class PrecomputedAggregator extends Aggregator {
        private final @Nullable Object value;

        PrecomputedAggregator(@Nullable Object value) {
            this.value = value;
        }

        @Override
        void accept(@Nullable Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        void merge(Aggregator other) {
            throw new UnsupportedOperationException();
        }

        @Override
        @Nullable
        Object result() {
            return value;
        }
    }

    /**
     * Fallback when the inline primitive fast path can't finish on its own (null
     * group present, or winner set still requires a complex materialize). Walks
     * every slot, synthesizes Aggregator[] + boxed key, and hands to
     * {@link #finalizeAggregated} — same work as the old path, only invoked on the
     * rare null-group edge.
     */
    private static QueryResult fallbackFinalize(BoundSelect plan, List<BoundExpr> groupExprs, List<BoundAgg> aggs,
            PrimitiveKeyShape keyShape, SimpleAggShape aggShape, PrimitiveAggMap map, Table table) {
        List<GroupEntry> groups = new ArrayList<>(map.size());
        int[] slots = map.occupiedSlots();
        int sz = map.nonNullSize();
        InlineAccept[] accepts = aggShape.accepts();
        for (int i = 0; i < sz; i++) {
            int slot = slots[i];
            groups.add(
                    new GroupEntry(buildKeyFromSlot(map, slot, keyShape, table), synthesizeStates(map, slot, accepts)));
        }
        if (map.hasNull()) {
            int slot = map.nullSlot();
            // Null group: every column component reports null; literal positions
            // keep their constant.
            int g = keyShape.exprCount();
            int[] exprToKey = keyShape.exprToKey();
            @Nullable
            Object[] literalValues = keyShape.literalValues();
            List<Object> key;
            if (g == 1) {
                int ki = exprToKey[0];
                key = java.util.Collections.singletonList(ki < 0 ? literalValues[0] : null);
            } else {
                @Nullable
                Object[] ka = new @Nullable Object[g];
                for (int i = 0; i < g; i++) {
                    int ki = exprToKey[i];
                    ka[i] = ki < 0 ? literalValues[i] : null;
                }
                key = Arrays.asList(ka);
            }
            groups.add(new GroupEntry(key, synthesizeStates(map, slot, accepts)));
        }
        return finalizeAggregated(plan, groupExprs, aggs, groups);
    }

    /**
     * Bounded-top-k over slot indices. Comparison uses primitive {@code long}s
     * extracted on demand — no Object[], no boxing. Returns winners in sorted order
     * (best first).
     */
    private static int[] selectTopKSlots(PrimitiveAggMap map, SlotOrderShape order, int k) {
        if (k <= 0) {
            return new int[0];
        }
        SlotLongExtractor[] extractors = order.extractors();
        boolean[] desc = order.desc();
        int[] slots = map.occupiedSlots();
        int n = map.nonNullSize();
        // Max-heap semantics for top-k: keep the k "best" (by the multi-key
        // comparator). Use a min-heap ordered by "worst-first" so the root is
        // the item to evict. Store candidate long vectors inline: heapSlots[i]
        // with heapOrderVals[i * orderN .. +orderN].
        int orderN = extractors.length;
        int[] heapSlots = new int[k];
        long[] heapOrderVals = new long[(long) k * orderN > Integer.MAX_VALUE ? Integer.MAX_VALUE : k * orderN];
        int heapSize = 0;
        long[] scratch = new long[orderN];
        for (int i = 0; i < n; i++) {
            int slot = slots[i];
            for (int j = 0; j < orderN; j++) {
                scratch[j] = extractors[j].extract(map, slot);
            }
            if (heapSize < k) {
                heapSlots[heapSize] = slot;
                System.arraycopy(scratch, 0, heapOrderVals, heapSize * orderN, orderN);
                heapSize++;
                siftUpMinHeap(heapSlots, heapOrderVals, heapSize - 1, orderN, desc);
            } else if (compareForTopK(scratch, 0, heapOrderVals, 0, orderN, desc) < 0) {
                // scratch is better than current worst (root). Replace root and sift.
                heapSlots[0] = slot;
                System.arraycopy(scratch, 0, heapOrderVals, 0, orderN);
                siftDownMinHeap(heapSlots, heapOrderVals, heapSize, 0, orderN, desc);
            }
        }
        // Extract sorted: repeatedly pull root (worst), reverse at end.
        int[] result = new int[heapSize];
        for (int i = heapSize - 1; i >= 0; i--) {
            result[i] = heapSlots[0];
            heapSlots[0] = heapSlots[i];
            System.arraycopy(heapOrderVals, i * orderN, heapOrderVals, 0, orderN);
            siftDownMinHeap(heapSlots, heapOrderVals, i, 0, orderN, desc);
        }
        return result;
    }

    /** Heap order: "worst" at root so better candidates displace it. */
    private static void siftUpMinHeap(int[] slots, long[] vals, int i, int orderN, boolean[] desc) {
        while (i > 0) {
            int parent = (i - 1) >>> 1;
            if (compareForTopK(vals, i * orderN, vals, parent * orderN, orderN, desc) > 0) {
                swapHeap(slots, vals, i, parent, orderN);
                i = parent;
            } else {
                break;
            }
        }
    }

    private static void siftDownMinHeap(int[] slots, long[] vals, int size, int i, int orderN, boolean[] desc) {
        while (true) {
            int l = 2 * i + 1;
            int r = 2 * i + 2;
            int worst = i;
            if (l < size && compareForTopK(vals, l * orderN, vals, worst * orderN, orderN, desc) > 0) {
                worst = l;
            }
            if (r < size && compareForTopK(vals, r * orderN, vals, worst * orderN, orderN, desc) > 0) {
                worst = r;
            }
            if (worst == i) {
                break;
            }
            swapHeap(slots, vals, i, worst, orderN);
            i = worst;
        }
    }

    private static void swapHeap(int[] slots, long[] vals, int i, int j, int orderN) {
        int ts = slots[i];
        slots[i] = slots[j];
        slots[j] = ts;
        for (int o = 0; o < orderN; o++) {
            long tv = vals[i * orderN + o];
            vals[i * orderN + o] = vals[j * orderN + o];
            vals[j * orderN + o] = tv;
        }
    }

    /**
     * Returns positive when {@code a} sorts WORSE than {@code b} under the user's
     * {@code desc} flags — i.e., a is a better candidate to be the heap's eviction
     * target. DESC: smaller value = worse. ASC: larger value = worse.
     */
    private static int compareForTopK(long[] aArr, int aOff, long[] bArr, int bOff, int orderN, boolean[] desc) {
        for (int i = 0; i < orderN; i++) {
            long a = aArr[aOff + i];
            long b = bArr[bOff + i];
            if (a != b) {
                int cmp = Long.compare(a, b);
                return desc[i] ? -cmp : cmp;
            }
        }
        return 0;
    }

    private static int[] sortAllSlots(PrimitiveAggMap map, SlotOrderShape order) {
        int n = map.nonNullSize();
        int[] slots = new int[n];
        System.arraycopy(map.occupiedSlots(), 0, slots, 0, n);
        SlotLongExtractor[] extractors = order.extractors();
        boolean[] desc = order.desc();
        int orderN = extractors.length;
        long[] vals = new long[(long) n * orderN > Integer.MAX_VALUE ? Integer.MAX_VALUE : n * orderN];
        for (int i = 0; i < n; i++) {
            int slot = slots[i];
            for (int j = 0; j < orderN; j++) {
                vals[i * orderN + j] = extractors[j].extract(map, slot);
            }
        }
        Integer[] idx = new Integer[n];
        for (int i = 0; i < n; i++) {
            idx[i] = i;
        }
        final long[] finalVals = vals;
        final boolean[] finalDesc = desc;
        final int finalOrderN = orderN;
        // compareForTopK returns positive when `a` is worse than `b` (heap "evict"
        // semantic); negative when `a` is better — exactly the natural sort
        // contract where the "better" element sorts earlier.
        Arrays.sort(idx, (a, b) -> compareForTopK(finalVals, a * finalOrderN, finalVals, b * finalOrderN, finalOrderN,
                finalDesc));
        int[] out = new int[n];
        for (int i = 0; i < n; i++) {
            out[i] = slots[idx[i]];
        }
        return out;
    }

    private static PrimitiveAggMap scanAggChunkInline(BoundSelect plan, Table table, PrimitiveKeyShape keyShape,
            SimpleAggShape aggShape, long from, long to) {
        ExprEvaluator ev = new ExprEvaluator(table);
        int chunkHint = (int) Math.min((long) Integer.MAX_VALUE, Math.max(64L, to - from));
        PrimitiveAggMap map = new PrimitiveAggMap(keyShape.width(), chunkHint, aggShape.mapOps(), aggShape.longFields(),
                aggShape.doubleFields());
        BoundExpr where = plan.where();
        InlineAccept[] accepts = aggShape.accepts();

        LongKeyReader reader0 = keyReader(table, keyShape, 0);
        long[][] lf = map.longFields;
        double[][] df = map.doubleFields;
        int cap = map.capacity();
        if (keyShape.width() == 1) {
            for (long r = from; r < to; r++) {
                if (where != null && !truthy(ev.eval(where, r))) {
                    continue;
                }
                int slot;
                if (reader0.isNullAt(r)) {
                    slot = map.getOrCreateNullSlot();
                } else {
                    slot = map.getOrCreateSlot1(reader0.keyAt(r));
                }
                if (map.capacity() != cap) {
                    lf = map.longFields;
                    df = map.doubleFields;
                    cap = map.capacity();
                }
                for (int i = 0; i < accepts.length; i++) {
                    accepts[i].acceptRow(lf, df, slot, r);
                }
            }
        } else if (keyShape.width() == 2) {
            LongKeyReader reader1 = keyReader(table, keyShape, 1);
            for (long r = from; r < to; r++) {
                if (where != null && !truthy(ev.eval(where, r))) {
                    continue;
                }
                int slot;
                if (reader0.isNullAt(r) || reader1.isNullAt(r)) {
                    slot = map.getOrCreateNullSlot();
                } else {
                    slot = map.getOrCreateSlot2(reader0.keyAt(r), reader1.keyAt(r));
                }
                if (map.capacity() != cap) {
                    lf = map.longFields;
                    df = map.doubleFields;
                    cap = map.capacity();
                }
                for (int i = 0; i < accepts.length; i++) {
                    accepts[i].acceptRow(lf, df, slot, r);
                }
            }
        } else { // width == 3
            LongKeyReader reader1 = keyReader(table, keyShape, 1);
            LongKeyReader reader2 = keyReader(table, keyShape, 2);
            for (long r = from; r < to; r++) {
                if (where != null && !truthy(ev.eval(where, r))) {
                    continue;
                }
                int slot;
                if (reader0.isNullAt(r) || reader1.isNullAt(r) || reader2.isNullAt(r)) {
                    slot = map.getOrCreateNullSlot();
                } else {
                    slot = map.getOrCreateSlot3(reader0.keyAt(r), reader1.keyAt(r), reader2.keyAt(r));
                }
                if (map.capacity() != cap) {
                    lf = map.longFields;
                    df = map.doubleFields;
                    cap = map.capacity();
                }
                for (int i = 0; i < accepts.length; i++) {
                    accepts[i].acceptRow(lf, df, slot, r);
                }
            }
        }
        return map;
    }

    private static @Nullable PrimitiveKeyShape detectPrimitiveKey(List<BoundExpr> groupExprs, Table table) {
        int g = groupExprs.size();
        if (g < 1) {
            return null;
        }
        int[] exprToKey = new int[g];
        @Nullable
        Object[] literalValues = new @Nullable Object[g];
        BoundExpr[] derivedExprs = null;
        // Pass 1: classify + count hash-key columns. A groupExpr becomes a key
        // column in three ways: direct BoundColumn on a primitive/DICT column,
        // BoundLiteral (no key col — a constant partition), extract(...) over a
        // DICT STRING column (its own precomputed long key), or a "derived"
        // expression whose value is a pure function of BoundColumns that appear
        // as direct BoundColumn groupExprs. Derived exprs don't add new key
        // columns — their value is materialized from the stored raw keys.
        int colCount = 0;
        // Map from BoundColumn.index() -> the key slot it occupies, if it was
        // seen as a direct BoundColumn groupExpr (not inside a derived expr).
        java.util.HashMap<Integer, Integer> directColToKey = new java.util.HashMap<>();
        for (int i = 0; i < g; i++) {
            BoundExpr e = groupExprs.get(i);
            if (e instanceof BoundColumn bc) {
                Integer existing = directColToKey.get(bc.index());
                if (existing != null) {
                    // Duplicate column reference — reuse the same key slot.
                    exprToKey[i] = existing;
                } else {
                    exprToKey[i] = colCount;
                    directColToKey.put(bc.index(), colCount);
                    colCount++;
                }
            } else if (e instanceof BoundLiteral lit) {
                exprToKey[i] = -1;
                literalValues[i] = lit.value();
            } else if (extractOverDictString(e, table) >= 0) {
                // Each extract call gets its own precomputed key slot even if
                // the underlying STRING column is also grouped directly.
                exprToKey[i] = colCount++;
            } else {
                // Candidate derived expr — validate in pass 3 once we know the
                // full set of direct-column key slots.
                exprToKey[i] = -2;
                if (derivedExprs == null) {
                    derivedExprs = new BoundExpr[g];
                }
                derivedExprs[i] = e;
            }
        }
        if (colCount < 1 || colCount > 3) {
            return null;
        }
        int[] cols = new int[colCount];
        KeyKind[] kinds = new KeyKind[colCount];
        long[][] dictMaps = null;
        for (int i = 0; i < g; i++) {
            int ki = exprToKey[i];
            if (ki < 0) {
                continue;
            }
            if (cols[ki] != 0 || kinds[ki] != null) {
                // Already filled by an earlier duplicate; skip.
                continue;
            }
            BoundExpr e = groupExprs.get(i);
            if (e instanceof BoundColumn bc) {
                ColumnType t = table.columnMeta(bc.index()).type();
                if (t == ColumnType.I32) {
                    kinds[ki] = KeyKind.I32;
                } else if (t == ColumnType.I64) {
                    kinds[ki] = KeyKind.I64;
                } else if (t == ColumnType.STRING
                        && table.string(bc.index()).mode() == io.jpointdb.core.column.StringColumnWriter.Mode.DICT) {
                    kinds[ki] = KeyKind.DICT_STRING;
                } else {
                    return null;
                }
                cols[ki] = bc.index();
            } else if (e instanceof BoundScalarCall sc) {
                int colIdx = extractOverDictString(sc, table);
                if (colIdx < 0) {
                    return null;
                }
                long[] map = buildExtractMap(sc, table.string(colIdx).dictionary());
                if (map == null) {
                    return null;
                }
                if (dictMaps == null) {
                    dictMaps = new long[colCount][];
                }
                dictMaps[ki] = map;
                kinds[ki] = KeyKind.DICT_STRING_EXTRACT_LONG;
                cols[ki] = colIdx;
            } else {
                return null;
            }
        }
        // Pass 3: validate derived exprs depend only on direct-column keys.
        if (derivedExprs != null) {
            for (int i = 0; i < g; i++) {
                BoundExpr d = derivedExprs[i];
                if (d == null) {
                    continue;
                }
                if (!dependsOnlyOnDirectKeys(d, directColToKey.keySet())) {
                    return null;
                }
            }
        }
        return new PrimitiveKeyShape(cols, kinds, exprToKey, literalValues, dictMaps, derivedExprs);
    }

    /**
     * A groupExpr is "derived" if every {@link BoundColumn} it transitively
     * references is a direct column of the GROUP BY list — i.e., the expression
     * is a pure function of the hash-key state, and we can recompute it at
     * materialize time from stored raw keys. Rejects aggregates, subqueries,
     * and anything whose column refs aren't all tracked.
     */
    private static boolean dependsOnlyOnDirectKeys(BoundExpr e, java.util.Set<Integer> keyColIndexes) {
        return switch (e) {
            case BoundLiteral l -> true;
            case BoundColumn c -> keyColIndexes.contains(c.index());
            case BoundUnary u -> dependsOnlyOnDirectKeys(u.operand(), keyColIndexes);
            case BoundBinary b -> dependsOnlyOnDirectKeys(b.left(), keyColIndexes)
                    && dependsOnlyOnDirectKeys(b.right(), keyColIndexes);
            case BoundIsNull n -> dependsOnlyOnDirectKeys(n.operand(), keyColIndexes);
            case BoundLike l -> dependsOnlyOnDirectKeys(l.value(), keyColIndexes)
                    && dependsOnlyOnDirectKeys(l.pattern(), keyColIndexes);
            case BoundInList il -> {
                if (!dependsOnlyOnDirectKeys(il.value(), keyColIndexes)) {
                    yield false;
                }
                for (BoundExpr item : il.items()) {
                    if (!dependsOnlyOnDirectKeys(item, keyColIndexes)) {
                        yield false;
                    }
                }
                yield true;
            }
            case BoundCase c -> {
                for (BoundWhen w : c.whens()) {
                    if (!dependsOnlyOnDirectKeys(w.when(), keyColIndexes)
                            || !dependsOnlyOnDirectKeys(w.then(), keyColIndexes)) {
                        yield false;
                    }
                }
                yield c.elseExpr() == null || dependsOnlyOnDirectKeys(c.elseExpr(), keyColIndexes);
            }
            case BoundScalarCall sc -> {
                for (BoundExpr arg : sc.args()) {
                    if (!dependsOnlyOnDirectKeys(arg, keyColIndexes)) {
                        yield false;
                    }
                }
                yield true;
            }
            // Aggregates and bitset-matches don't belong in GROUP BY; reject.
            case BoundAgg a -> false;
            case BoundDictBitsetMatch m -> false;
        };
    }

    /**
     * Returns the DICT-encoded STRING column index if {@code e} is a scalar
     * extract/date_trunc-like call whose single arg is such a column; else -1.
     * Current fast path supports {@code extract:<field>} only.
     */
    private static int extractOverDictString(BoundExpr e, Table table) {
        if (!(e instanceof BoundScalarCall sc) || !sc.name().startsWith("extract:")) {
            return -1;
        }
        if (sc.args().size() != 1 || !(sc.args().get(0) instanceof BoundColumn bc)) {
            return -1;
        }
        if (table.columnMeta(bc.index()).type() != ColumnType.STRING) {
            return -1;
        }
        io.jpointdb.core.column.StringColumn col = table.string(bc.index());
        if (col.mode() != io.jpointdb.core.column.StringColumnWriter.Mode.DICT) {
            return -1;
        }
        if (col.dictionary() == null) {
            return -1;
        }
        return bc.index();
    }

    /**
     * Precomputes a {@code dictId → extracted-long} table so the scan reduces to a
     * single array load per row. Any dict entry that {@link ScalarFns#extract}
     * rejects (malformed timestamp) returns {@link Long#MIN_VALUE} in the map —
     * row-level nulling is caught by the null-group path via the source column's
     * null mask, and this sentinel only shows up when the dict holds a bad string.
     */
    private static long @Nullable [] buildExtractMap(BoundScalarCall sc,
            io.jpointdb.core.column.@Nullable Dictionary d) {
        if (d == null) {
            return null;
        }
        String field = sc.name().substring("extract:".length());
        int n = d.size();
        long[] map = new long[n];
        for (int i = 0; i < n; i++) {
            Long v = ScalarFns.extract(field, d.stringAt(i));
            map[i] = v == null ? Long.MIN_VALUE : v;
        }
        return map;
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
        int g = shape.exprCount();
        int[] exprToKey = shape.exprToKey();
        @Nullable
        Object[] literalValues = shape.literalValues();
        int[] colIdx = shape.colIdx();
        KeyKind[] kinds = shape.kinds();
        if (shape.width() == 1) {
            merged.forEachKey1((k, isNull, states) -> {
                Object boxed = boxKey(table, k, isNull, colIdx[0], kinds[0]);
                List<Object> key = buildKeyList(g, exprToKey, literalValues, boxed, null);
                groups.add(new GroupEntry(key, states));
            });
        } else {
            merged.forEachKey2((a, b, isNull, states) -> {
                Object boxedA = boxKey(table, a, isNull, colIdx[0], kinds[0]);
                Object boxedB = boxKey(table, b, isNull, colIdx[1], kinds[1]);
                List<Object> key = buildKeyList(g, exprToKey, literalValues, boxedA, boxedB);
                groups.add(new GroupEntry(key, states));
            });
        }
        return finalizeAggregated(plan, groupExprs, aggs, groups);
    }

    /**
     * Assembles a GROUP BY key list that honours the original expr ordering and
     * splices literals back in at the positions {@link PrimitiveKeyShape} recorded.
     */
    private static List<Object> buildKeyList(int exprCount, int[] exprToKey, @Nullable Object[] literalValues,
            @Nullable Object keyA, @Nullable Object keyB) {
        if (exprCount == 1) {
            int ki = exprToKey[0];
            return java.util.Collections.singletonList(ki < 0 ? literalValues[0] : keyA);
        }
        @Nullable
        Object[] out = new @Nullable Object[exprCount];
        for (int i = 0; i < exprCount; i++) {
            int ki = exprToKey[i];
            if (ki < 0) {
                out[i] = literalValues[i];
            } else {
                out[i] = (ki == 0) ? keyA : keyB;
            }
        }
        return Arrays.asList(out);
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

    private static LongKeyReader keyReader(Table table, PrimitiveKeyShape shape, int keyIdx) {
        int colIdx = shape.colIdx()[keyIdx];
        KeyKind kind = shape.kinds()[keyIdx];
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
            case DICT_STRING_EXTRACT_LONG -> {
                io.jpointdb.core.column.StringColumn c = table.string(colIdx);
                long[][] maps = shape.dictMaps();
                if (maps == null || maps[keyIdx] == null) {
                    throw new IllegalStateException("DICT_STRING_EXTRACT_LONG requires a dict map at key " + keyIdx);
                }
                long[] dictMap = maps[keyIdx];
                yield new LongKeyReader() {
                    @Override
                    public boolean isNullAt(long row) {
                        return c.isNullAt(row);
                    }

                    @Override
                    public long keyAt(long row) {
                        return dictMap[c.idAt(row)];
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

        LongKeyReader reader0 = keyReader(table, shape, 0);
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
            LongKeyReader reader1 = keyReader(table, shape, 1);
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
            case DICT_STRING_EXTRACT_LONG -> k;
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
     * Compiles an ORDER BY expression once per query into an extractor that avoids
     * per-group findMatch / sameAgg scans. Direct hits are the common case (ORDER
     * BY <groupKey> or ORDER BY <aggregate>); anything else falls back to the
     * general evalPostAgg.
     */
    private static OrderResolver compileOrderResolver(BoundExpr expr, List<BoundExpr> groupExprs, List<BoundAgg> aggs) {
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
            case BoundDictBitsetMatch m ->
                throw new AssertionError("dict-bitset match should only appear in WHERE, not post-agg");
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
            case BoundDictBitsetMatch ignored -> {}
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

    @SuppressWarnings("NullAway")
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
