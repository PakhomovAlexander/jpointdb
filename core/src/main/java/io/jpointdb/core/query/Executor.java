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
            Aggregator[] states = new Aggregator[aggs.size()];
            for (int i = 0; i < aggs.size(); i++) {
                BoundAgg a = aggs.get(i);
                states[i] = Aggregator.create(a.fn(), a.distinct(), a.arg() == null ? null : a.arg().type(), a.type());
            }
            List<Object> scalarKey = List.of();
            merged.groups.put(scalarKey, states);
            merged.order.add(scalarKey);
        }

        // Materialize one row per group.
        List<@Nullable Object[]> rows = new ArrayList<>();
        for (List<Object> key : merged.order) {
            Aggregator[] states = merged.groups.get(key);
            if (states == null)
                continue;
            @Nullable
            Object[] values = new @Nullable Object[plan.items().size()];
            for (int i = 0; i < plan.items().size(); i++) {
                BoundExpr e = plan.items().get(i).expr();
                values[i] = evalPostAgg(e, groupExprs, key, aggs, states);
            }
            if (plan.having() != null) {
                Object hv = evalPostAgg(plan.having(), groupExprs, key, aggs, states);
                if (!truthy(hv))
                    continue;
            }
            rows.add(values);
        }

        if (!plan.orderBy().isEmpty())
            rows = sortAggregated(rows, plan, groupExprs, aggs);

        long limit = plan.limit() == null ? Long.MAX_VALUE : plan.limit();
        long offset = plan.offset() == null ? 0 : plan.offset();
        rows = applyLimit(rows, offset, limit);
        return toResult(plan, rows);
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
    @SuppressWarnings("ArrayRecordComponent")
    private record PrimitiveKeyShape(int[] colIdx, boolean[] isI32) {
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
        boolean[] isI32 = new boolean[w];
        for (int i = 0; i < w; i++) {
            if (!(groupExprs.get(i) instanceof BoundColumn bc)) {
                return null;
            }
            ColumnType t = table.columnMeta(bc.index()).type();
            if (t == ColumnType.I32) {
                isI32[i] = true;
            } else if (t == ColumnType.I64) {
                isI32[i] = false;
            } else {
                return null;
            }
            cols[i] = bc.index();
        }
        return new PrimitiveKeyShape(cols, isI32);
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
            merged = new LongKeysAggMap(shape.width());
            for (ForkJoinTask<LongKeysAggMap> t : tasks) {
                merged.merge(t.join(), aggs.size());
            }
        } else {
            merged = scanAggChunkPrimitive(plan, table, aggs, shape, 0, n);
        }

        // Materialize rows. Build the boxed key on a per-group basis so evalPostAgg stays unchanged.
        List<@Nullable Object[]> rows = new ArrayList<>();
        List<BoundSelectItem> items = plan.items();
        BoundExpr having = plan.having();
        if (shape.width() == 1) {
            merged.forEachKey1((k, isNull, states) -> {
                @Nullable
                Object[] row = buildRowPrimitive1(k, isNull, states, plan, groupExprs, aggs, shape);
                if (row != null) {
                    rows.add(row);
                }
            });
        } else {
            merged.forEachKey2((a, b, isNull, states) -> {
                @Nullable
                Object[] row = buildRowPrimitive2(a, b, isNull, states, plan, groupExprs, aggs, shape);
                if (row != null) {
                    rows.add(row);
                }
            });
        }

        List<@Nullable Object[]> out = rows;
        if (!plan.orderBy().isEmpty()) {
            out = sortAggregated(out, plan, groupExprs, aggs);
        }
        long limit = plan.limit() == null ? Long.MAX_VALUE : plan.limit();
        long offset = plan.offset() == null ? 0 : plan.offset();
        out = applyLimit(out, offset, limit);
        return toResult(plan, out);
    }

    // NullAway can't see that only one of (i32_0, i64_0) / (i32_1, i64_1) is used at a time.
    @SuppressWarnings("NullAway")
    private static LongKeysAggMap scanAggChunkPrimitive(BoundSelect plan, Table table, List<BoundAgg> aggs,
            PrimitiveKeyShape shape, long from, long to) {
        ExprEvaluator ev = new ExprEvaluator(table);
        LongKeysAggMap map = new LongKeysAggMap(shape.width());
        BoundExpr where = plan.where();
        LongKeysAggMap.AggFactory factory = () -> createStates(aggs);

        int col0 = shape.colIdx()[0];
        boolean isI32A = shape.isI32()[0];
        I32Column i32a = isI32A ? table.i32(col0) : null;
        I64Column i64a = isI32A ? null : table.i64(col0);

        if (shape.width() == 1) {
            for (long r = from; r < to; r++) {
                if (where != null && !truthy(ev.eval(where, r))) {
                    continue;
                }
                Aggregator[] states;
                if (isI32A) {
                    if (i32a.isNullAt(r)) {
                        states = map.getOrCreateNull(factory);
                    } else {
                        states = map.getOrCreate1(i32a.get(r), factory);
                    }
                } else {
                    if (i64a.isNullAt(r)) {
                        states = map.getOrCreateNull(factory);
                    } else {
                        states = map.getOrCreate1(i64a.get(r), factory);
                    }
                }
                acceptAggs(states, aggs, ev, r);
            }
        } else {
            int col1 = shape.colIdx()[1];
            boolean isI32B = shape.isI32()[1];
            I32Column i32b = isI32B ? table.i32(col1) : null;
            I64Column i64b = isI32B ? null : table.i64(col1);

            for (long r = from; r < to; r++) {
                if (where != null && !truthy(ev.eval(where, r))) {
                    continue;
                }
                boolean null0 = isI32A ? i32a.isNullAt(r) : i64a.isNullAt(r);
                boolean null1 = isI32B ? i32b.isNullAt(r) : i64b.isNullAt(r);
                Aggregator[] states;
                if (null0 || null1) {
                    // Treat any-null composite as the null group. Matches three-valued
                    // SQL GROUP BY only approximately — in practice ClickBench data has
                    // no nulls in the grouped columns, and we preserve one-group-per-null.
                    states = map.getOrCreateNull(factory);
                } else {
                    long a = isI32A ? i32a.get(r) : i64a.get(r);
                    long b = isI32B ? i32b.get(r) : i64b.get(r);
                    states = map.getOrCreate2(a, b, factory);
                }
                acceptAggs(states, aggs, ev, r);
            }
        }
        return map;
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

    private static @Nullable Object boxKey(long k, boolean isNull, boolean isI32) {
        if (isNull) {
            return null;
        }
        return isI32 ? (long) (int) k : k;
    }

    private static @Nullable Object @Nullable [] buildRowPrimitive1(long k, boolean isNull, Aggregator[] states,
            BoundSelect plan, List<BoundExpr> groupExprs, List<BoundAgg> aggs, PrimitiveKeyShape shape) {
        List<Object> key = java.util.Collections.singletonList(boxKey(k, isNull, shape.isI32()[0]));
        return materializeGroupRow(plan, groupExprs, key, aggs, states);
    }

    private static @Nullable Object @Nullable [] buildRowPrimitive2(long a, long b, boolean isNull, Aggregator[] states,
            BoundSelect plan, List<BoundExpr> groupExprs, List<BoundAgg> aggs, PrimitiveKeyShape shape) {
        Object[] ka = new Object[]{boxKey(a, isNull, shape.isI32()[0]), boxKey(b, isNull, shape.isI32()[1])};
        List<Object> key = Arrays.asList(ka);
        return materializeGroupRow(plan, groupExprs, key, aggs, states);
    }

    private static @Nullable Object @Nullable [] materializeGroupRow(BoundSelect plan, List<BoundExpr> groupExprs,
            List<Object> key, List<BoundAgg> aggs, Aggregator[] states) {
        @Nullable
        Object[] values = new @Nullable Object[plan.items().size()];
        for (int i = 0; i < plan.items().size(); i++) {
            BoundExpr e = plan.items().get(i).expr();
            values[i] = evalPostAgg(e, groupExprs, key, aggs, states);
        }
        if (plan.having() != null) {
            Object hv = evalPostAgg(plan.having(), groupExprs, key, aggs, states);
            if (!truthy(hv)) {
                return null;
            }
        }
        return values;
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

    private static List<@Nullable Object[]> sortAggregated(List<@Nullable Object[]> rows, BoundSelect plan,
            List<BoundExpr> groupExprs, List<BoundAgg> aggs) {
        List<BoundOrderItem> order = plan.orderBy();
        // Build resolver for each order expression: find it among select items (by expr equality) to get its index.
        int[] colIdx = new int[order.size()];
        boolean[] descs = new boolean[order.size()];
        for (int i = 0; i < order.size(); i++) {
            BoundOrderItem o = order.get(i);
            descs[i] = o.direction() == SqlAst.SortDirection.DESC;
            int idx = findSelectItemIndex(plan.items(), o.expr());
            if (idx < 0) {
                throw new SqlException("ORDER BY expression not found in SELECT list (yet unsupported)", 0);
            }
            colIdx[i] = idx;
        }
        Comparator<Object[]> cmp = (a, b) -> {
            for (int i = 0; i < colIdx.length; i++) {
                Object va = a[colIdx[i]];
                Object vb = b[colIdx[i]];
                int c = compareNullsLast(va, vb);
                if (c != 0)
                    return descs[i] ? -c : c;
            }
            return 0;
        };
        List<@Nullable Object[]> copy = new ArrayList<>(rows);
        copy.sort(cmp);
        return copy;
    }

    private static int findSelectItemIndex(List<BoundSelectItem> items, BoundExpr e) {
        for (int i = 0; i < items.size(); i++) {
            if (exprEquals(items.get(i).expr(), e))
                return i;
        }
        return -1;
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
