package io.jpointdb.core.query;

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

/**
 * Pull-based executor for MVP SQL: scalar SELECT, grand-total aggregates, GROUP
 * BY via hash map, ORDER BY via in-memory sort, LIMIT/OFFSET. Row-at-a-time;
 * vectorization comes later.
 */
public final class Executor {

    private Executor() {
    }

    public static QueryResult execute(BoundSelect plan, Table table) {
        ExprEvaluator ev = new ExprEvaluator(table);
        if (!plan.isAggregate() && plan.groupBy().isEmpty()) {
            return executeScalar(plan, table, ev);
        }
        return executeAggregated(plan, table, ev);
    }

    // ---------- scalar ----------

    private static QueryResult executeScalar(BoundSelect plan, Table table, ExprEvaluator ev) {
        long n = table.rowCount();
        long limit = plan.limit() == null ? Long.MAX_VALUE : plan.limit();
        long offset = plan.offset() == null ? 0 : plan.offset();
        int orderCount = plan.orderBy().size();

        List<@Nullable Object[]> values = new ArrayList<>();
        List<@Nullable Object[]> sortKeys = orderCount == 0 ? null : new ArrayList<>();
        for (long r = 0; r < n; r++) {
            if (plan.where() != null && !truthy(ev.eval(plan.where(), r)))
                continue;
            @Nullable
            Object[] v = new @Nullable Object[plan.items().size()];
            for (int i = 0; i < plan.items().size(); i++) {
                v[i] = ev.eval(plan.items().get(i).expr(), r);
            }
            values.add(v);
            if (sortKeys != null) {
                @Nullable
                Object[] k = new @Nullable Object[orderCount];
                for (int i = 0; i < orderCount; i++)
                    k[i] = ev.eval(plan.orderBy().get(i).expr(), r);
                sortKeys.add(k);
            }
        }
        if (sortKeys != null) {
            boolean[] desc = new boolean[orderCount];
            for (int i = 0; i < orderCount; i++)
                desc[i] = plan.orderBy().get(i).direction() == SqlAst.SortDirection.DESC;
            Integer[] idx = new Integer[values.size()];
            for (int i = 0; i < idx.length; i++)
                idx[i] = i;
            java.util.Arrays.sort(idx, (a, b) -> {
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

    // ---------- aggregate (GROUP BY or grand total) ----------

    private static QueryResult executeAggregated(BoundSelect plan, Table table, ExprEvaluator ev) {
        List<BoundExpr> groupExprs = plan.groupBy();
        List<BoundAgg> aggs = collectAggregates(plan);

        // Build a map: group key → aggregator states (one per agg call).
        Map<List<Object>, Aggregator[]> groups = new HashMap<>();
        List<List<Object>> groupOrder = new ArrayList<>();
        List<Object> scalarKey = List.of();
        long n = table.rowCount();
        for (long r = 0; r < n; r++) {
            if (plan.where() != null && !truthy(ev.eval(plan.where(), r)))
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
                groupOrder.add(key);
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

        // If grand-total aggregate with zero rows, still emit one row.
        if (groupExprs.isEmpty() && groupOrder.isEmpty()) {
            Aggregator[] states = new Aggregator[aggs.size()];
            for (int i = 0; i < aggs.size(); i++) {
                BoundAgg a = aggs.get(i);
                states[i] = Aggregator.create(a.fn(), a.distinct(), a.arg() == null ? null : a.arg().type(), a.type());
            }
            groups.put(scalarKey, states);
            groupOrder.add(scalarKey);
        }

        // Materialize a row per group: evaluate each select item, resolving agg calls
        // to their computed state and group exprs to their key values.
        List<@Nullable Object[]> rows = new ArrayList<>();
        for (List<Object> key : groupOrder) {
            Aggregator[] states = groups.get(key);
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
