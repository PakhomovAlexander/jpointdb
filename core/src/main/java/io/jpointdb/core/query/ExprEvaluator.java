package io.jpointdb.core.query;

import io.jpointdb.core.column.F64Column;
import io.jpointdb.core.column.I32Column;
import io.jpointdb.core.column.I64Column;
import io.jpointdb.core.column.StringColumn;
import io.jpointdb.core.schema.ColumnType;
import io.jpointdb.core.sql.BoundAst.*;
import io.jpointdb.core.sql.SqlAst;
import io.jpointdb.core.sql.SqlException;
import io.jpointdb.core.sql.ValueType;
import io.jpointdb.core.table.Table;
import org.jspecify.annotations.Nullable;

import java.util.regex.Pattern;

/**
 * Row-at-a-time evaluator for {@link BoundExpr}. Returns boxed Java values:
 * Long for I32/I64, Double for F64, String for STRING, Boolean for BOOL, null
 * for SQL NULL. Three-valued logic for AND/OR/NOT.
 *
 * <p>
 * Allocation-heavy by design — MVP correctness first, vectorize later.
 */
public final class ExprEvaluator {

    private final Table table;

    public ExprEvaluator(Table table) {
        this.table = table;
    }

    public @Nullable Object eval(BoundExpr e, long row) {
        return switch (e) {
            case BoundLiteral l -> l.value();
            case BoundColumn c -> readColumn(c.index(), row);
            case BoundUnary u -> evalUnary(u, row);
            case BoundBinary b -> evalBinary(b, row);
            case BoundIsNull n -> {
                boolean isNull = eval(n.operand(), row) == null;
                yield n.negated() ? !isNull : isNull;
            }
            case BoundLike l -> evalLike(l, row);
            case BoundInList il -> evalInList(il, row);
            case BoundCase c -> evalCase(c, row);
            case BoundScalarCall sc -> evalScalarCall(sc, arg -> eval(arg, row));
            case BoundAgg a -> throw new SqlException("aggregate encountered in row-eval path", 0);
        };
    }

    /**
     * Evaluate a scalar call given a lookup that resolves each argument. Shared
     * with post-aggregate paths.
     */
    static @Nullable Object evalScalarCall(BoundScalarCall sc,
            java.util.function.Function<BoundExpr, @Nullable Object> eval) {
        String name = sc.name();
        if (name.startsWith("extract:")) {
            Object v = eval.apply(sc.args().get(0));
            if (v == null)
                return null;
            return ScalarFns.extract(name.substring("extract:".length()), (String) v);
        }
        if (name.startsWith("date_trunc:")) {
            Object v = eval.apply(sc.args().get(0));
            if (v == null)
                return null;
            return ScalarFns.dateTrunc(name.substring("date_trunc:".length()), (String) v);
        }
        return switch (name) {
            case "strlen", "length" -> {
                Object v = eval.apply(sc.args().get(0));
                if (v == null)
                    yield null;
                String s = v instanceof String str ? str : String.valueOf(v);
                yield (long) s.codePointCount(0, s.length());
            }
            case "regexp_replace" -> {
                Object v = eval.apply(sc.args().get(0));
                Object p = eval.apply(sc.args().get(1));
                Object r = eval.apply(sc.args().get(2));
                if (v == null || p == null || r == null)
                    yield null;
                yield ScalarFns.regexpReplace((String) v, (String) p, (String) r);
            }
            default -> throw new SqlException("unsupported scalar call: " + name, 0);
        };
    }

    private @Nullable Object readColumn(int idx, long row) {
        ColumnType t = table.columnMeta(idx).type();
        return switch (t) {
            case I32 -> {
                I32Column c = table.i32(idx);
                yield c.isNullAt(row) ? null : (long) c.get(row);
            }
            case I64 -> {
                I64Column c = table.i64(idx);
                yield c.isNullAt(row) ? null : c.get(row);
            }
            case F64 -> {
                F64Column c = table.f64(idx);
                yield c.isNullAt(row) ? null : c.get(row);
            }
            case STRING -> {
                StringColumn c = table.string(idx);
                yield c.isNullAt(row) ? null : c.valueAsString(row);
            }
        };
    }

    private @Nullable Object evalUnary(BoundUnary u, long row) {
        Object v = eval(u.operand(), row);
        if (v == null)
            return null;
        return switch (u.op()) {
            case NEG -> {
                if (v instanceof Long l)
                    yield -l;
                if (v instanceof Double d)
                    yield -d;
                throw new SqlException("cannot negate " + v.getClass(), 0);
            }
            case NOT -> {
                if (v instanceof Boolean b)
                    yield !b;
                throw new SqlException("NOT requires boolean", 0);
            }
        };
    }

    private @Nullable Object evalBinary(BoundBinary b, long row) {
        if (b.op() == SqlAst.BinaryOp.AND)
            return andOp(eval(b.left(), row), eval(b.right(), row));
        if (b.op() == SqlAst.BinaryOp.OR)
            return orOp(eval(b.left(), row), eval(b.right(), row));
        Object l = eval(b.left(), row);
        Object r = eval(b.right(), row);
        if (l == null || r == null)
            return null;
        return switch (b.op()) {
            case PLUS -> numPlus(l, r);
            case MINUS -> numMinus(l, r);
            case MUL -> numMul(l, r);
            case DIV -> numDiv(l, r);
            case MOD -> numMod(l, r);
            case EQ -> compare(l, r) == 0;
            case NEQ -> compare(l, r) != 0;
            case LT -> compare(l, r) < 0;
            case LE -> compare(l, r) <= 0;
            case GT -> compare(l, r) > 0;
            case GE -> compare(l, r) >= 0;
            case AND, OR -> throw new AssertionError();
        };
    }

    private static @Nullable Object andOp(@Nullable Object a, @Nullable Object b) {
        if (Boolean.FALSE.equals(a) || Boolean.FALSE.equals(b))
            return Boolean.FALSE;
        if (a == null || b == null)
            return null;
        return ((Boolean) a) && ((Boolean) b);
    }

    private static @Nullable Object orOp(@Nullable Object a, @Nullable Object b) {
        if (Boolean.TRUE.equals(a) || Boolean.TRUE.equals(b))
            return Boolean.TRUE;
        if (a == null || b == null)
            return null;
        return ((Boolean) a) || ((Boolean) b);
    }

    private static Object numPlus(Object a, Object b) {
        if (a instanceof Double || b instanceof Double)
            return ((Number) a).doubleValue() + ((Number) b).doubleValue();
        return ((Number) a).longValue() + ((Number) b).longValue();
    }

    private static Object numMinus(Object a, Object b) {
        if (a instanceof Double || b instanceof Double)
            return ((Number) a).doubleValue() - ((Number) b).doubleValue();
        return ((Number) a).longValue() - ((Number) b).longValue();
    }

    private static Object numMul(Object a, Object b) {
        if (a instanceof Double || b instanceof Double)
            return ((Number) a).doubleValue() * ((Number) b).doubleValue();
        return ((Number) a).longValue() * ((Number) b).longValue();
    }

    private static @Nullable Object numDiv(Object a, Object b) {
        if (a instanceof Double || b instanceof Double) {
            double rb = ((Number) b).doubleValue();
            if (rb == 0.0)
                return null;
            return ((Number) a).doubleValue() / rb;
        }
        long rb = ((Number) b).longValue();
        if (rb == 0)
            return null;
        return ((Number) a).longValue() / rb;
    }

    private static @Nullable Object numMod(Object a, Object b) {
        if (a instanceof Double || b instanceof Double) {
            double rb = ((Number) b).doubleValue();
            if (rb == 0.0)
                return null;
            return ((Number) a).doubleValue() % rb;
        }
        long rb = ((Number) b).longValue();
        if (rb == 0)
            return null;
        return ((Number) a).longValue() % rb;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static int compare(Object a, Object b) {
        if (a instanceof Number na && b instanceof Number nb) {
            if (a instanceof Double || b instanceof Double) {
                return Double.compare(na.doubleValue(), nb.doubleValue());
            }
            return Long.compare(na.longValue(), nb.longValue());
        }
        if (a instanceof String sa && b instanceof String sb)
            return sa.compareTo(sb);
        if (a instanceof Boolean ba && b instanceof Boolean bb)
            return ba.compareTo(bb);
        if (a.getClass() == b.getClass() && a instanceof Comparable ca) {
            return ca.compareTo(b);
        }
        throw new SqlException("cannot compare " + a.getClass() + " vs " + b.getClass(), 0);
    }

    private @Nullable Object evalLike(BoundLike l, long row) {
        Object v = eval(l.value(), row);
        Object p = eval(l.pattern(), row);
        if (v == null || p == null)
            return null;
        if (!(v instanceof String s) || !(p instanceof String pat)) {
            throw new SqlException("LIKE requires string arguments", 0);
        }
        boolean m = Pattern.compile(likeToRegex(pat), Pattern.DOTALL).matcher(s).matches();
        return l.negated() ? !m : m;
    }

    private @Nullable Object evalInList(BoundInList il, long row) {
        Object v = eval(il.value(), row);
        if (v == null)
            return null;
        boolean seenNull = false;
        for (BoundExpr item : il.items()) {
            Object iv = eval(item, row);
            if (iv == null) {
                seenNull = true;
                continue;
            }
            if (compare(v, iv) == 0)
                return il.negated() ? Boolean.FALSE : Boolean.TRUE;
        }
        if (seenNull)
            return null;
        return il.negated();
    }

    private @Nullable Object evalCase(BoundCase c, long row) {
        for (BoundWhen w : c.whens()) {
            Object cond = eval(w.when(), row);
            if (Boolean.TRUE.equals(cond))
                return coerce(eval(w.then(), row), c.type());
        }
        return c.elseExpr() == null ? null : coerce(eval(c.elseExpr(), row), c.type());
    }

    private static @Nullable Object coerce(@Nullable Object v, ValueType target) {
        if (v == null)
            return null;
        if (target == ValueType.F64 && v instanceof Long l)
            return l.doubleValue();
        if (target == ValueType.F64 && v instanceof Integer i)
            return i.doubleValue();
        return v;
    }

    /**
     * Convert SQL LIKE pattern to Java regex. {@code %} → {@code .*}, {@code _} →
     * {@code .}
     */
    public static String likeToRegex(String pattern) {
        StringBuilder sb = new StringBuilder(pattern.length() + 8);
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            switch (c) {
                case '%' -> sb.append(".*");
                case '_' -> sb.append('.');
                case '.', '\\', '+', '*', '?', '(', ')', '[', ']', '{', '}', '^', '$', '|' -> sb.append('\\').append(c);
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }

    /** Materialize a column value as Java number for aggregators. */
    public @Nullable Number readNumeric(int colIdx, long row) {
        Object v = readColumn(colIdx, row);
        return v == null ? null : (Number) v;
    }
}
