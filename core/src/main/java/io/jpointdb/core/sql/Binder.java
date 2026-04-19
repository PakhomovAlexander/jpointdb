package io.jpointdb.core.sql;

import io.jpointdb.core.sql.BoundAst.*;
import io.jpointdb.core.sql.SqlAst.*;
import io.jpointdb.core.table.ColumnMeta;
import io.jpointdb.core.table.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.jspecify.annotations.Nullable;

/**
 * Resolves a parsed {@link SqlAst.Select} against a {@link Table}: maps column
 * names to indices, infers expression types, flags aggregates.
 */
public final class Binder {

    private final Table table;
    @Nullable
    private Map<String, BoundExpr> aliasScope;

    private Binder(Table table) {
        this.table = table;
    }

    public static BoundSelect bind(SqlAst.Select sel, Table table) {
        if (!sel.fromTable().equalsIgnoreCase(table.meta().columns().isEmpty() ? "" : tableName(table))) {
            // For MVP we have one logical table per Table object.
        }
        Binder b = new Binder(table);
        return b.bindSelect(sel);
    }

    private static String tableName(Table table) {
        String dn = table.dir().getFileName().toString();
        if (dn.endsWith(".jpdb"))
            dn = dn.substring(0, dn.length() - 5);
        return dn;
    }

    private BoundSelect bindSelect(Select sel) {
        List<BoundSelectItem> items = new ArrayList<>();
        for (SelectItem item : sel.items()) {
            if (item.expr() instanceof Star) {
                for (int i = 0; i < table.columnCount(); i++) {
                    ColumnMeta m = table.columnMeta(i);
                    BoundExpr e = new BoundColumn(i, m.name(), ValueType.fromColumn(m.type()));
                    items.add(new BoundSelectItem(e, m.name()));
                }
                continue;
            }
            BoundExpr e = bindExpr(item.expr());
            String name = item.alias() != null ? item.alias() : e.defaultName();
            items.add(new BoundSelectItem(e, name));
        }
        // WHERE cannot reference aliases (standard SQL scope).
        aliasScope = null;
        BoundExpr where = sel.where() == null ? null : ensureBool(bindExpr(sel.where()));
        // GROUP BY / HAVING / ORDER BY may reference SELECT-list aliases.
        aliasScope = new HashMap<>();
        for (BoundSelectItem it : items) {
            if (it.outputName() != null)
                aliasScope.putIfAbsent(it.outputName(), it.expr());
        }
        List<BoundExpr> groupBy = new ArrayList<>();
        for (Expr g : sel.groupBy())
            groupBy.add(bindExpr(g));
        BoundExpr having = sel.having() == null ? null : ensureBool(bindExpr(sel.having()));
        List<BoundOrderItem> orderBy = new ArrayList<>();
        for (OrderByItem o : sel.orderBy()) {
            orderBy.add(new BoundOrderItem(bindExpr(o.expr()), o.direction()));
        }
        boolean isAgg = items.stream().anyMatch(i -> containsAggregate(i.expr()))
                || (having != null && containsAggregate(having))
                || orderBy.stream().anyMatch(o -> containsAggregate(o.expr()));
        return new BoundSelect(items, where, groupBy, having, orderBy, sel.limit(), sel.offset(), isAgg);
    }

    private BoundExpr bindExpr(Expr e) {
        return switch (e) {
            case Literal l -> bindLiteral(l);
            case ColumnRef c -> bindColumnRef(c);
            case Unary u -> bindUnary(u);
            case Binary b -> bindBinary(b);
            case FunctionCall f -> bindFunctionCall(f);
            case SqlAst.IsNull n -> new BoundIsNull(bindExpr(n.value()), n.negated());
            case SqlAst.Like l -> new BoundLike(bindExpr(l.value()), bindExpr(l.pattern()), l.negated());
            case SqlAst.InList il -> {
                BoundExpr v = bindExpr(il.value());
                List<BoundExpr> items = new ArrayList<>();
                for (Expr x : il.items())
                    items.add(bindExpr(x));
                yield new BoundInList(v, items, il.negated());
            }
            case SqlAst.CaseExpr c -> bindCase(c);
            case Star s -> throw new SqlException("unexpected '*'", 0);
        };
    }

    private BoundExpr bindLiteral(Literal l) {
        ValueType t = switch (l.kind()) {
            case INT -> ValueType.I64;
            case FLOAT -> ValueType.F64;
            case STRING -> ValueType.STRING;
            case BOOL -> ValueType.BOOL;
            case NULL -> ValueType.I64;
        };
        return new BoundLiteral(l.value(), t);
    }

    private BoundExpr bindColumnRef(ColumnRef c) {
        if (aliasScope != null) {
            BoundExpr aliased = aliasScope.get(c.name());
            if (aliased != null)
                return aliased;
        }
        int idx = findColumn(c.name());
        ColumnMeta meta = table.columnMeta(idx);
        return new BoundColumn(idx, meta.name(), ValueType.fromColumn(meta.type()));
    }

    private int findColumn(String name) {
        for (int i = 0; i < table.columnCount(); i++) {
            if (table.columnMeta(i).name().equals(name))
                return i;
        }
        for (int i = 0; i < table.columnCount(); i++) {
            if (table.columnMeta(i).name().equalsIgnoreCase(name))
                return i;
        }
        throw new SqlException("unknown column: " + name, 0);
    }

    private BoundExpr bindUnary(Unary u) {
        BoundExpr operand = bindExpr(u.operand());
        ValueType t = switch (u.op()) {
            case NEG -> {
                if (!operand.type().isNumeric()) {
                    throw new SqlException("unary - requires numeric operand, got " + operand.type(), 0);
                }
                yield operand.type();
            }
            case NOT -> ValueType.BOOL;
        };
        return new BoundUnary(u.op(), operand, t);
    }

    private BoundExpr bindBinary(Binary b) {
        BoundExpr l = bindExpr(b.left());
        BoundExpr r = bindExpr(b.right());
        ValueType t = switch (b.op()) {
            case PLUS, MINUS, MUL, DIV, MOD -> {
                if (!l.type().isNumeric() || !r.type().isNumeric()) {
                    throw new SqlException("arithmetic requires numeric operands", 0);
                }
                yield ValueType.widerNumeric(l.type(), r.type());
            }
            case EQ, NEQ, LT, LE, GT, GE -> ValueType.BOOL;
            case AND, OR -> ValueType.BOOL;
        };
        return new BoundBinary(b.op(), l, r, t);
    }

    private BoundExpr bindCase(SqlAst.CaseExpr c) {
        List<BoundWhen> whens = new ArrayList<>();
        if (c.whens().isEmpty()) {
            throw new SqlException("CASE requires at least one WHEN", 0);
        }
        ValueType resultType = bindExpr(c.whens().get(0).then()).type();
        for (SqlAst.WhenClause w : c.whens()) {
            BoundExpr cond = ensureBool(bindExpr(w.when()));
            BoundExpr then = bindExpr(w.then());
            whens.add(new BoundWhen(cond, then));
            resultType = combineTypes(resultType, then.type());
        }
        BoundExpr elseExpr = null;
        if (c.elseExpr() != null) {
            elseExpr = bindExpr(c.elseExpr());
            resultType = combineTypes(resultType, elseExpr.type());
        }
        return new BoundCase(whens, elseExpr, resultType);
    }

    private ValueType combineTypes(ValueType a, ValueType b) {
        if (a == b)
            return a;
        if (a.isNumeric() && b.isNumeric())
            return ValueType.widerNumeric(a, b);
        // Mixed types fall back to STRING (caller is asking for trouble but we accept).
        return ValueType.STRING;
    }

    private BoundExpr bindFunctionCall(FunctionCall f) {
        String n = f.name().toLowerCase(Locale.ROOT);
        switch (n) {
            case "count" -> {
                if (f.args().size() == 1 && f.args().get(0) instanceof Star) {
                    if (f.distinct())
                        throw new SqlException("COUNT(DISTINCT *) is invalid", 0);
                    return new BoundAgg(AggregateFn.COUNT_STAR, null, false, ValueType.I64);
                }
                if (f.args().size() != 1)
                    throw new SqlException("COUNT takes exactly one argument", 0);
                BoundExpr arg = bindExpr(f.args().get(0));
                return new BoundAgg(AggregateFn.COUNT, arg, f.distinct(), ValueType.I64);
            }
            case "sum" -> {
                return newSingleArgAgg(f, AggregateFn.SUM,
                        (@Nullable BoundExpr agg) -> agg != null && agg.type().isNumeric() ? agg.type() : null);
            }
            case "avg" -> {
                return newSingleArgAgg(f, AggregateFn.AVG,
                        (@Nullable BoundExpr agg) -> agg != null && agg.type().isNumeric() ? ValueType.F64 : null);
            }
            case "min", "max" -> {
                AggregateFn fn = n.equals("min") ? AggregateFn.MIN : AggregateFn.MAX;
                return newSingleArgAgg(f, fn, (@Nullable BoundExpr agg) -> agg == null ? null : agg.type());
            }
            // ---- scalar functions ----
            case "strlen", "length" -> {
                if (f.distinct())
                    throw new SqlException(n + " does not accept DISTINCT", 0);
                if (f.args().size() != 1 || f.args().get(0) instanceof Star) {
                    throw new SqlException(n + " takes one non-* argument", 0);
                }
                BoundExpr arg = bindExpr(f.args().get(0));
                if (arg.type() != ValueType.STRING) {
                    throw new SqlException(n + " requires STRING argument, got " + arg.type(), 0);
                }
                return new BoundScalarCall(n, List.of(arg), ValueType.I64);
            }
            case "extract" -> {
                if (f.distinct())
                    throw new SqlException("EXTRACT does not accept DISTINCT", 0);
                if (f.args().size() != 2)
                    throw new SqlException("EXTRACT expects field and expression", 0);
                BoundExpr fieldBound = bindExpr(f.args().get(0));
                if (!(fieldBound instanceof BoundLiteral fl) || !(fl.value() instanceof String)) {
                    throw new SqlException("EXTRACT field must be a name (year/month/day/hour/minute/second)", 0);
                }
                BoundExpr tsArg = bindExpr(f.args().get(1));
                if (tsArg.type() != ValueType.STRING) {
                    throw new SqlException("EXTRACT expects STRING datetime (we don't have a native DATETIME type)", 0);
                }
                String field = ((String) fl.value()).toLowerCase(Locale.ROOT);
                return new BoundScalarCall("extract:" + field, List.of(tsArg), ValueType.I64);
            }
            case "regexp_replace" -> {
                if (f.distinct())
                    throw new SqlException("REGEXP_REPLACE does not accept DISTINCT", 0);
                if (f.args().size() != 3)
                    throw new SqlException("REGEXP_REPLACE takes (value, pattern, replacement)", 0);
                List<BoundExpr> args = new ArrayList<>(3);
                for (Expr a : f.args())
                    args.add(bindExpr(a));
                for (int i = 0; i < 3; i++) {
                    if (args.get(i).type() != ValueType.STRING) {
                        throw new SqlException("REGEXP_REPLACE arg " + (i + 1) + " must be STRING", 0);
                    }
                }
                return new BoundScalarCall("regexp_replace", args, ValueType.STRING);
            }
            case "date_trunc" -> {
                if (f.distinct())
                    throw new SqlException("DATE_TRUNC does not accept DISTINCT", 0);
                if (f.args().size() != 2)
                    throw new SqlException("DATE_TRUNC takes (precision, datetime)", 0);
                BoundExpr precBound = bindExpr(f.args().get(0));
                if (!(precBound instanceof BoundLiteral pl) || !(pl.value() instanceof String)) {
                    throw new SqlException("DATE_TRUNC precision must be a string literal", 0);
                }
                BoundExpr tsArg = bindExpr(f.args().get(1));
                if (tsArg.type() != ValueType.STRING) {
                    throw new SqlException("DATE_TRUNC expects STRING datetime", 0);
                }
                String precision = ((String) pl.value()).toLowerCase(Locale.ROOT);
                return new BoundScalarCall("date_trunc:" + precision, List.of(tsArg), ValueType.STRING);
            }
            default -> throw new SqlException("unsupported function: " + f.name(), 0);
        }
    }

    private BoundExpr newSingleArgAgg(FunctionCall f, AggregateFn fn,
            java.util.function.Function<@Nullable BoundExpr, @Nullable ValueType> typer) {
        if (f.args().size() != 1 || f.args().get(0) instanceof Star) {
            throw new SqlException(fn + " takes one non-* argument", 0);
        }
        BoundExpr arg = bindExpr(f.args().get(0));
        ValueType rt = typer.apply(arg);
        if (rt == null)
            throw new SqlException(fn + " requires compatible argument type, got " + arg.type(), 0);
        return new BoundAgg(fn, arg, f.distinct(), rt);
    }

    private BoundExpr ensureBool(BoundExpr e) {
        return e;
    }

    private static boolean containsAggregate(BoundExpr e) {
        return switch (e) {
            case BoundAgg a -> true;
            case BoundUnary u -> containsAggregate(u.operand());
            case BoundBinary b -> containsAggregate(b.left()) || containsAggregate(b.right());
            case BoundIsNull n -> containsAggregate(n.operand());
            case BoundLike l -> containsAggregate(l.value()) || containsAggregate(l.pattern());
            case BoundInList il -> {
                if (containsAggregate(il.value()))
                    yield true;
                for (BoundExpr i : il.items())
                    if (containsAggregate(i))
                        yield true;
                yield false;
            }
            case BoundCase c -> {
                for (BoundWhen w : c.whens()) {
                    if (containsAggregate(w.when()) || containsAggregate(w.then()))
                        yield true;
                }
                yield c.elseExpr() != null && containsAggregate(c.elseExpr());
            }
            case BoundScalarCall sc -> {
                for (BoundExpr a : sc.args())
                    if (containsAggregate(a))
                        yield true;
                yield false;
            }
            case BoundLiteral ignored -> false;
            case BoundColumn ignored -> false;
        };
    }
}
