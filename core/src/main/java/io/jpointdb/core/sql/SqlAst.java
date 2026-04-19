package io.jpointdb.core.sql;

import java.util.List;

import org.jspecify.annotations.Nullable;

/**
 * SQL AST: sealed expression hierarchy + SELECT-statement record.
 */
public final class SqlAst {

    private SqlAst() {
    }

    public enum BinaryOp {
        PLUS, MINUS, MUL, DIV, MOD, EQ, NEQ, LT, LE, GT, GE, AND, OR
    }

    public enum UnaryOp {
        NEG, NOT
    }

    public enum SortDirection {
        ASC, DESC
    }

    public enum LiteralKind {
        INT, FLOAT, STRING, BOOL, NULL
    }

    public sealed interface Expr
            permits Literal, ColumnRef, Unary, Binary, FunctionCall, CaseExpr, InList, Like, IsNull, Star {
    }

    public record Literal(LiteralKind kind, @Nullable Object value) implements Expr {
        public static Literal ofInt(long v) {
            return new Literal(LiteralKind.INT, v);
        }
        public static Literal ofFloat(double v) {
            return new Literal(LiteralKind.FLOAT, v);
        }
        public static Literal ofString(String v) {
            return new Literal(LiteralKind.STRING, v);
        }
        public static Literal ofBool(boolean v) {
            return new Literal(LiteralKind.BOOL, v);
        }
        public static Literal ofNull() {
            return new Literal(LiteralKind.NULL, null);
        }
    }

    public record ColumnRef(String name) implements Expr {
    }

    public record Unary(UnaryOp op, Expr operand) implements Expr {
    }

    public record Binary(BinaryOp op, Expr left, Expr right) implements Expr {
    }

    public record FunctionCall(String name, List<Expr> args, boolean distinct) implements Expr {
        public FunctionCall {
            args = List.copyOf(args);
        }
    }

    public record WhenClause(Expr when, Expr then) {
    }

    public record CaseExpr(List<WhenClause> whens, @Nullable Expr elseExpr) implements Expr {
        public CaseExpr {
            whens = List.copyOf(whens);
        }
    }

    public record InList(Expr value, List<Expr> items, boolean negated) implements Expr {
        public InList {
            items = List.copyOf(items);
        }
    }

    public record Like(Expr value, Expr pattern, boolean negated) implements Expr {
    }

    public record IsNull(Expr value, boolean negated) implements Expr {
    }

    /**
     * Wildcard, e.g. in {@code SELECT *} or {@code COUNT(*)}.
     */
    public record Star() implements Expr {
        public static final Star INSTANCE = new Star();
    }

    public record SelectItem(Expr expr, @Nullable String alias) {
    }

    public record OrderByItem(Expr expr, SortDirection direction) {
    }

    public record Select(List<SelectItem> items, String fromTable, @Nullable Expr where, List<Expr> groupBy,
            @Nullable Expr having, List<OrderByItem> orderBy, @Nullable Long limit, @Nullable Long offset) {
        public Select {
            items = List.copyOf(items);
            groupBy = groupBy == null ? List.of() : List.copyOf(groupBy);
            orderBy = orderBy == null ? List.of() : List.copyOf(orderBy);
        }
    }
}
