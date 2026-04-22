package io.jpointdb.core.sql;

import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * Resolved SQL AST: column references are mapped to indices, types are known,
 * aggregate calls are identified.
 */
public final class BoundAst {

    private BoundAst() {
    }

    public enum AggregateFn {
        COUNT_STAR, COUNT, SUM, AVG, MIN, MAX
    }

    public sealed interface BoundExpr permits BoundLiteral, BoundColumn, BoundUnary, BoundBinary, BoundIsNull,
            BoundLike, BoundInList, BoundCase, BoundAgg, BoundScalarCall, BoundDictBitsetMatch {
        ValueType type();
        String defaultName();
    }

    public record BoundLiteral(@Nullable Object value, ValueType type) implements BoundExpr {
        @Override
        public String defaultName() {
            if (value == null)
                return "NULL";
            return String.valueOf(value);
        }
    }

    public record BoundColumn(int index, String name, ValueType type) implements BoundExpr {
        @Override
        public String defaultName() {
            return name;
        }
    }

    public record BoundUnary(SqlAst.UnaryOp op, BoundExpr operand, ValueType type) implements BoundExpr {
        @Override
        public String defaultName() {
            return "(" + op.name().toLowerCase(java.util.Locale.ROOT) + " " + operand.defaultName() + ")";
        }
    }

    public record BoundBinary(SqlAst.BinaryOp op, BoundExpr left, BoundExpr right,
            ValueType type) implements BoundExpr {
        @Override
        public String defaultName() {
            return "(" + left.defaultName() + " " + op.name() + " " + right.defaultName() + ")";
        }
    }

    public record BoundIsNull(BoundExpr operand, boolean negated) implements BoundExpr {
        @Override
        public ValueType type() {
            return ValueType.BOOL;
        }
        @Override
        public String defaultName() {
            return operand.defaultName() + (negated ? " IS NOT NULL" : " IS NULL");
        }
    }

    /**
     * {@code matcher} is non-null when {@code pattern} was a literal at bind time —
     * that's every real SQL query — so the executor skips the per-row cache lookup.
     */
    public record BoundLike(BoundExpr value, BoundExpr pattern, boolean negated,
            @Nullable LikeMatcher matcher) implements BoundExpr {
        @Override
        public ValueType type() {
            return ValueType.BOOL;
        }
        @Override
        public String defaultName() {
            return value.defaultName() + (negated ? " NOT LIKE " : " LIKE ") + pattern.defaultName();
        }
    }

    public record BoundInList(BoundExpr value, List<BoundExpr> items, boolean negated) implements BoundExpr {
        public BoundInList {
            items = List.copyOf(items);
        }
        @Override
        public ValueType type() {
            return ValueType.BOOL;
        }
        @Override
        public String defaultName() {
            return value.defaultName() + (negated ? " NOT IN (...)" : " IN (...)");
        }
    }

    public record BoundWhen(BoundExpr when, BoundExpr then) {
    }

    public record BoundCase(List<BoundWhen> whens, @Nullable BoundExpr elseExpr, ValueType type) implements BoundExpr {
        public BoundCase {
            whens = List.copyOf(whens);
        }
        @Override
        public String defaultName() {
            return "CASE ... END";
        }
    }

    /** A non-aggregate (scalar) function call, e.g. {@code strlen(URL)}. */
    /**
     * Precomputed predicate on a DICT-encoded STRING column: {@code bitset[id]}
     * answers whether each dict id satisfies the original string comparison(s).
     * Built at bind time from patterns like {@code col = 'x'}, {@code col <> 'x'},
     * {@code col >= 'lo' AND col <= 'hi'}. The hot path reads one int (dict id) and
     * one boolean[] lookup per row — no String allocation, no compareTo.
     *
     * <p>
     * {@code negated} toggles the stored bitset (used for {@code NOT} / {@code <>}
     * without rewriting the array).
     */
    @SuppressWarnings("ArrayRecordComponent")
    public record BoundDictBitsetMatch(int columnIndex, String columnName, boolean[] bitset,
            boolean negated) implements BoundExpr {
        @Override
        public ValueType type() {
            return ValueType.BOOL;
        }

        @Override
        public String defaultName() {
            return columnName + (negated ? " NOT IN (...)" : " IN (...)");
        }
    }

    public record BoundScalarCall(String name, List<BoundExpr> args, ValueType type) implements BoundExpr {
        public BoundScalarCall {
            args = List.copyOf(args);
        }
        @Override
        public String defaultName() {
            StringBuilder sb = new StringBuilder(name).append('(');
            for (int i = 0; i < args.size(); i++) {
                if (i > 0)
                    sb.append(", ");
                sb.append(args.get(i).defaultName());
            }
            return sb.append(')').toString();
        }
    }

    public record BoundAgg(AggregateFn fn, @Nullable BoundExpr arg, boolean distinct,
            ValueType type) implements BoundExpr {
        @Override
        public String defaultName() {
            return switch (fn) {
                case COUNT_STAR -> "count_star()";
                case COUNT -> "count(" + (distinct ? "DISTINCT " : "") + argName() + ")";
                case SUM -> "sum(" + argName() + ")";
                case AVG -> "avg(" + argName() + ")";
                case MIN -> "min(" + argName() + ")";
                case MAX -> "max(" + argName() + ")";
            };
        }
        private String argName() {
            return arg == null ? "" : arg.defaultName();
        }
    }

    public record BoundSelectItem(BoundExpr expr, String outputName) {
    }

    public record BoundOrderItem(BoundExpr expr, SqlAst.SortDirection direction) {
    }

    public record BoundSelect(List<BoundSelectItem> items, @Nullable BoundExpr where, List<BoundExpr> groupBy,
            @Nullable BoundExpr having, List<BoundOrderItem> orderBy, @Nullable Long limit, @Nullable Long offset,
            boolean isAggregate) {
        public BoundSelect {
            items = List.copyOf(items);
            groupBy = groupBy == null ? List.of() : List.copyOf(groupBy);
            orderBy = orderBy == null ? List.of() : List.copyOf(orderBy);
        }
    }
}
