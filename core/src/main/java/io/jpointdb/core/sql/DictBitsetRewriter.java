package io.jpointdb.core.sql;

import io.jpointdb.core.column.Dictionary;
import io.jpointdb.core.column.StringColumn;
import io.jpointdb.core.column.StringColumnWriter;
import io.jpointdb.core.schema.ColumnType;
import io.jpointdb.core.sql.BoundAst.*;
import io.jpointdb.core.sql.SqlAst.BinaryOp;
import io.jpointdb.core.table.Table;
import org.jspecify.annotations.Nullable;

/**
 * Post-bind rewrite that turns comparisons of a DICT-encoded STRING column
 * against a STRING literal into a {@link BoundDictBitsetMatch} — a precomputed
 * {@code boolean[dictSize]} lookup. Saves per-row String materialization and
 * {@code String.compareTo} on ClickBench's date-range filters (Q37/38/41/42/43)
 * and equality filters on small-cardinality STRING columns.
 *
 * <p>Only rewrites leaves; AND/OR composition is left to the executor so this
 * remains a local per-comparison optimization.
 *
 * <p>Gated by {@link #MAX_DICT_SIZE} so we don't build a 1 M-entry bitset for
 * high-cardinality columns like URL where the per-query walk would cost more
 * than the per-row savings.
 */
public final class DictBitsetRewriter {

    /**
     * Walking a 1 M-entry dict once at bind time costs ~1-3 ms but saves per-row
     * String materialization/comparison over ~1 M scan rows — break-even ratio
     * is >100× and bitset memory stays under 2 MB. Skip only pathological dicts.
     */
    private static final int MAX_DICT_SIZE = 2_000_000;

    private DictBitsetRewriter() {
    }

    public static BoundExpr rewrite(BoundExpr e, Table table) {
        return switch (e) {
            case BoundBinary b -> rewriteBinary(b, table);
            case BoundUnary u -> {
                BoundExpr inner = rewrite(u.operand(), table);
                yield inner == u.operand() ? u : new BoundUnary(u.op(), inner, u.type());
            }
            case BoundIsNull n -> {
                BoundExpr inner = rewrite(n.operand(), table);
                yield inner == n.operand() ? n : new BoundIsNull(inner, n.negated());
            }
            default -> e;
        };
    }

    private static BoundExpr rewriteBinary(BoundBinary b, Table table) {
        // Recurse into AND/OR operands first.
        if (b.op() == BinaryOp.AND || b.op() == BinaryOp.OR) {
            BoundExpr l = rewrite(b.left(), table);
            BoundExpr r = rewrite(b.right(), table);
            if (l == b.left() && r == b.right()) {
                return b;
            }
            return new BoundBinary(b.op(), l, r, b.type());
        }
        // Only leaf comparisons are candidates.
        BoundColumn col;
        BoundLiteral lit;
        BinaryOp op = b.op();
        if (b.left() instanceof BoundColumn c && b.right() instanceof BoundLiteral l) {
            col = c;
            lit = l;
        } else if (b.left() instanceof BoundLiteral l && b.right() instanceof BoundColumn c) {
            // Swap to canonical 'col OP lit' form; flip operator.
            col = c;
            lit = l;
            op = flipOp(op);
            if (op == null) {
                return b;
            }
        } else {
            return b;
        }
        if (col.type() != ValueType.STRING || lit.type() != ValueType.STRING) {
            return b;
        }
        if (!(lit.value() instanceof String litValue)) {
            return b;
        }
        ColumnType ct = table.columnMeta(col.index()).type();
        if (ct != ColumnType.STRING) {
            return b;
        }
        StringColumn sc = table.string(col.index());
        if (sc.mode() != StringColumnWriter.Mode.DICT) {
            return b;
        }
        Dictionary dict = sc.dictionary();
        if (dict == null || dict.size() > MAX_DICT_SIZE) {
            return b;
        }
        boolean[] bitset = buildBitset(dict, op, litValue);
        if (bitset == null) {
            return b;
        }
        return new BoundDictBitsetMatch(col.index(), col.name(), bitset, false);
    }

    private static @Nullable BinaryOp flipOp(BinaryOp op) {
        return switch (op) {
            case EQ -> BinaryOp.EQ;
            case NEQ -> BinaryOp.NEQ;
            case LT -> BinaryOp.GT;
            case LE -> BinaryOp.GE;
            case GT -> BinaryOp.LT;
            case GE -> BinaryOp.LE;
            default -> null;
        };
    }

    private static boolean @Nullable [] buildBitset(Dictionary dict, BinaryOp op, String lit) {
        int n = dict.size();
        boolean[] out = new boolean[n];
        switch (op) {
            case EQ -> {
                for (int i = 0; i < n; i++) {
                    out[i] = dict.stringAt(i).equals(lit);
                }
            }
            case NEQ -> {
                for (int i = 0; i < n; i++) {
                    out[i] = !dict.stringAt(i).equals(lit);
                }
            }
            case LT -> {
                for (int i = 0; i < n; i++) {
                    out[i] = dict.stringAt(i).compareTo(lit) < 0;
                }
            }
            case LE -> {
                for (int i = 0; i < n; i++) {
                    out[i] = dict.stringAt(i).compareTo(lit) <= 0;
                }
            }
            case GT -> {
                for (int i = 0; i < n; i++) {
                    out[i] = dict.stringAt(i).compareTo(lit) > 0;
                }
            }
            case GE -> {
                for (int i = 0; i < n; i++) {
                    out[i] = dict.stringAt(i).compareTo(lit) >= 0;
                }
            }
            default -> {
                return null;
            }
        }
        return out;
    }
}
