package io.jpointdb.core.schema;

import io.jpointdb.core.tsv.TsvScanner;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * Decides per-column types from observed TSV values. Widening order: I32 → I64
 * → F64 → STRING. A column with no non-null observations is STRING. NULL and
 * empty values are both skipped for type inference; their presence marks the
 * column as nullable.
 */
public final class SchemaSniffer {

    private SchemaSniffer() {
    }

    public static Schema sniff(TsvScanner scanner, @Nullable List<String> columnNames) {
        List<Accumulator> accs = new ArrayList<>();
        while (scanner.nextRecord()) {
            int n = scanner.fieldCount();
            while (accs.size() < n)
                accs.add(new Accumulator());
            for (int i = 0; i < n; i++) {
                Accumulator a = accs.get(i);
                if (scanner.fieldIsNull(i) || scanner.fieldLength(i) == 0) {
                    a.nullable = true;
                } else {
                    a.observe(scanner.data(), scanner.fieldOffset(i), scanner.fieldLength(i));
                }
            }
        }
        List<Schema.Column> columns = new ArrayList<>(accs.size());
        for (int i = 0; i < accs.size(); i++) {
            String name = (columnNames != null && i < columnNames.size()) ? columnNames.get(i) : "col" + i;
            columns.add(new Schema.Column(name, accs.get(i).resolve(), accs.get(i).nullable));
        }
        return new Schema(columns);
    }

    public static Schema sniff(TsvScanner scanner) {
        return sniff(scanner, null);
    }

    private static final class Accumulator {
        boolean nullable = false;
        int nonNullCount = 0;
        boolean allI32 = true;
        boolean allI64 = true;
        boolean allF64 = true;

        void observe(MemorySegment data, long off, int len) {
            nonNullCount++;
            boolean intShape = isIntPattern(data, off, len);
            long[] holder = LONG_HOLDER.get();
            boolean fitsLong = intShape && parseLong(data, off, len, holder);
            boolean fitsInt = fitsLong && holder[0] >= Integer.MIN_VALUE && holder[0] <= Integer.MAX_VALUE;
            boolean oversizedInt = intShape && !fitsLong;

            if (!fitsInt)
                allI32 = false;
            if (!fitsLong)
                allI64 = false;
            if (oversizedInt || (!fitsLong && !isF64Pattern(data, off, len)))
                allF64 = false;
        }

        ColumnType resolve() {
            if (nonNullCount == 0)
                return ColumnType.STRING;
            if (allI32)
                return ColumnType.I32;
            if (allI64)
                return ColumnType.I64;
            if (allF64)
                return ColumnType.F64;
            return ColumnType.STRING;
        }
    }

    private static final ThreadLocal<long[]> LONG_HOLDER = ThreadLocal.withInitial(() -> new long[1]);

    static boolean isIntPattern(MemorySegment data, long off, int len) {
        if (len == 0)
            return false;
        int i = 0;
        byte first = data.get(ValueLayout.JAVA_BYTE, off);
        if (first == '-' || first == '+') {
            i = 1;
            if (i == len)
                return false;
        }
        for (; i < len; i++) {
            byte b = data.get(ValueLayout.JAVA_BYTE, off + i);
            if (b < '0' || b > '9')
                return false;
        }
        return true;
    }

    static boolean parseLong(MemorySegment data, long off, int len, long[] out) {
        if (len == 0)
            return false;
        boolean neg = false;
        int i = 0;
        byte first = data.get(ValueLayout.JAVA_BYTE, off);
        if (first == '-') {
            neg = true;
            i = 1;
        } else if (first == '+') {
            i = 1;
        }
        if (i == len)
            return false;
        long acc = 0;
        long limit = Long.MIN_VALUE / 10;
        for (; i < len; i++) {
            byte b = data.get(ValueLayout.JAVA_BYTE, off + i);
            if (b < '0' || b > '9')
                return false;
            int digit = b - '0';
            if (acc < limit)
                return false;
            acc *= 10;
            if (acc < Long.MIN_VALUE + digit)
                return false;
            acc -= digit;
        }
        if (neg) {
            out[0] = acc;
        } else {
            if (acc == Long.MIN_VALUE)
                return false;
            out[0] = -acc;
        }
        return true;
    }

    static boolean isF64Pattern(MemorySegment data, long off, int len) {
        if (len == 0)
            return false;
        int i = 0;
        byte b = data.get(ValueLayout.JAVA_BYTE, off + i);
        if (b == '+' || b == '-') {
            i++;
            if (i == len)
                return false;
        }
        boolean sawDigit = false;
        while (i < len) {
            b = data.get(ValueLayout.JAVA_BYTE, off + i);
            if (b < '0' || b > '9')
                break;
            sawDigit = true;
            i++;
        }
        if (i < len && data.get(ValueLayout.JAVA_BYTE, off + i) == '.') {
            i++;
            while (i < len) {
                b = data.get(ValueLayout.JAVA_BYTE, off + i);
                if (b < '0' || b > '9')
                    break;
                sawDigit = true;
                i++;
            }
        }
        if (!sawDigit)
            return false;
        if (i < len) {
            b = data.get(ValueLayout.JAVA_BYTE, off + i);
            if (b == 'e' || b == 'E') {
                i++;
                if (i == len)
                    return false;
                b = data.get(ValueLayout.JAVA_BYTE, off + i);
                if (b == '+' || b == '-') {
                    i++;
                    if (i == len)
                        return false;
                }
                boolean sawExpDigit = false;
                while (i < len) {
                    b = data.get(ValueLayout.JAVA_BYTE, off + i);
                    if (b < '0' || b > '9')
                        break;
                    sawExpDigit = true;
                    i++;
                }
                if (!sawExpDigit)
                    return false;
            }
        }
        return i == len;
    }
}
