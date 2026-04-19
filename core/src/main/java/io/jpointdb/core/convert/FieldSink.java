package io.jpointdb.core.convert;

import io.jpointdb.core.column.*;
import io.jpointdb.core.schema.Encoding;
import io.jpointdb.core.table.ColumnMeta;
import io.jpointdb.core.tsv.TsvScanner;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import org.jspecify.annotations.Nullable;

/**
 * Polymorphic writer adapter: takes a {@link TsvScanner} field and appends it
 * to the appropriate typed column writer. Uses shared parsing helpers for
 * numerics.
 */
abstract class FieldSink implements AutoCloseable {

    static FieldSink open(Path file, ColumnMeta meta, @Nullable DictionaryBuilder dict) throws IOException {
        return switch (meta.type()) {
            case I32 -> new I32Sink(new I32ColumnWriter(file, meta.nullable()));
            case I64 -> new I64Sink(new I64ColumnWriter(file, meta.nullable()));
            case F64 -> new F64Sink(new F64ColumnWriter(file, meta.nullable()));
            case STRING -> {
                if (meta.encoding() == Encoding.DICT) {
                    if (dict == null)
                        throw new IllegalStateException("dict required for DICT encoding");
                    yield new DictStringSink(StringColumnWriter.createDict(file, meta.nullable(), dict));
                }
                yield new RawStringSink(StringColumnWriter.createRaw(file, meta.nullable()));
            }
        };
    }

    abstract void accept(TsvScanner scanner, int fieldIndex) throws IOException;

    abstract void acceptMissing() throws IOException;

    @Override
    public abstract void close() throws IOException;

    static final ThreadLocal<long[]> LONG_BUF = ThreadLocal.withInitial(() -> new long[1]);

    static boolean parseLong(MemorySegment data, long off, int len, long[] out) {
        if (len == 0)
            return false;
        int i = 0;
        byte first = data.get(ValueLayout.JAVA_BYTE, off);
        boolean neg = false;
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
        if (neg)
            out[0] = acc;
        else {
            if (acc == Long.MIN_VALUE)
                return false;
            out[0] = -acc;
        }
        return true;
    }

    static double parseDouble(MemorySegment data, long off, int len) {
        byte[] buf = new byte[len];
        MemorySegment.copy(data, ValueLayout.JAVA_BYTE, off, buf, 0, len);
        return Double.parseDouble(new String(buf, java.nio.charset.StandardCharsets.US_ASCII));
    }

    static final class I32Sink extends FieldSink {
        private final I32ColumnWriter writer;
        I32Sink(I32ColumnWriter w) {
            this.writer = w;
        }

        @Override
        void accept(TsvScanner sc, int i) throws IOException {
            if (sc.fieldIsNull(i) || sc.fieldLength(i) == 0) {
                writer.appendNull();
                return;
            }
            long[] out = LONG_BUF.get();
            if (!parseLong(sc.data(), sc.fieldOffset(i), sc.fieldLength(i), out)) {
                writer.appendNull();
                return;
            }
            long v = out[0];
            if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
                writer.appendNull();
                return;
            }
            writer.appendNonNull((int) v);
        }

        @Override
        void acceptMissing() throws IOException {
            writer.appendNull();
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }
    }

    static final class I64Sink extends FieldSink {
        private final I64ColumnWriter writer;
        I64Sink(I64ColumnWriter w) {
            this.writer = w;
        }

        @Override
        void accept(TsvScanner sc, int i) throws IOException {
            if (sc.fieldIsNull(i) || sc.fieldLength(i) == 0) {
                writer.appendNull();
                return;
            }
            long[] out = LONG_BUF.get();
            if (!parseLong(sc.data(), sc.fieldOffset(i), sc.fieldLength(i), out)) {
                writer.appendNull();
                return;
            }
            writer.appendNonNull(out[0]);
        }

        @Override
        void acceptMissing() throws IOException {
            writer.appendNull();
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }
    }

    static final class F64Sink extends FieldSink {
        private final F64ColumnWriter writer;
        F64Sink(F64ColumnWriter w) {
            this.writer = w;
        }

        @Override
        void accept(TsvScanner sc, int i) throws IOException {
            if (sc.fieldIsNull(i) || sc.fieldLength(i) == 0) {
                writer.appendNull();
                return;
            }
            try {
                writer.appendNonNull(parseDouble(sc.data(), sc.fieldOffset(i), sc.fieldLength(i)));
            } catch (NumberFormatException e) {
                writer.appendNull();
            }
        }

        @Override
        void acceptMissing() throws IOException {
            writer.appendNull();
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }
    }

    static final class DictStringSink extends FieldSink {
        private final StringColumnWriter writer;
        DictStringSink(StringColumnWriter w) {
            this.writer = w;
        }

        @Override
        void accept(TsvScanner sc, int i) throws IOException {
            if (sc.fieldIsNull(i)) {
                writer.appendNull();
                return;
            }
            writer.appendNonNull(sc.data(), sc.fieldOffset(i), sc.fieldLength(i));
        }

        @Override
        void acceptMissing() throws IOException {
            writer.appendNull();
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }
    }

    static final class RawStringSink extends FieldSink {
        private final StringColumnWriter writer;
        RawStringSink(StringColumnWriter w) {
            this.writer = w;
        }

        @Override
        void accept(TsvScanner sc, int i) throws IOException {
            if (sc.fieldIsNull(i)) {
                writer.appendNull();
                return;
            }
            writer.appendNonNull(sc.data(), sc.fieldOffset(i), sc.fieldLength(i));
        }

        @Override
        void acceptMissing() throws IOException {
            writer.appendNull();
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }
    }
}
