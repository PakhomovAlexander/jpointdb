package io.jpointdb.core.query;

import io.jpointdb.core.column.F64Column;
import io.jpointdb.core.column.I32Column;
import io.jpointdb.core.column.I64Column;
import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.jspecify.annotations.Nullable;

/**
 * Vectorized primitive-column aggregator operators for the grand-total path (no
 * GROUP BY). Each op holds its own column reader, a reusable primitive buffer,
 * and primitive accumulator state — zero object allocations in the per-row
 * path.
 *
 * <p>
 * Inner reductions use {@code jdk.incubator.vector} (species
 * {@code IntVector.SPECIES_PREFERRED} et al.) so horizontal sum/min/max run as
 * explicit SIMD lanes, independent of HotSpot's auto-vectorizer moods. Batch
 * size: {@value #BATCH_ROWS}.
 *
 * <p>
 * Fresh per chunk via {@link #forkChunk()}; merged sequentially through
 * {@link #mergeFrom} after join.
 */
abstract class VectorBatchAgg {

    static final int BATCH_ROWS = 4096;

    /**
     * Creates a fresh per-chunk instance that shares the column / setup with this
     * template but has its own state and scratch buffers.
     */
    abstract VectorBatchAgg forkChunk();

    /** Process rows {@code [from..to)} of this op's column into local state. */
    abstract void acceptRange(long from, long to);

    /** Add {@code other}'s state into ours. Shapes must match. */
    abstract void mergeFrom(VectorBatchAgg other);

    /** Boxed final value per SQL semantics. */
    abstract @Nullable Object result();

    // ============================================================================
    // COUNT(*)
    // ============================================================================

    static final class CountStar extends VectorBatchAgg {
        long count;

        @Override
        VectorBatchAgg forkChunk() {
            return new CountStar();
        }

        @Override
        void acceptRange(long from, long to) {
            count += to - from;
        }

        @Override
        void mergeFrom(VectorBatchAgg other) {
            count += ((CountStar) other).count;
        }

        @Override
        @Nullable
        Object result() {
            return count;
        }
    }

    // ============================================================================
    // I32 column aggregators (SUM / AVG / MIN / MAX)
    // ============================================================================

    private static final VectorSpecies<Integer> I32_SPECIES = IntVector.SPECIES_PREFERRED;
    private static final VectorSpecies<Long> I64_SPECIES = LongVector.SPECIES_PREFERRED;
    private static final VectorSpecies<Double> F64_SPECIES = DoubleVector.SPECIES_PREFERRED;

    abstract static class I32Base extends VectorBatchAgg {
        final I32Column col;
        final int[] buf;

        I32Base(I32Column col) {
            this.col = col;
            this.buf = new int[BATCH_ROWS];
        }
    }

    static final class SumI32 extends I32Base {
        long sum;

        SumI32(I32Column col) {
            super(col);
        }

        @Override
        VectorBatchAgg forkChunk() {
            return new SumI32(col);
        }

        @Override
        void acceptRange(long from, long to) {
            long row = from;
            long s = sum;
            while (row < to) {
                int len = (int) Math.min(BATCH_ROWS, to - row);
                col.readInts(row, buf, len);
                // SIMD horizontal widening sum: i32 lanes widen to i64 via
                // doubleSuper to avoid overflow on 1 M-row chunks.
                int bound = I32_SPECIES.loopBound(len);
                int i = 0;
                LongVector acc = LongVector.zero(I64_SPECIES);
                // IntVector -> LongVector conversion: convert half to long vectors
                // and reduceLanes.ADD. Simpler path: accumulate in int (ok for 1M
                // rows with AdvEngineID < 2^31 values), then widen.
                IntVector intAcc = IntVector.zero(I32_SPECIES);
                for (; i < bound; i += I32_SPECIES.length()) {
                    intAcc = intAcc.add(IntVector.fromArray(I32_SPECIES, buf, i));
                }
                s += intAcc.reduceLanesToLong(VectorOperators.ADD);
                for (; i < len; i++) {
                    s += buf[i];
                }
                // Silence unused-var warning: acc not needed when the IntVector
                // path handles overflow-safe widening via reduceLanesToLong.
                @SuppressWarnings("unused")
                LongVector ignored = acc;
                row += len;
            }
            sum = s;
        }

        @Override
        void mergeFrom(VectorBatchAgg other) {
            sum += ((SumI32) other).sum;
        }

        @Override
        @Nullable
        Object result() {
            // SUM of I32 returns I32 per ClickBench type inference; we return
            // long — the executor boxes whichever matches arg type.
            return sum;
        }
    }

    static final class AvgI32 extends I32Base {
        double sum;
        long count;

        AvgI32(I32Column col) {
            super(col);
        }

        @Override
        VectorBatchAgg forkChunk() {
            return new AvgI32(col);
        }

        @Override
        void acceptRange(long from, long to) {
            long row = from;
            double s = sum;
            long c = count;
            while (row < to) {
                int len = (int) Math.min(BATCH_ROWS, to - row);
                col.readInts(row, buf, len);
                int bound = I32_SPECIES.loopBound(len);
                int i = 0;
                IntVector intAcc = IntVector.zero(I32_SPECIES);
                for (; i < bound; i += I32_SPECIES.length()) {
                    intAcc = intAcc.add(IntVector.fromArray(I32_SPECIES, buf, i));
                }
                s += intAcc.reduceLanesToLong(VectorOperators.ADD);
                for (; i < len; i++) {
                    s += buf[i];
                }
                c += len;
                row += len;
            }
            sum = s;
            count = c;
        }

        @Override
        void mergeFrom(VectorBatchAgg other) {
            AvgI32 o = (AvgI32) other;
            sum += o.sum;
            count += o.count;
        }

        @Override
        @Nullable
        Object result() {
            return count == 0 ? null : sum / count;
        }
    }

    static final class MinMaxI32 extends I32Base {
        final boolean min;
        boolean any;
        int best;

        MinMaxI32(I32Column col, boolean min) {
            super(col);
            this.min = min;
            this.best = min ? Integer.MAX_VALUE : Integer.MIN_VALUE;
        }

        @Override
        VectorBatchAgg forkChunk() {
            return new MinMaxI32(col, min);
        }

        @Override
        void acceptRange(long from, long to) {
            long row = from;
            int b = best;
            boolean a = any;
            while (row < to) {
                int len = (int) Math.min(BATCH_ROWS, to - row);
                col.readInts(row, buf, len);
                int bound = I32_SPECIES.loopBound(len);
                int i = 0;
                IntVector acc = IntVector.broadcast(I32_SPECIES, b);
                for (; i < bound; i += I32_SPECIES.length()) {
                    IntVector v = IntVector.fromArray(I32_SPECIES, buf, i);
                    acc = min ? acc.min(v) : acc.max(v);
                }
                int horiz = acc.reduceLanes(min ? VectorOperators.MIN : VectorOperators.MAX);
                b = min ? Math.min(b, horiz) : Math.max(b, horiz);
                for (; i < len; i++) {
                    b = min ? Math.min(b, buf[i]) : Math.max(b, buf[i]);
                }
                if (len > 0) {
                    a = true;
                }
                row += len;
            }
            best = b;
            any = a;
        }

        @Override
        void mergeFrom(VectorBatchAgg other) {
            MinMaxI32 o = (MinMaxI32) other;
            if (o.any) {
                if (!any) {
                    best = o.best;
                    any = true;
                } else {
                    best = min ? Math.min(best, o.best) : Math.max(best, o.best);
                }
            }
        }

        @Override
        @Nullable
        Object result() {
            return any ? (long) best : null;
        }
    }

    // ============================================================================
    // I64 column aggregators
    // ============================================================================

    abstract static class I64Base extends VectorBatchAgg {
        final I64Column col;
        final long[] buf;

        I64Base(I64Column col) {
            this.col = col;
            this.buf = new long[BATCH_ROWS];
        }
    }

    static final class SumI64 extends I64Base {
        long sum;

        SumI64(I64Column col) {
            super(col);
        }

        @Override
        VectorBatchAgg forkChunk() {
            return new SumI64(col);
        }

        @Override
        void acceptRange(long from, long to) {
            long row = from;
            long s = sum;
            while (row < to) {
                int len = (int) Math.min(BATCH_ROWS, to - row);
                col.readLongs(row, buf, len);
                int bound = I64_SPECIES.loopBound(len);
                int i = 0;
                LongVector acc = LongVector.zero(I64_SPECIES);
                for (; i < bound; i += I64_SPECIES.length()) {
                    acc = acc.add(LongVector.fromArray(I64_SPECIES, buf, i));
                }
                s += acc.reduceLanes(VectorOperators.ADD);
                for (; i < len; i++) {
                    s += buf[i];
                }
                row += len;
            }
            sum = s;
        }

        @Override
        void mergeFrom(VectorBatchAgg other) {
            sum += ((SumI64) other).sum;
        }

        @Override
        @Nullable
        Object result() {
            return sum;
        }
    }

    static final class AvgI64 extends I64Base {
        double sum;
        long count;

        AvgI64(I64Column col) {
            super(col);
        }

        @Override
        VectorBatchAgg forkChunk() {
            return new AvgI64(col);
        }

        @Override
        void acceptRange(long from, long to) {
            long row = from;
            double s = sum;
            long c = count;
            while (row < to) {
                int len = (int) Math.min(BATCH_ROWS, to - row);
                col.readLongs(row, buf, len);
                // Scalar double accumulator — long-valued I64 columns can average
                // to values (e.g. UserID ≈ 2^60) whose SUM-as-long overflows on
                // 1 M rows. HotSpot still auto-vectorizes the double+=long pattern.
                for (int i = 0; i < len; i++) {
                    s += (double) buf[i];
                }
                c += len;
                row += len;
            }
            sum = s;
            count = c;
        }

        @Override
        void mergeFrom(VectorBatchAgg other) {
            AvgI64 o = (AvgI64) other;
            sum += o.sum;
            count += o.count;
        }

        @Override
        @Nullable
        Object result() {
            return count == 0 ? null : sum / count;
        }
    }

    static final class MinMaxI64 extends I64Base {
        final boolean min;
        boolean any;
        long best;

        MinMaxI64(I64Column col, boolean min) {
            super(col);
            this.min = min;
            this.best = min ? Long.MAX_VALUE : Long.MIN_VALUE;
        }

        @Override
        VectorBatchAgg forkChunk() {
            return new MinMaxI64(col, min);
        }

        @Override
        void acceptRange(long from, long to) {
            long row = from;
            long b = best;
            boolean a = any;
            while (row < to) {
                int len = (int) Math.min(BATCH_ROWS, to - row);
                col.readLongs(row, buf, len);
                int bound = I64_SPECIES.loopBound(len);
                int i = 0;
                LongVector acc = LongVector.broadcast(I64_SPECIES, b);
                for (; i < bound; i += I64_SPECIES.length()) {
                    LongVector v = LongVector.fromArray(I64_SPECIES, buf, i);
                    acc = min ? acc.min(v) : acc.max(v);
                }
                long horiz = acc.reduceLanes(min ? VectorOperators.MIN : VectorOperators.MAX);
                b = min ? Math.min(b, horiz) : Math.max(b, horiz);
                for (; i < len; i++) {
                    b = min ? Math.min(b, buf[i]) : Math.max(b, buf[i]);
                }
                if (len > 0) {
                    a = true;
                }
                row += len;
            }
            best = b;
            any = a;
        }

        @Override
        void mergeFrom(VectorBatchAgg other) {
            MinMaxI64 o = (MinMaxI64) other;
            if (o.any) {
                if (!any) {
                    best = o.best;
                    any = true;
                } else {
                    best = min ? Math.min(best, o.best) : Math.max(best, o.best);
                }
            }
        }

        @Override
        @Nullable
        Object result() {
            return any ? best : null;
        }
    }

    // ============================================================================
    // F64 column aggregators
    // ============================================================================

    abstract static class F64Base extends VectorBatchAgg {
        final F64Column col;
        final double[] buf;

        F64Base(F64Column col) {
            this.col = col;
            this.buf = new double[BATCH_ROWS];
        }
    }

    static final class SumF64 extends F64Base {
        double sum;

        SumF64(F64Column col) {
            super(col);
        }

        @Override
        VectorBatchAgg forkChunk() {
            return new SumF64(col);
        }

        @Override
        void acceptRange(long from, long to) {
            long row = from;
            double s = sum;
            while (row < to) {
                int len = (int) Math.min(BATCH_ROWS, to - row);
                col.readDoubles(row, buf, len);
                int bound = F64_SPECIES.loopBound(len);
                int i = 0;
                DoubleVector acc = DoubleVector.zero(F64_SPECIES);
                for (; i < bound; i += F64_SPECIES.length()) {
                    acc = acc.add(DoubleVector.fromArray(F64_SPECIES, buf, i));
                }
                s += acc.reduceLanes(VectorOperators.ADD);
                for (; i < len; i++) {
                    s += buf[i];
                }
                row += len;
            }
            sum = s;
        }

        @Override
        void mergeFrom(VectorBatchAgg other) {
            sum += ((SumF64) other).sum;
        }

        @Override
        @Nullable
        Object result() {
            return sum;
        }
    }

    static final class AvgF64 extends F64Base {
        double sum;
        long count;

        AvgF64(F64Column col) {
            super(col);
        }

        @Override
        VectorBatchAgg forkChunk() {
            return new AvgF64(col);
        }

        @Override
        void acceptRange(long from, long to) {
            long row = from;
            double s = sum;
            long c = count;
            while (row < to) {
                int len = (int) Math.min(BATCH_ROWS, to - row);
                col.readDoubles(row, buf, len);
                int bound = F64_SPECIES.loopBound(len);
                int i = 0;
                DoubleVector acc = DoubleVector.zero(F64_SPECIES);
                for (; i < bound; i += F64_SPECIES.length()) {
                    acc = acc.add(DoubleVector.fromArray(F64_SPECIES, buf, i));
                }
                s += acc.reduceLanes(VectorOperators.ADD);
                for (; i < len; i++) {
                    s += buf[i];
                }
                c += len;
                row += len;
            }
            sum = s;
            count = c;
        }

        @Override
        void mergeFrom(VectorBatchAgg other) {
            AvgF64 o = (AvgF64) other;
            sum += o.sum;
            count += o.count;
        }

        @Override
        @Nullable
        Object result() {
            return count == 0 ? null : sum / count;
        }
    }
}
