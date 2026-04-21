package io.jpointdb.core.query;

import io.jpointdb.core.sql.BoundAst.AggregateFn;
import io.jpointdb.core.sql.ValueType;

import java.util.HashSet;

import org.jspecify.annotations.Nullable;

/**
 * Mutable accumulator for a single aggregate call. One instance per group.
 * {@link #merge} combines another accumulator of the same shape — used by the
 * parallel executor to fold per-chunk states back into one result.
 *
 * <p>
 * {@link #acceptLong} / {@link #acceptDouble} are primitive-specialized fast
 * paths: when the agg's argument is a plain I32/I64/F64 column we skip boxing
 * in the scan loop and call straight in.
 */
abstract class Aggregator {

    abstract void accept(@Nullable Object value);

    /** Primitive fast-path for numeric aggs. Default boxes and delegates. */
    void acceptLong(long value, boolean isNull) {
        accept(isNull ? null : value);
    }

    /** Primitive fast-path for F64 aggs. Default boxes and delegates. */
    void acceptDouble(double value, boolean isNull) {
        accept(isNull ? null : value);
    }

    abstract void merge(Aggregator other);

    abstract @Nullable Object result();

    static Aggregator create(AggregateFn fn, boolean distinct, @Nullable ValueType argType, ValueType resultType) {
        Aggregator base = switch (fn) {
            case COUNT_STAR -> new CountStar();
            case COUNT -> new Count();
            case SUM -> resultType == ValueType.F64 ? new SumDouble() : new SumLong();
            case AVG -> new Avg();
            case MIN -> new MinMax(true);
            case MAX -> new MinMax(false);
        };
        if (distinct && fn != AggregateFn.COUNT_STAR) {
            return new Distinct(base);
        }
        return base;
    }

    static final class CountStar extends Aggregator {
        long count;
        @Override
        void accept(@Nullable Object value) {
            count++;
        }
        @Override
        void acceptLong(long value, boolean isNull) {
            count++;
        }
        @Override
        void acceptDouble(double value, boolean isNull) {
            count++;
        }
        @Override
        void merge(Aggregator other) {
            count += ((CountStar) other).count;
        }
        @Override
        @Nullable
        Object result() {
            return count;
        }
    }

    static final class Count extends Aggregator {
        long count;
        @Override
        void accept(@Nullable Object value) {
            if (value != null)
                count++;
        }
        @Override
        void acceptLong(long value, boolean isNull) {
            if (!isNull)
                count++;
        }
        @Override
        void acceptDouble(double value, boolean isNull) {
            if (!isNull)
                count++;
        }
        @Override
        void merge(Aggregator other) {
            count += ((Count) other).count;
        }
        @Override
        @Nullable
        Object result() {
            return count;
        }
    }

    static final class SumLong extends Aggregator {
        long sum;
        boolean any;
        @Override
        void accept(@Nullable Object value) {
            if (value == null)
                return;
            any = true;
            sum += ((Number) value).longValue();
        }
        @Override
        void acceptLong(long value, boolean isNull) {
            if (isNull)
                return;
            any = true;
            sum += value;
        }
        @Override
        void merge(Aggregator other) {
            SumLong o = (SumLong) other;
            sum += o.sum;
            any |= o.any;
        }
        @Override
        @Nullable
        Object result() {
            return any ? sum : null;
        }
    }

    static final class SumDouble extends Aggregator {
        double sum;
        boolean any;
        @Override
        void accept(@Nullable Object value) {
            if (value == null)
                return;
            any = true;
            sum += ((Number) value).doubleValue();
        }
        @Override
        void acceptLong(long value, boolean isNull) {
            if (isNull)
                return;
            any = true;
            sum += value;
        }
        @Override
        void acceptDouble(double value, boolean isNull) {
            if (isNull)
                return;
            any = true;
            sum += value;
        }
        @Override
        void merge(Aggregator other) {
            SumDouble o = (SumDouble) other;
            sum += o.sum;
            any |= o.any;
        }
        @Override
        @Nullable
        Object result() {
            return any ? sum : null;
        }
    }

    static final class Avg extends Aggregator {
        double sum;
        long count;
        @Override
        void accept(@Nullable Object value) {
            if (value == null)
                return;
            count++;
            sum += ((Number) value).doubleValue();
        }
        @Override
        void acceptLong(long value, boolean isNull) {
            if (isNull)
                return;
            count++;
            sum += value;
        }
        @Override
        void acceptDouble(double value, boolean isNull) {
            if (isNull)
                return;
            count++;
            sum += value;
        }
        @Override
        void merge(Aggregator other) {
            Avg o = (Avg) other;
            sum += o.sum;
            count += o.count;
        }
        @Override
        @Nullable
        Object result() {
            return count == 0 ? null : sum / count;
        }
    }

    static final class MinMax extends Aggregator {
        final boolean min;
        @Nullable
        Object best;
        MinMax(boolean min) {
            this.min = min;
        }
        @Override
        void accept(@Nullable Object value) {
            if (value == null)
                return;
            if (best == null) {
                best = value;
                return;
            }
            int cmp = ExprEvaluator.compare(value, best);
            if ((min && cmp < 0) || (!min && cmp > 0))
                best = value;
        }
        @Override
        void acceptLong(long value, boolean isNull) {
            if (isNull)
                return;
            if (best == null) {
                best = value;
                return;
            }
            long cur = ((Long) best);
            if ((min && value < cur) || (!min && value > cur))
                best = value;
        }
        @Override
        void acceptDouble(double value, boolean isNull) {
            if (isNull)
                return;
            if (best == null) {
                best = value;
                return;
            }
            double cur = ((Double) best);
            if ((min && value < cur) || (!min && value > cur))
                best = value;
        }
        @Override
        void merge(Aggregator other) {
            MinMax o = (MinMax) other;
            if (o.best != null)
                accept(o.best);
        }
        @Override
        @Nullable
        Object result() {
            return best;
        }
    }

    static final class Distinct extends Aggregator {
        final HashSet<Object> seen = new HashSet<>();
        final Aggregator inner;
        Distinct(Aggregator inner) {
            this.inner = inner;
        }
        @Override
        void accept(@Nullable Object value) {
            if (value == null)
                return;
            if (seen.add(value))
                inner.accept(value);
        }
        // acceptLong/acceptDouble fall back to the boxed path — Distinct needs
        // the Object to hash, so there's no win from specializing here.
        @Override
        void merge(Aggregator other) {
            Distinct o = (Distinct) other;
            for (Object v : o.seen) {
                if (seen.add(v))
                    inner.accept(v);
            }
        }
        @Override
        @Nullable
        Object result() {
            return inner.result();
        }
    }
}
