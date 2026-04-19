package io.jpointdb.core.query;

import io.jpointdb.core.sql.BoundAst.AggregateFn;
import io.jpointdb.core.sql.ValueType;

import java.util.HashSet;

import org.jspecify.annotations.Nullable;

/**
 * Mutable accumulator for a single aggregate call. One instance per group.
 */
abstract class Aggregator {

    abstract void accept(@Nullable Object value);

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
        @Override
        @Nullable
        Object result() {
            return inner.result();
        }
    }
}
