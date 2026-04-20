package io.jpointdb.core.query;

/**
 * Open-addressing hash map keyed by one or two {@code long}s, values are
 * per-group {@link Aggregator} arrays. Used by the executor when GROUP BY is a
 * single integer column (width=1) or two integer columns (width=2) — primitive
 * keys sidestep the {@code HashMap<List<Object>, ...>} boxing / equals path.
 *
 * <p>
 * Linear probing, load factor 0.7, power-of-two capacity. NULL keys are tracked
 * out-of-band ({@link #nullStates}) and appended last during
 * {@link #forEachKey1}/{@link #forEachKey2}.
 */
@SuppressWarnings("NullAway")
final class LongKeysAggMap {

    @FunctionalInterface
    interface AggFactory {
        Aggregator[] create();
    }

    @FunctionalInterface
    interface Visitor1 {
        void visit(long k, boolean isNull, Aggregator[] states);
    }

    @FunctionalInterface
    interface Visitor2 {
        void visit(long k0, long k1, boolean isNull, Aggregator[] states);
    }

    private static final double LOAD = 0.7;

    final int width;

    private long[] keys;
    private Aggregator[][] values;
    private int capacity;
    private int mask;
    private int size;

    private long[] orderKeys;
    private int orderSize;

    private boolean hasNull;
    private Aggregator[] nullStates;

    LongKeysAggMap(int width) {
        this(width, 64);
    }

    LongKeysAggMap(int width, int capacityHint) {
        if (width != 1 && width != 2) {
            throw new IllegalArgumentException("width must be 1 or 2");
        }
        this.width = width;
        int cap = 16;
        int target = (int) Math.min((long) Integer.MAX_VALUE, Math.max(16L, (long) (capacityHint / LOAD) + 1));
        while (cap < target) {
            cap <<= 1;
        }
        init(cap);
        orderKeys = new long[Math.max(16 * width, capacityHint * width)];
    }

    private void init(int cap) {
        capacity = cap;
        mask = cap - 1;
        keys = new long[cap * width];
        values = new Aggregator[cap][];
        size = 0;
    }

    int size() {
        return size + (hasNull ? 1 : 0);
    }

    // ---------- width=1 ----------

    Aggregator[] getOrCreate1(long k, AggFactory f) {
        int slot = probe1(k);
        Aggregator[] v = values[slot];
        if (v == null) {
            keys[slot] = k;
            v = f.create();
            values[slot] = v;
            size++;
            appendOrder1(k);
            if (size >= (int) (capacity * LOAD)) {
                rehash();
            }
        }
        return v;
    }

    private int probe1(long k) {
        int slot = (int) (mix(k) & mask);
        while (values[slot] != null && keys[slot] != k) {
            slot = (slot + 1) & mask;
        }
        return slot;
    }

    private void appendOrder1(long k) {
        if (orderSize == orderKeys.length) {
            long[] grown = new long[orderKeys.length * 2];
            System.arraycopy(orderKeys, 0, grown, 0, orderKeys.length);
            orderKeys = grown;
        }
        orderKeys[orderSize++] = k;
    }

    void forEachKey1(Visitor1 v) {
        for (int i = 0; i < orderSize; i++) {
            long k = orderKeys[i];
            v.visit(k, false, values[probe1(k)]);
        }
        if (hasNull) {
            v.visit(0L, true, nullStates);
        }
    }

    // ---------- width=2 ----------

    Aggregator[] getOrCreate2(long a, long b, AggFactory f) {
        int slot = probe2(a, b);
        Aggregator[] v = values[slot];
        if (v == null) {
            keys[slot * 2] = a;
            keys[slot * 2 + 1] = b;
            v = f.create();
            values[slot] = v;
            size++;
            appendOrder2(a, b);
            if (size >= (int) (capacity * LOAD)) {
                rehash();
            }
        }
        return v;
    }

    private int probe2(long a, long b) {
        int slot = (int) (mix2(a, b) & mask);
        while (values[slot] != null && (keys[slot * 2] != a || keys[slot * 2 + 1] != b)) {
            slot = (slot + 1) & mask;
        }
        return slot;
    }

    private void appendOrder2(long a, long b) {
        if (orderSize * 2 >= orderKeys.length) {
            long[] grown = new long[orderKeys.length * 2];
            System.arraycopy(orderKeys, 0, grown, 0, orderKeys.length);
            orderKeys = grown;
        }
        orderKeys[orderSize * 2] = a;
        orderKeys[orderSize * 2 + 1] = b;
        orderSize++;
    }

    void forEachKey2(Visitor2 v) {
        for (int i = 0; i < orderSize; i++) {
            long a = orderKeys[i * 2];
            long b = orderKeys[i * 2 + 1];
            v.visit(a, b, false, values[probe2(a, b)]);
        }
        if (hasNull) {
            v.visit(0L, 0L, true, nullStates);
        }
    }

    // ---------- null group ----------

    Aggregator[] getOrCreateNull(AggFactory f) {
        if (!hasNull) {
            hasNull = true;
            nullStates = f.create();
        }
        return nullStates;
    }

    // ---------- merge ----------

    void merge(LongKeysAggMap other, int aggCount) {
        if (other.width != width) {
            throw new IllegalStateException("merge width mismatch");
        }
        if (other.hasNull) {
            if (!hasNull) {
                hasNull = true;
                nullStates = other.nullStates;
            } else {
                for (int j = 0; j < aggCount; j++) {
                    nullStates[j].merge(other.nullStates[j]);
                }
            }
        }
        if (width == 1) {
            for (int i = 0; i < other.orderSize; i++) {
                long k = other.orderKeys[i];
                Aggregator[] src = other.values[other.probe1(k)];
                int slot = probe1(k);
                Aggregator[] dst = values[slot];
                if (dst == null) {
                    keys[slot] = k;
                    values[slot] = src;
                    size++;
                    appendOrder1(k);
                    if (size >= (int) (capacity * LOAD)) {
                        rehash();
                    }
                } else {
                    for (int j = 0; j < aggCount; j++) {
                        dst[j].merge(src[j]);
                    }
                }
            }
        } else {
            for (int i = 0; i < other.orderSize; i++) {
                long a = other.orderKeys[i * 2];
                long b = other.orderKeys[i * 2 + 1];
                Aggregator[] src = other.values[other.probe2(a, b)];
                int slot = probe2(a, b);
                Aggregator[] dst = values[slot];
                if (dst == null) {
                    keys[slot * 2] = a;
                    keys[slot * 2 + 1] = b;
                    values[slot] = src;
                    size++;
                    appendOrder2(a, b);
                    if (size >= (int) (capacity * LOAD)) {
                        rehash();
                    }
                } else {
                    for (int j = 0; j < aggCount; j++) {
                        dst[j].merge(src[j]);
                    }
                }
            }
        }
    }

    // ---------- rehash ----------

    private void rehash() {
        long[] oldKeys = keys;
        Aggregator[][] oldValues = values;
        int oldCap = capacity;
        int oldSize = size;
        init(capacity * 2);
        if (width == 1) {
            for (int i = 0; i < oldCap; i++) {
                Aggregator[] v = oldValues[i];
                if (v == null) {
                    continue;
                }
                long k = oldKeys[i];
                int slot = (int) (mix(k) & mask);
                while (values[slot] != null) {
                    slot = (slot + 1) & mask;
                }
                keys[slot] = k;
                values[slot] = v;
            }
        } else {
            for (int i = 0; i < oldCap; i++) {
                Aggregator[] v = oldValues[i];
                if (v == null) {
                    continue;
                }
                long a = oldKeys[i * 2];
                long b = oldKeys[i * 2 + 1];
                int slot = (int) (mix2(a, b) & mask);
                while (values[slot] != null) {
                    slot = (slot + 1) & mask;
                }
                keys[slot * 2] = a;
                keys[slot * 2 + 1] = b;
                values[slot] = v;
            }
        }
        size = oldSize;
    }

    // ---------- hashes ----------

    private static long mix(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }

    private static long mix2(long a, long b) {
        long h = a * 0x9E3779B97F4A7C15L + b;
        return mix(h);
    }
}
