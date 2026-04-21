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

    // occupiedSlots[0..size) lists the slot indices of non-empty entries in
    // insertion order. Iteration (forEachKey / merge) reads this directly —
    // no re-probe, no hash recompute, no scanning past empty slots.
    private int[] occupiedSlots;

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
        occupiedSlots = new int[Math.max(16, capacityHint)];
    }

    private void init(int cap) {
        capacity = cap;
        mask = cap - 1;
        keys = new long[cap * width];
        values = new Aggregator[cap][];
        size = 0;
    }

    private void recordSlot(int slot) {
        if (size == occupiedSlots.length) {
            int[] grown = new int[occupiedSlots.length * 2];
            System.arraycopy(occupiedSlots, 0, grown, 0, occupiedSlots.length);
            occupiedSlots = grown;
        }
        occupiedSlots[size] = slot;
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
            recordSlot(slot);
            size++;
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

    void forEachKey1(Visitor1 v) {
        int[] slots = occupiedSlots;
        long[] ks = keys;
        Aggregator[][] vals = values;
        int n = size;
        for (int i = 0; i < n; i++) {
            int slot = slots[i];
            v.visit(ks[slot], false, vals[slot]);
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
            recordSlot(slot);
            size++;
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

    void forEachKey2(Visitor2 v) {
        int[] slots = occupiedSlots;
        long[] ks = keys;
        Aggregator[][] vals = values;
        int n = size;
        for (int i = 0; i < n; i++) {
            int slot = slots[i];
            v.visit(ks[slot * 2], ks[slot * 2 + 1], false, vals[slot]);
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
        int[] otherSlots = other.occupiedSlots;
        Aggregator[][] otherVals = other.values;
        long[] otherKeys = other.keys;
        int n = other.size;
        if (width == 1) {
            for (int i = 0; i < n; i++) {
                int oslot = otherSlots[i];
                Aggregator[] src = otherVals[oslot];
                long k = otherKeys[oslot];
                int slot = probe1(k);
                Aggregator[] dst = values[slot];
                if (dst == null) {
                    keys[slot] = k;
                    values[slot] = src;
                    recordSlot(slot);
                    size++;
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
            for (int i = 0; i < n; i++) {
                int oslot = otherSlots[i];
                Aggregator[] src = otherVals[oslot];
                long a = otherKeys[oslot * 2];
                long b = otherKeys[oslot * 2 + 1];
                int slot = probe2(a, b);
                Aggregator[] dst = values[slot];
                if (dst == null) {
                    keys[slot * 2] = a;
                    keys[slot * 2 + 1] = b;
                    values[slot] = src;
                    recordSlot(slot);
                    size++;
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
        int[] oldSlots = occupiedSlots;
        int oldSize = size;
        init(capacity * 2);
        // Walk oldSlots in insertion order so occupiedSlots stays in the same
        // insertion order after the rehash.
        if (width == 1) {
            for (int i = 0; i < oldSize; i++) {
                int srcSlot = oldSlots[i];
                long k = oldKeys[srcSlot];
                int slot = (int) (mix(k) & mask);
                while (values[slot] != null) {
                    slot = (slot + 1) & mask;
                }
                keys[slot] = k;
                values[slot] = oldValues[srcSlot];
                oldSlots[i] = slot; // reuse the array as new occupiedSlots
            }
        } else {
            for (int i = 0; i < oldSize; i++) {
                int srcSlot = oldSlots[i];
                long a = oldKeys[srcSlot * 2];
                long b = oldKeys[srcSlot * 2 + 1];
                int slot = (int) (mix2(a, b) & mask);
                while (values[slot] != null) {
                    slot = (slot + 1) & mask;
                }
                keys[slot * 2] = a;
                keys[slot * 2 + 1] = b;
                values[slot] = oldValues[srcSlot];
                oldSlots[i] = slot;
            }
        }
        // Keep the old occupiedSlots array (it's been updated in place and may
        // still have capacity for more inserts).
        occupiedSlots = oldSlots;
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
