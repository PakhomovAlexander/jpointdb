package io.jpointdb.core.query;

/**
 * Open-addressing hash map with primitive-long keys and flat primitive-array
 * aggregate state — no {@code Aggregator} objects, no per-slot object arrays.
 *
 * <p>
 * Used by the specialized inline primitive-agg path when every aggregate in a
 * query matches a narrow pattern (COUNT_STAR, SUM/AVG over a non-nullable
 * I32/I64/F64 column). Each agg uses 1-2 primitive fields stored in flat
 * {@code long[capacity]} / {@code double[capacity]} arrays indexed by slot.
 *
 * <p>
 * Null-group state is stored at slot {@code capacity} (one past the hash
 * table); {@link #hasNull()} flags presence.
 *
 * <p>
 * Iteration yields raw slot indices — callers read state via
 * {@link #longField(int)} / {@link #doubleField(int)}.
 */
@SuppressWarnings("NullAway")
final class PrimitiveAggMap {

    /** One primitive aggregator's layout inside the flat state. */
    static final class AggOp {
        enum Kind {
            COUNT_STAR, SUM_LONG, AVG_LONG, SUM_DOUBLE, AVG_DOUBLE
        }

        final Kind kind;
        final int longField;
        final int doubleField;

        AggOp(Kind kind, int longField, int doubleField) {
            this.kind = kind;
            this.longField = longField;
            this.doubleField = doubleField;
        }
    }

    private static final double LOAD = 0.7;

    final int width;
    final AggOp[] ops;
    private final int longFieldCount;
    private final int doubleFieldCount;

    private long[] keys;
    private boolean[] occupied;
    private int[] occupiedSlots;
    private int capacity;
    private int mask;
    private int size;

    // Package-visible: scan loop hoists the reference once per rehash.
    long[][] longFields;
    double[][] doubleFields;

    private boolean hasNull;

    PrimitiveAggMap(int width, int capacityHint, AggOp[] ops, int longFieldCount, int doubleFieldCount) {
        if (width != 1 && width != 2) {
            throw new IllegalArgumentException("width must be 1 or 2");
        }
        this.width = width;
        this.ops = ops;
        this.longFieldCount = longFieldCount;
        this.doubleFieldCount = doubleFieldCount;
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
        occupied = new boolean[cap + 1]; // slot 'capacity' reserved for null group
        longFields = new long[longFieldCount][cap + 1];
        doubleFields = new double[doubleFieldCount][cap + 1];
        size = 0;
    }

    int size() {
        return size + (hasNull ? 1 : 0);
    }

    int capacity() {
        return capacity;
    }

    int nullSlot() {
        return capacity;
    }

    long[] longField(int idx) {
        return longFields[idx];
    }

    double[] doubleField(int idx) {
        return doubleFields[idx];
    }

    long keyAt(int slot, int component) {
        return keys[slot * width + component];
    }

    int[] occupiedSlots() {
        return occupiedSlots;
    }

    int nonNullSize() {
        return size;
    }

    boolean hasNull() {
        return hasNull;
    }

    private void recordSlot(int slot) {
        if (size == occupiedSlots.length) {
            int[] grown = new int[occupiedSlots.length * 2];
            System.arraycopy(occupiedSlots, 0, grown, 0, occupiedSlots.length);
            occupiedSlots = grown;
        }
        occupiedSlots[size] = slot;
    }

    // ---------- width=1 ----------

    int getOrCreateSlot1(long k) {
        int slot = probe1(k);
        if (!occupied[slot]) {
            keys[slot] = k;
            occupied[slot] = true;
            recordSlot(slot);
            size++;
            if (size >= (int) (capacity * LOAD)) {
                rehash();
                slot = probe1(k);
            }
        }
        return slot;
    }

    private int probe1(long k) {
        int slot = (int) (mix(k) & mask);
        while (occupied[slot] && keys[slot] != k) {
            slot = (slot + 1) & mask;
        }
        return slot;
    }

    // ---------- width=2 ----------

    int getOrCreateSlot2(long a, long b) {
        int slot = probe2(a, b);
        if (!occupied[slot]) {
            keys[slot * 2] = a;
            keys[slot * 2 + 1] = b;
            occupied[slot] = true;
            recordSlot(slot);
            size++;
            if (size >= (int) (capacity * LOAD)) {
                rehash();
                slot = probe2(a, b);
            }
        }
        return slot;
    }

    private int probe2(long a, long b) {
        int slot = (int) (mix2(a, b) & mask);
        while (occupied[slot] && (keys[slot * 2] != a || keys[slot * 2 + 1] != b)) {
            slot = (slot + 1) & mask;
        }
        return slot;
    }

    // ---------- null group ----------

    int getOrCreateNullSlot() {
        hasNull = true;
        return nullSlot();
    }

    // ---------- merge ----------

    void ensureCapacity(int targetEntries) {
        int target = (int) Math.min((long) Integer.MAX_VALUE, Math.max(16L, (long) (targetEntries / LOAD) + 1));
        if (target <= capacity) {
            return;
        }
        int cap = capacity;
        while (cap < target) {
            cap <<= 1;
        }
        rehashTo(cap);
    }

    void merge(PrimitiveAggMap other) {
        if (other.width != width || other.ops.length != ops.length) {
            throw new IllegalStateException("merge shape mismatch");
        }
        if (other.hasNull) {
            hasNull = true;
            foldSlot(nullSlot(), other, other.nullSlot());
        }
        int[] otherSlots = other.occupiedSlots;
        int n = other.size;
        if (width == 1) {
            for (int i = 0; i < n; i++) {
                int oslot = otherSlots[i];
                long k = other.keys[oslot];
                int slot = probe1(k);
                if (!occupied[slot]) {
                    keys[slot] = k;
                    occupied[slot] = true;
                    recordSlot(slot);
                    size++;
                    if (size >= (int) (capacity * LOAD)) {
                        rehash();
                        slot = probe1(k);
                    }
                }
                foldSlot(slot, other, oslot);
            }
        } else {
            for (int i = 0; i < n; i++) {
                int oslot = otherSlots[i];
                long a = other.keys[oslot * 2];
                long b = other.keys[oslot * 2 + 1];
                int slot = probe2(a, b);
                if (!occupied[slot]) {
                    keys[slot * 2] = a;
                    keys[slot * 2 + 1] = b;
                    occupied[slot] = true;
                    recordSlot(slot);
                    size++;
                    if (size >= (int) (capacity * LOAD)) {
                        rehash();
                        slot = probe2(a, b);
                    }
                }
                foldSlot(slot, other, oslot);
            }
        }
    }

    private void foldSlot(int dstSlot, PrimitiveAggMap src, int srcSlot) {
        for (AggOp op : ops) {
            switch (op.kind) {
                case COUNT_STAR -> longFields[op.longField][dstSlot] += src.longFields[op.longField][srcSlot];
                case SUM_LONG -> longFields[op.longField][dstSlot] += src.longFields[op.longField][srcSlot];
                case SUM_DOUBLE -> doubleFields[op.doubleField][dstSlot] += src.doubleFields[op.doubleField][srcSlot];
                case AVG_LONG, AVG_DOUBLE -> {
                    doubleFields[op.doubleField][dstSlot] += src.doubleFields[op.doubleField][srcSlot];
                    longFields[op.longField][dstSlot] += src.longFields[op.longField][srcSlot];
                }
            }
        }
    }

    // ---------- rehash ----------

    private void rehash() {
        rehashTo(capacity * 2);
    }

    private void rehashTo(int newCap) {
        long[] oldKeys = keys;
        int[] oldSlots = occupiedSlots;
        long[][] oldLong = longFields;
        double[][] oldDouble = doubleFields;
        int oldSize = size;
        boolean oldHasNull = hasNull;
        int oldCap = capacity;
        init(newCap);
        hasNull = oldHasNull;
        if (oldHasNull) {
            int oldNullSlot = oldCap;
            for (int i = 0; i < longFieldCount; i++) {
                longFields[i][nullSlot()] = oldLong[i][oldNullSlot];
            }
            for (int i = 0; i < doubleFieldCount; i++) {
                doubleFields[i][nullSlot()] = oldDouble[i][oldNullSlot];
            }
        }
        if (width == 1) {
            for (int i = 0; i < oldSize; i++) {
                int srcSlot = oldSlots[i];
                long k = oldKeys[srcSlot];
                int slot = (int) (mix(k) & mask);
                while (occupied[slot]) {
                    slot = (slot + 1) & mask;
                }
                keys[slot] = k;
                occupied[slot] = true;
                for (int f = 0; f < longFieldCount; f++) {
                    longFields[f][slot] = oldLong[f][srcSlot];
                }
                for (int f = 0; f < doubleFieldCount; f++) {
                    doubleFields[f][slot] = oldDouble[f][srcSlot];
                }
                oldSlots[i] = slot;
            }
        } else {
            for (int i = 0; i < oldSize; i++) {
                int srcSlot = oldSlots[i];
                long a = oldKeys[srcSlot * 2];
                long b = oldKeys[srcSlot * 2 + 1];
                int slot = (int) (mix2(a, b) & mask);
                while (occupied[slot]) {
                    slot = (slot + 1) & mask;
                }
                keys[slot * 2] = a;
                keys[slot * 2 + 1] = b;
                occupied[slot] = true;
                for (int f = 0; f < longFieldCount; f++) {
                    longFields[f][slot] = oldLong[f][srcSlot];
                }
                for (int f = 0; f < doubleFieldCount; f++) {
                    doubleFields[f][slot] = oldDouble[f][srcSlot];
                }
                oldSlots[i] = slot;
            }
        }
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
