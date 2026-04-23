package io.jpointdb.core.query;

/**
 * Open-addressing primitive long set. Used by {@link Aggregator.CountDistinctLong}
 * to track distinct values without the {@code HashSet<Long>} boxing overhead.
 * Not thread-safe; each parallel chunk builds its own and they merge sequentially.
 */
final class LongHashSet {

    private static final double LOAD = 0.7;

    private long[] keys;
    private boolean[] occupied;
    private int capacity;
    private int mask;
    private int size;
    private boolean hasZero;

    LongHashSet() {
        this(16);
    }

    LongHashSet(int capacityHint) {
        int cap = 16;
        int target = (int) Math.min((long) Integer.MAX_VALUE, Math.max(16L, (long) (capacityHint / LOAD) + 1));
        while (cap < target) {
            cap <<= 1;
        }
        init(cap);
    }

    private void init(int cap) {
        capacity = cap;
        mask = cap - 1;
        keys = new long[cap];
        occupied = new boolean[cap];
        size = 0;
    }

    /**
     * Adds {@code k} if not already present. Returns {@code true} if it was
     * inserted. {@code 0L} is handled out-of-band — occupancy mark for a plain
     * open-addressing table collides with the value 0 otherwise.
     */
    boolean add(long k) {
        if (k == 0L) {
            if (hasZero) {
                return false;
            }
            hasZero = true;
            size++;
            return true;
        }
        int slot = (int) (mix(k) & mask);
        while (occupied[slot]) {
            if (keys[slot] == k) {
                return false;
            }
            slot = (slot + 1) & mask;
        }
        keys[slot] = k;
        occupied[slot] = true;
        size++;
        if (size >= (int) (capacity * LOAD)) {
            rehash();
        }
        return true;
    }

    int size() {
        return size;
    }

    void merge(LongHashSet other) {
        if (other.hasZero && !hasZero) {
            hasZero = true;
            size++;
        }
        long[] ok = other.keys;
        boolean[] oo = other.occupied;
        for (int i = 0; i < ok.length; i++) {
            if (oo[i]) {
                add(ok[i]);
            }
        }
    }

    private void rehash() {
        long[] oldKeys = keys;
        boolean[] oldOccupied = occupied;
        init(capacity * 2);
        // hasZero / size for the zero slot stay as-is; reinsert the rest.
        boolean preservedZero = hasZero;
        int preservedSize = hasZero ? 1 : 0;
        for (int i = 0; i < oldKeys.length; i++) {
            if (oldOccupied[i]) {
                long k = oldKeys[i];
                int slot = (int) (mix(k) & mask);
                while (occupied[slot]) {
                    slot = (slot + 1) & mask;
                }
                keys[slot] = k;
                occupied[slot] = true;
                preservedSize++;
            }
        }
        size = preservedSize;
        hasZero = preservedZero;
    }

    private static long mix(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }
}
