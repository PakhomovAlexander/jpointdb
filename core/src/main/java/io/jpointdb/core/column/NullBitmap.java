package io.jpointdb.core.column;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;

/**
 * Growable 1-bit-per-row validity bitmap for a nullable column. Bit convention:
 * 1 = value is present, 0 = value is NULL (matches Arrow). Bits are packed
 * little-endian within each byte: row {@code i} lives in {@code byte i/8} at
 * bit position {@code i%8}.
 */
public final class NullBitmap {

    private byte[] bits;
    private long rowCount;

    public NullBitmap() {
        this.bits = new byte[16];
    }

    public void appendValid() {
        ensureCapacity(rowCount + 1);
        int byteIdx = (int) (rowCount >>> 3);
        int bitIdx = (int) (rowCount & 7);
        bits[byteIdx] |= (byte) (1 << bitIdx);
        rowCount++;
    }

    public void appendNull() {
        ensureCapacity(rowCount + 1);
        rowCount++;
    }

    public long rowCount() {
        return rowCount;
    }

    /**
     * Returns the packed bitmap bytes for persistence, trimmed to
     * {@code ceil(rowCount/8)}.
     */
    public byte[] toByteArray() {
        int len = byteLength();
        if (len == bits.length)
            return bits;
        return Arrays.copyOf(bits, len);
    }

    public int byteLength() {
        return (int) ((rowCount + 7) >>> 3);
    }

    private void ensureCapacity(long newRowCount) {
        int needed = (int) ((newRowCount + 7) >>> 3);
        if (needed > bits.length) {
            int newLen = Math.max(bits.length * 2, needed);
            bits = Arrays.copyOf(bits, newLen);
        }
    }

    public static boolean isValid(MemorySegment bitmap, long rowIndex) {
        long byteIdx = rowIndex >>> 3;
        int bitIdx = (int) (rowIndex & 7);
        byte b = bitmap.get(ValueLayout.JAVA_BYTE, byteIdx);
        return (b & (1 << bitIdx)) != 0;
    }
}
