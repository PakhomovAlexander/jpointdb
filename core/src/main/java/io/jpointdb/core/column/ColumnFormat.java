package io.jpointdb.core.column;

/**
 * Binary column file format.
 *
 * <pre>
 *   offset  size  field
 *   ------  ----  -----
 *   0       4     magic "JPCO"
 *   4       1     version = 1
 *   5       1     type code (I32=0, I64=1, F64=2, BOOL=3)
 *   6       1     flags (bit 0: has null bitmap)
 *   7       1     reserved
 *   8       8     row count (i64, native byte order)
 *   16      8     data byte size (i64)
 *   24      8     null bitmap byte size (i64)
 *   32      -     data (rowCount * typeSize)
 *   ...     -     null bitmap (nullBitmapByteSize bytes, if flag set)
 * </pre>
 *
 * <p>
 * Byte order is the platform native order. Values are naturally aligned so the
 * mmapped region can be read with aligned
 * {@link java.lang.foreign.ValueLayout}s.
 */
public final class ColumnFormat {

    @SuppressWarnings("MutablePublicArray") // byte-level format constant; never mutated by any caller.
    public static final byte[] MAGIC = {'J', 'P', 'C', 'O'};
    public static final byte VERSION = 1;

    public static final byte TYPE_I32 = 0;
    public static final byte TYPE_I64 = 1;
    public static final byte TYPE_F64 = 2;
    public static final byte TYPE_BOOL = 3;

    public static final byte FLAG_HAS_NULLS = 1;

    public static final int HEADER_SIZE = 32;

    private ColumnFormat() {
    }
}
