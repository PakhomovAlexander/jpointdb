package io.jpointdb.core.column;

/**
 * Binary layout for STRING column files (encoding: DICT or RAW).
 *
 * <pre>
 *   offset  size  field
 *   ------  ----  -----
 *   0       4     magic "JPCS"
 *   4       1     version = 1
 *   5       1     encoding (0=DICT, 1=RAW)
 *   6       1     flags (bit 0: has null bitmap)
 *   7       1     reserved
 *   8       8     rowCount
 *   16      8     section1 byte size    (DICT: IDs = rowCount*4; RAW: bytes = total UTF-8 bytes)
 *   24      8     section2 byte size    (DICT: 0;                RAW: offsets = (rowCount+1)*8)
 *   32      8     null bitmap byte size
 *   40      -     section1
 *   ...     -     section2
 *   ...     -     null bitmap
 * </pre>
 *
 * <p>
 * For DICT-encoded columns the dictionary itself lives in a separate file (see
 * {@link DictionaryBuilder} / {@link Dictionary}).
 */
public final class StringColumnFormat {

    @SuppressWarnings("MutablePublicArray") // byte-level format constant; never mutated by any caller.
    public static final byte[] MAGIC = {'J', 'P', 'C', 'S'};
    public static final byte VERSION = 1;

    public static final byte ENCODING_DICT = 0;
    public static final byte ENCODING_RAW = 1;

    public static final byte FLAG_HAS_NULLS = 1;

    public static final int HEADER_SIZE = 40;

    private StringColumnFormat() {
    }
}
