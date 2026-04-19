package io.jpointdb.core.tsv;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Scans a TSV byte stream, record by record. Backed by a MemorySegment so
 * inputs can be heap arrays, off-heap buffers, or mmapped files without copy.
 * Fields are exposed as (offset, length) pairs into the original segment.
 *
 * <p>
 * Null semantics: a field equal to the two bytes {@code \N} is NULL. An empty
 * field between tabs is an empty value, not NULL.
 *
 * <p>
 * Line endings: both LF and CRLF are recognized; a trailing CR on the last
 * field of a record is stripped.
 */
public final class TsvScanner {

    private static final byte TAB = (byte) '\t';
    private static final byte LF = (byte) '\n';
    private static final byte CR = (byte) '\r';
    private static final byte BACKSLASH = (byte) '\\';
    private static final byte UPPER_N = (byte) 'N';

    private final MemorySegment data;
    private final long size;
    private long pos;

    private long[] fieldOffsets = new long[16];
    private int[] fieldLengths = new int[16];
    private int fieldCount;

    public TsvScanner(MemorySegment data) {
        this.data = data;
        this.size = data.byteSize();
    }

    public boolean nextRecord() {
        if (pos >= size) {
            return false;
        }
        fieldCount = 0;
        long fieldStart = pos;
        while (pos < size) {
            byte b = data.get(ValueLayout.JAVA_BYTE, pos);
            if (b == TAB) {
                addField(fieldStart, (int) (pos - fieldStart));
                pos++;
                fieldStart = pos;
            } else if (b == LF) {
                addField(fieldStart, (int) (pos - fieldStart));
                pos++;
                trimTrailingCrFromLastField();
                return true;
            } else {
                pos++;
            }
        }
        addField(fieldStart, (int) (pos - fieldStart));
        trimTrailingCrFromLastField();
        return true;
    }

    private void addField(long offset, int length) {
        if (fieldCount == fieldOffsets.length) {
            int grown = fieldOffsets.length * 2;
            fieldOffsets = Arrays.copyOf(fieldOffsets, grown);
            fieldLengths = Arrays.copyOf(fieldLengths, grown);
        }
        fieldOffsets[fieldCount] = offset;
        fieldLengths[fieldCount] = length;
        fieldCount++;
    }

    private void trimTrailingCrFromLastField() {
        int i = fieldCount - 1;
        if (i < 0)
            return;
        int len = fieldLengths[i];
        if (len == 0)
            return;
        long offset = fieldOffsets[i];
        if (data.get(ValueLayout.JAVA_BYTE, offset + len - 1) == CR) {
            fieldLengths[i] = len - 1;
        }
    }

    public int fieldCount() {
        return fieldCount;
    }

    public long fieldOffset(int i) {
        return fieldOffsets[i];
    }

    public int fieldLength(int i) {
        return fieldLengths[i];
    }

    public boolean fieldIsNull(int i) {
        if (fieldLengths[i] != 2)
            return false;
        long off = fieldOffsets[i];
        return data.get(ValueLayout.JAVA_BYTE, off) == BACKSLASH && data.get(ValueLayout.JAVA_BYTE, off + 1) == UPPER_N;
    }

    public String fieldAsString(int i) {
        int len = fieldLengths[i];
        if (len == 0)
            return "";
        byte[] buf = new byte[len];
        MemorySegment.copy(data, ValueLayout.JAVA_BYTE, fieldOffsets[i], buf, 0, len);
        return new String(buf, StandardCharsets.UTF_8);
    }

    public byte byteAt(long offset) {
        return data.get(ValueLayout.JAVA_BYTE, offset);
    }

    public MemorySegment data() {
        return data;
    }
}
