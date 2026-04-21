package io.jpointdb.core.column;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;

public final class I32Column extends AbstractColumn {

    private I32Column(Opened opened) {
        super(opened.arena(), opened.data(), opened.nullBitmap(), opened.rowCount(), opened.nullable());
    }

    public static I32Column open(Path file) throws IOException {
        return new I32Column(openMapped(file, ColumnFormat.TYPE_I32));
    }

    public int get(long i) {
        return data.get(ValueLayout.JAVA_INT, i * 4);
    }

    /**
     * Bulk-read {@code len} ints starting at {@code startRow} into
     * {@code dst[0..len]}. Collapses to a single {@code memcpy} on aligned
     * little-endian hardware — SIMD-friendly input for batch aggregators.
     */
    public void readInts(long startRow, int[] dst, int len) {
        MemorySegment.copy(data, ValueLayout.JAVA_INT, startRow * 4L, dst, 0, len);
    }
}
