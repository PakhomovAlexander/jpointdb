package io.jpointdb.core.column;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

abstract class AbstractColumn implements AutoCloseable {

    private final Arena arena;
    protected final MemorySegment data;
    protected final MemorySegment nullBitmap;
    protected final long rowCount;
    protected final boolean nullable;

    protected AbstractColumn(Arena arena, MemorySegment data, MemorySegment nullBitmap, long rowCount,
            boolean nullable) {
        this.arena = arena;
        this.data = data;
        this.nullBitmap = nullBitmap;
        this.rowCount = rowCount;
        this.nullable = nullable;
    }

    public final long rowCount() {
        return rowCount;
    }

    public final boolean nullable() {
        return nullable;
    }

    public final boolean isNullAt(long i) {
        if (!nullable)
            return false;
        return !NullBitmap.isValid(nullBitmap, i);
    }

    public final MemorySegment dataSegment() {
        return data;
    }

    public final MemorySegment nullBitmapSegment() {
        return nullBitmap;
    }

    @Override
    public final void close() {
        arena.close();
    }

    protected static Opened openMapped(Path file, byte expectedType) throws IOException {
        Arena arena = Arena.ofShared();
        MemorySegment seg;
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            seg = ch.map(FileChannel.MapMode.READ_ONLY, 0, ch.size(), arena);
        } catch (IOException e) {
            arena.close();
            throw e;
        }
        try {
            byte m0 = seg.get(ValueLayout.JAVA_BYTE, 0);
            byte m1 = seg.get(ValueLayout.JAVA_BYTE, 1);
            byte m2 = seg.get(ValueLayout.JAVA_BYTE, 2);
            byte m3 = seg.get(ValueLayout.JAVA_BYTE, 3);
            if (m0 != 'J' || m1 != 'P' || m2 != 'C' || m3 != 'O') {
                throw new IOException("bad magic in " + file);
            }
            byte version = seg.get(ValueLayout.JAVA_BYTE, 4);
            if (version != ColumnFormat.VERSION) {
                throw new IOException("unsupported column format version " + version);
            }
            byte type = seg.get(ValueLayout.JAVA_BYTE, 5);
            if (type != expectedType) {
                throw new IOException("type mismatch in " + file + ": expected " + expectedType + ", got " + type);
            }
            byte flags = seg.get(ValueLayout.JAVA_BYTE, 6);
            long rowCount = seg.get(ValueLayout.JAVA_LONG, 8);
            long dataBytes = seg.get(ValueLayout.JAVA_LONG, 16);
            long nullBitmapBytes = seg.get(ValueLayout.JAVA_LONG, 24);
            boolean nullable = (flags & ColumnFormat.FLAG_HAS_NULLS) != 0;

            MemorySegment data = seg.asSlice(ColumnFormat.HEADER_SIZE, dataBytes);
            MemorySegment nb =
                    nullable ? seg.asSlice(ColumnFormat.HEADER_SIZE + dataBytes, nullBitmapBytes) : MemorySegment.NULL;

            return new Opened(arena, data, nb, rowCount, nullable);
        } catch (Throwable t) {
            arena.close();
            throw t;
        }
    }

    protected record Opened(Arena arena, MemorySegment data, MemorySegment nullBitmap, long rowCount,
            boolean nullable) {
    }
}
