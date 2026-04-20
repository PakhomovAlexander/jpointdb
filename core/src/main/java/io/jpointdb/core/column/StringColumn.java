package io.jpointdb.core.column;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.jspecify.annotations.Nullable;

/**
 * Mmapped reader for string columns (DICT or RAW).
 *
 * <p>
 * For DICT columns, the caller provides the path to the companion dictionary
 * file; {@link StringColumn#close()} releases both the column and the
 * dictionary resources.
 */
public final class StringColumn implements AutoCloseable {

    private final Arena arena;
    private final StringColumnWriter.Mode mode;
    private final long rowCount;
    private final boolean nullable;

    private final MemorySegment ids;
    @Nullable
    private final Dictionary dict;

    private final MemorySegment offsets;
    private final MemorySegment bytes;

    private final MemorySegment nullBitmap;

    private StringColumn(Arena arena, StringColumnWriter.Mode mode, long rowCount, boolean nullable, MemorySegment ids,
            @Nullable Dictionary dict, MemorySegment offsets, MemorySegment bytes, MemorySegment nullBitmap) {
        this.arena = arena;
        this.mode = mode;
        this.rowCount = rowCount;
        this.nullable = nullable;
        this.ids = ids;
        this.dict = dict;
        this.offsets = offsets;
        this.bytes = bytes;
        this.nullBitmap = nullBitmap;
    }

    public static StringColumn openRaw(Path file) throws IOException {
        return open(file, null);
    }

    public static StringColumn openDict(Path file, Path dictFile) throws IOException {
        return open(file, dictFile);
    }

    private static StringColumn open(Path file, @Nullable Path dictFile) throws IOException {
        Arena arena = Arena.ofShared();
        MemorySegment seg;
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            seg = ch.map(FileChannel.MapMode.READ_ONLY, 0, ch.size(), arena);
        } catch (IOException e) {
            arena.close();
            throw e;
        }
        Dictionary openedDict = null;
        try {
            byte m0 = seg.get(ValueLayout.JAVA_BYTE, 0);
            byte m1 = seg.get(ValueLayout.JAVA_BYTE, 1);
            byte m2 = seg.get(ValueLayout.JAVA_BYTE, 2);
            byte m3 = seg.get(ValueLayout.JAVA_BYTE, 3);
            if (m0 != 'J' || m1 != 'P' || m2 != 'C' || m3 != 'S') {
                throw new IOException("bad string column magic in " + file);
            }
            byte version = seg.get(ValueLayout.JAVA_BYTE, 4);
            if (version != StringColumnFormat.VERSION) {
                throw new IOException("unsupported string column version " + version);
            }
            byte encoding = seg.get(ValueLayout.JAVA_BYTE, 5);
            byte flags = seg.get(ValueLayout.JAVA_BYTE, 6);
            long rowCount = seg.get(ValueLayout.JAVA_LONG, 8);
            long section1 = seg.get(ValueLayout.JAVA_LONG, 16);
            long section2 = seg.get(ValueLayout.JAVA_LONG, 24);
            long nullBitmapBytes = seg.get(ValueLayout.JAVA_LONG, 32);
            boolean nullable = (flags & StringColumnFormat.FLAG_HAS_NULLS) != 0;
            long base = StringColumnFormat.HEADER_SIZE;

            StringColumnWriter.Mode mode;
            MemorySegment idsSeg = MemorySegment.NULL;
            MemorySegment offsetsSeg = MemorySegment.NULL;
            MemorySegment bytesSeg = MemorySegment.NULL;

            if (encoding == StringColumnFormat.ENCODING_DICT) {
                if (dictFile == null) {
                    throw new IOException("dictionary path required for DICT-encoded column " + file);
                }
                mode = StringColumnWriter.Mode.DICT;
                idsSeg = seg.asSlice(base, section1);
                openedDict = Dictionary.open(dictFile);
            } else if (encoding == StringColumnFormat.ENCODING_RAW) {
                mode = StringColumnWriter.Mode.RAW;
                bytesSeg = seg.asSlice(base, section1);
                offsetsSeg = seg.asSlice(base + section1, section2);
            } else {
                throw new IOException("unknown string encoding " + encoding + " in " + file);
            }

            MemorySegment nullBitmapSeg =
                    nullable ? seg.asSlice(base + section1 + section2, nullBitmapBytes) : MemorySegment.NULL;

            return new StringColumn(arena, mode, rowCount, nullable, idsSeg, openedDict, offsetsSeg, bytesSeg,
                    nullBitmapSeg);
        } catch (Throwable t) {
            if (openedDict != null)
                openedDict.close();
            arena.close();
            throw t;
        }
    }

    public StringColumnWriter.Mode mode() {
        return mode;
    }

    public long rowCount() {
        return rowCount;
    }

    public boolean nullable() {
        return nullable;
    }

    public boolean isNullAt(long i) {
        if (!nullable)
            return false;
        return !NullBitmap.isValid(nullBitmap, i);
    }

    public int idAt(long i) {
        if (mode != StringColumnWriter.Mode.DICT) {
            throw new IllegalStateException("idAt is only valid for DICT-encoded columns");
        }
        return ids.get(ValueLayout.JAVA_INT, i * 4);
    }

    public byte[] valueBytes(long i) {
        if (mode == StringColumnWriter.Mode.DICT) {
            int id = ids.get(ValueLayout.JAVA_INT, i * 4);
            Dictionary d = dict;
            if (d == null)
                throw new IllegalStateException("dict missing in DICT mode");
            return d.keyBytes(id);
        }
        long start = offsets.get(ValueLayout.JAVA_LONG_UNALIGNED, i * 8);
        long end = offsets.get(ValueLayout.JAVA_LONG_UNALIGNED, (i + 1) * 8);
        int len = (int) (end - start);
        byte[] out = new byte[len];
        if (len > 0) {
            MemorySegment.copy(bytes, ValueLayout.JAVA_BYTE, start, out, 0, len);
        }
        return out;
    }

    public String valueAsString(long i) {
        if (mode == StringColumnWriter.Mode.DICT) {
            Dictionary d = dict;
            if (d == null)
                throw new IllegalStateException("dict missing in DICT mode");
            return d.stringAt(ids.get(ValueLayout.JAVA_INT, i * 4));
        }
        return new String(valueBytes(i), StandardCharsets.UTF_8);
    }

    public @Nullable Dictionary dictionary() {
        return dict;
    }

    public MemorySegment idsSegment() {
        return ids;
    }

    public MemorySegment offsetsSegment() {
        return offsets;
    }

    public MemorySegment bytesSegment() {
        return bytes;
    }

    public MemorySegment nullBitmapSegment() {
        return nullBitmap;
    }

    @Override
    public void close() {
        if (dict != null)
            dict.close();
        arena.close();
    }
}
