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
 * Mmapped reader for a dictionary file written by
 * {@link DictionaryBuilder#writeToFile}. Exposes key bytes by ID without
 * allocating intermediate objects.
 */
public final class Dictionary implements AutoCloseable {

    private final Arena arena;
    private final MemorySegment offsets;
    private final MemorySegment keyBytes;
    private final int size;
    // Lazy String cache by dict id. First read decodes UTF-8 and stores;
    // subsequent reads return the cached reference for free. Data race on
    // a slot is benign — Strings are immutable and keyAsString(id) is pure.
    private final @Nullable String[] stringCache;

    private Dictionary(Arena arena, MemorySegment offsets, MemorySegment keyBytes, int size) {
        this.arena = arena;
        this.offsets = offsets;
        this.keyBytes = keyBytes;
        this.size = size;
        this.stringCache = new String[size];
    }

    public static Dictionary open(Path file) throws IOException {
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
            if (m0 != 'J' || m1 != 'P' || m2 != 'D' || m3 != 'I') {
                throw new IOException("bad dictionary magic in " + file);
            }
            byte version = seg.get(ValueLayout.JAVA_BYTE, 4);
            if (version != 1)
                throw new IOException("unsupported dictionary version " + version);
            long count = seg.get(ValueLayout.JAVA_LONG, 8);
            long keyBytesLen = seg.get(ValueLayout.JAVA_LONG, 16);
            if (count < 0 || count > Integer.MAX_VALUE - 1) {
                throw new IOException("dictionary size out of range: " + count);
            }
            int size = (int) count;
            long offsetsStart = DictionaryBuilder.HEADER_SIZE;
            long offsetsLen = (long) (size + 1) * 8L;
            MemorySegment offsets = seg.asSlice(offsetsStart, offsetsLen);
            MemorySegment keyBytes = seg.asSlice(offsetsStart + offsetsLen, keyBytesLen);
            return new Dictionary(arena, offsets, keyBytes, size);
        } catch (Throwable t) {
            arena.close();
            throw t;
        }
    }

    public int size() {
        return size;
    }

    public int keyLength(int id) {
        long start = offsets.get(ValueLayout.JAVA_LONG, (long) id * 8);
        long end = offsets.get(ValueLayout.JAVA_LONG, (long) (id + 1) * 8);
        return (int) (end - start);
    }

    public long keyOffset(int id) {
        return offsets.get(ValueLayout.JAVA_LONG, (long) id * 8);
    }

    public byte[] keyBytes(int id) {
        long start = offsets.get(ValueLayout.JAVA_LONG, (long) id * 8);
        long end = offsets.get(ValueLayout.JAVA_LONG, (long) (id + 1) * 8);
        int len = (int) (end - start);
        byte[] out = new byte[len];
        if (len > 0) {
            MemorySegment.copy(keyBytes, ValueLayout.JAVA_BYTE, start, out, 0, len);
        }
        return out;
    }

    public String keyAsString(int id) {
        return new String(keyBytes(id), StandardCharsets.UTF_8);
    }

    /**
     * Returns the cached {@link String} for {@code id}, decoding it from
     * UTF-8 on the first access. Subsequent reads skip allocation entirely.
     */
    public String stringAt(int id) {
        String s = stringCache[id];
        if (s != null)
            return s;
        s = keyAsString(id);
        stringCache[id] = s;
        return s;
    }

    public MemorySegment keyBytesSegment() {
        return keyBytes;
    }

    public MemorySegment offsetsSegment() {
        return offsets;
    }

    @Override
    public void close() {
        arena.close();
    }
}
