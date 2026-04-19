package io.jpointdb.core.column;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import org.jspecify.annotations.Nullable;

/**
 * Writes a string column in either DICT (IDs pointing into a
 * {@link DictionaryBuilder}) or RAW (offsets + bytes) encoding. File layout is
 * described by {@link StringColumnFormat}.
 *
 * <p>
 * Use {@link #createDict} or {@link #createRaw} to instantiate.
 */
public final class StringColumnWriter implements AutoCloseable {

    private static final int BUFFER_CAPACITY = 64 * 1024;

    public enum Mode {
        DICT, RAW
    }

    private final FileChannel channel;
    private final ByteBuffer buffer;
    private final Mode mode;
    private final boolean nullable;
    @Nullable
    private final NullBitmap nullBitmap;
    @Nullable
    private final DictionaryBuilder dict;

    private long rowCount;

    private long bytesWritten;
    private long @Nullable [] rawOffsets;
    private int rawOffsetCount;

    private StringColumnWriter(Path file, Mode mode, boolean nullable, @Nullable DictionaryBuilder dict)
            throws IOException {
        this.channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
        this.buffer = ByteBuffer.allocateDirect(BUFFER_CAPACITY).order(ByteOrder.nativeOrder());
        this.mode = mode;
        this.nullable = nullable;
        this.nullBitmap = nullable ? new NullBitmap() : null;
        this.dict = dict;
        if (mode == Mode.RAW) {
            this.rawOffsets = new long[1024];
            this.rawOffsets[0] = 0L;
            this.rawOffsetCount = 1;
        }
        ByteBuffer stub = ByteBuffer.allocate(StringColumnFormat.HEADER_SIZE);
        while (stub.hasRemaining())
            channel.write(stub);
    }

    public static StringColumnWriter createDict(Path file, boolean nullable, DictionaryBuilder dict)
            throws IOException {
        if (dict == null)
            throw new IllegalArgumentException("dict must be non-null");
        return new StringColumnWriter(file, Mode.DICT, nullable, dict);
    }

    public static StringColumnWriter createRaw(Path file, boolean nullable) throws IOException {
        return new StringColumnWriter(file, Mode.RAW, nullable, null);
    }

    public Mode mode() {
        return mode;
    }

    public long rowCount() {
        return rowCount;
    }

    public void appendNonNull(MemorySegment data, long offset, int length) throws IOException {
        if (mode == Mode.DICT) {
            assert dict != null : "dict non-null in DICT mode";
            int id = dict.putOrGet(data, offset, length);
            if (id < 0) {
                throw new IllegalStateException("dictionary overflowed during write — column should use RAW encoding");
            }
            writeInt(id);
        } else {
            writeBytes(data, offset, length);
            bytesWritten += length;
            appendRawOffset(bytesWritten);
        }
        if (nullBitmap != null)
            nullBitmap.appendValid();
        rowCount++;
    }

    public void appendNull() throws IOException {
        if (nullBitmap == null)
            throw new IllegalStateException("column is non-nullable");
        if (mode == Mode.DICT) {
            writeInt(0);
        } else {
            appendRawOffset(bytesWritten);
        }
        nullBitmap.appendNull();
        rowCount++;
    }

    private void writeInt(int v) throws IOException {
        if (buffer.remaining() < 4)
            flush();
        buffer.putInt(v);
    }

    private void writeBytes(MemorySegment data, long offset, int length) throws IOException {
        if (length == 0)
            return;
        int pos = 0;
        while (pos < length) {
            if (!buffer.hasRemaining())
                flush();
            int chunk = Math.min(buffer.remaining(), length - pos);
            for (int i = 0; i < chunk; i++) {
                buffer.put(data.get(ValueLayout.JAVA_BYTE, offset + pos + i));
            }
            pos += chunk;
        }
    }

    private void appendRawOffset(long value) {
        assert rawOffsets != null : "rawOffsets non-null in RAW mode";
        if (rawOffsetCount == rawOffsets.length) {
            rawOffsets = Arrays.copyOf(rawOffsets, rawOffsets.length * 2);
        }
        rawOffsets[rawOffsetCount++] = value;
    }

    private void flush() throws IOException {
        buffer.flip();
        while (buffer.hasRemaining())
            channel.write(buffer);
        buffer.clear();
    }

    @Override
    public void close() throws IOException {
        flush();
        long section1Size;
        long section2Size;
        if (mode == Mode.DICT) {
            section1Size = rowCount * 4L;
            section2Size = 0L;
        } else {
            section1Size = bytesWritten;
            section2Size = (long) rawOffsetCount * 8L;
            writeRawOffsets();
        }
        long nullBitmapBytes = 0L;
        if (nullBitmap != null) {
            byte[] nb = nullBitmap.toByteArray();
            ByteBuffer nbBuf = ByteBuffer.wrap(nb);
            while (nbBuf.hasRemaining())
                channel.write(nbBuf);
            nullBitmapBytes = nb.length;
        }
        ByteBuffer header = ByteBuffer.allocate(StringColumnFormat.HEADER_SIZE).order(ByteOrder.nativeOrder());
        header.put(StringColumnFormat.MAGIC);
        header.put(StringColumnFormat.VERSION);
        header.put(mode == Mode.DICT ? StringColumnFormat.ENCODING_DICT : StringColumnFormat.ENCODING_RAW);
        header.put((byte) (nullable ? StringColumnFormat.FLAG_HAS_NULLS : 0));
        header.put((byte) 0);
        header.putLong(rowCount);
        header.putLong(section1Size);
        header.putLong(section2Size);
        header.putLong(nullBitmapBytes);
        header.flip();
        channel.position(0);
        while (header.hasRemaining())
            channel.write(header);
        channel.close();
    }

    private void writeRawOffsets() throws IOException {
        assert rawOffsets != null : "rawOffsets non-null in RAW mode";
        int remaining = rawOffsetCount;
        int idx = 0;
        ByteBuffer out = ByteBuffer.allocate(16 * 1024).order(ByteOrder.nativeOrder());
        while (remaining > 0) {
            int n = Math.min(remaining, out.capacity() / 8);
            out.clear();
            for (int i = 0; i < n; i++)
                out.putLong(rawOffsets[idx++]);
            out.flip();
            while (out.hasRemaining())
                channel.write(out);
            remaining -= n;
        }
    }
}
