package io.jpointdb.core.column;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.jspecify.annotations.Nullable;

/**
 * Common machinery for primitive column writers: reserves the header, streams
 * data through a {@link ByteBuffer}, accumulates a {@link NullBitmap} in
 * memory, and rewrites the header on close with final sizes.
 */
abstract class AbstractColumnWriter implements AutoCloseable {

    private final FileChannel channel;
    private final ByteBuffer buffer;
    private final boolean nullable;
    @Nullable
    private final NullBitmap nullBitmap;
    private long rowCount;

    protected AbstractColumnWriter(Path file, int bufferCapacity, boolean nullable) throws IOException {
        this.channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
        this.buffer = ByteBuffer.allocateDirect(bufferCapacity).order(ByteOrder.nativeOrder());
        this.nullable = nullable;
        this.nullBitmap = nullable ? new NullBitmap() : null;
        ByteBuffer headerStub = ByteBuffer.allocate(ColumnFormat.HEADER_SIZE);
        while (headerStub.hasRemaining())
            channel.write(headerStub);
    }

    protected abstract byte typeCode();

    protected abstract int valueSize();

    protected final ByteBuffer buffer() {
        return buffer;
    }

    protected final void ensureRoom() throws IOException {
        if (buffer.remaining() < valueSize())
            flush();
    }

    protected final void noteValidAppended() {
        if (nullBitmap != null)
            nullBitmap.appendValid();
        rowCount++;
    }

    protected final void appendNullInternal() throws IOException {
        if (nullBitmap == null)
            throw new IllegalStateException("column is non-nullable");
        ensureRoom();
        for (int i = 0; i < valueSize(); i++)
            buffer.put((byte) 0);
        nullBitmap.appendNull();
        rowCount++;
    }

    private void flush() throws IOException {
        buffer.flip();
        while (buffer.hasRemaining())
            channel.write(buffer);
        buffer.clear();
    }

    @Override
    public final void close() throws IOException {
        flush();
        long dataBytes = rowCount * valueSize();
        long nullBitmapBytes = 0;
        if (nullBitmap != null) {
            byte[] nb = nullBitmap.toByteArray();
            ByteBuffer nbBuf = ByteBuffer.wrap(nb);
            while (nbBuf.hasRemaining())
                channel.write(nbBuf);
            nullBitmapBytes = nb.length;
        }
        ByteBuffer header = ByteBuffer.allocate(ColumnFormat.HEADER_SIZE).order(ByteOrder.nativeOrder());
        header.put(ColumnFormat.MAGIC);
        header.put(ColumnFormat.VERSION);
        header.put(typeCode());
        header.put((byte) (nullable ? ColumnFormat.FLAG_HAS_NULLS : 0));
        header.put((byte) 0);
        header.putLong(rowCount);
        header.putLong(dataBytes);
        header.putLong(nullBitmapBytes);
        header.flip();
        channel.position(0);
        while (header.hasRemaining())
            channel.write(header);
        channel.close();
    }

    public final long rowCount() {
        return rowCount;
    }
}
