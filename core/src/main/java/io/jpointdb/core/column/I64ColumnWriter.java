package io.jpointdb.core.column;

import java.io.IOException;
import java.nio.file.Path;

public final class I64ColumnWriter extends AbstractColumnWriter {

    private static final int BUFFER_CAPACITY = 64 * 1024;

    public I64ColumnWriter(Path file, boolean nullable) throws IOException {
        super(file, BUFFER_CAPACITY, nullable);
    }

    @Override
    protected byte typeCode() {
        return ColumnFormat.TYPE_I64;
    }

    @Override
    protected int valueSize() {
        return 8;
    }

    public void appendNonNull(long v) throws IOException {
        ensureRoom();
        buffer().putLong(v);
        noteValidAppended();
    }

    public void appendNull() throws IOException {
        appendNullInternal();
    }
}
