package io.jpointdb.core.column;

import java.io.IOException;
import java.nio.file.Path;

public final class I32ColumnWriter extends AbstractColumnWriter {

    private static final int BUFFER_CAPACITY = 64 * 1024;

    public I32ColumnWriter(Path file, boolean nullable) throws IOException {
        super(file, BUFFER_CAPACITY, nullable);
    }

    @Override
    protected byte typeCode() {
        return ColumnFormat.TYPE_I32;
    }

    @Override
    protected int valueSize() {
        return 4;
    }

    public void appendNonNull(int v) throws IOException {
        ensureRoom();
        buffer().putInt(v);
        noteValidAppended();
    }

    public void appendNull() throws IOException {
        appendNullInternal();
    }
}
