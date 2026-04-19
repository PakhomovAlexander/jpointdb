package io.jpointdb.core.column;

import java.io.IOException;
import java.nio.file.Path;

public final class F64ColumnWriter extends AbstractColumnWriter {

    private static final int BUFFER_CAPACITY = 64 * 1024;

    public F64ColumnWriter(Path file, boolean nullable) throws IOException {
        super(file, BUFFER_CAPACITY, nullable);
    }

    @Override
    protected byte typeCode() {
        return ColumnFormat.TYPE_F64;
    }

    @Override
    protected int valueSize() {
        return 8;
    }

    public void appendNonNull(double v) throws IOException {
        ensureRoom();
        buffer().putDouble(v);
        noteValidAppended();
    }

    public void appendNull() throws IOException {
        appendNullInternal();
    }
}
