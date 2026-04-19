package io.jpointdb.core.column;

import java.io.IOException;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;

public final class I64Column extends AbstractColumn {

    private I64Column(Opened opened) {
        super(opened.arena(), opened.data(), opened.nullBitmap(), opened.rowCount(), opened.nullable());
    }

    public static I64Column open(Path file) throws IOException {
        return new I64Column(openMapped(file, ColumnFormat.TYPE_I64));
    }

    public long get(long i) {
        return data.get(ValueLayout.JAVA_LONG, i * 8);
    }
}
