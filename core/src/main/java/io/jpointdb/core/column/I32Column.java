package io.jpointdb.core.column;

import java.io.IOException;
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
}
