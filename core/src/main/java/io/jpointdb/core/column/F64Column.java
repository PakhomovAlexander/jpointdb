package io.jpointdb.core.column;

import java.io.IOException;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;

public final class F64Column extends AbstractColumn {

    private F64Column(Opened opened) {
        super(opened.arena(), opened.data(), opened.nullBitmap(), opened.rowCount(), opened.nullable());
    }

    public static F64Column open(Path file) throws IOException {
        return new F64Column(openMapped(file, ColumnFormat.TYPE_F64));
    }

    public double get(long i) {
        return data.get(ValueLayout.JAVA_DOUBLE, i * 8);
    }
}
