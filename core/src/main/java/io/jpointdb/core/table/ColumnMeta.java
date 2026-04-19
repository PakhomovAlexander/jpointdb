package io.jpointdb.core.table;

import io.jpointdb.core.schema.ColumnType;
import io.jpointdb.core.schema.Encoding;
import org.jspecify.annotations.Nullable;

/**
 * Per-column metadata stored in {@code meta.json}.
 */
public record ColumnMeta(String name, ColumnType type, boolean nullable, Encoding encoding, String dataFile,
        @Nullable String dictFile) {
    public ColumnMeta {
        if (name == null)
            throw new IllegalArgumentException("name required");
        if (type == null)
            throw new IllegalArgumentException("type required");
        if (encoding == null)
            throw new IllegalArgumentException("encoding required");
        if (dataFile == null)
            throw new IllegalArgumentException("dataFile required");
        if (encoding == Encoding.DICT && dictFile == null) {
            throw new IllegalArgumentException("dictFile required for DICT encoding");
        }
    }
}
