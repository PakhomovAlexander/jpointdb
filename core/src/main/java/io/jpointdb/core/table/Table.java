package io.jpointdb.core.table;

import io.jpointdb.core.column.F64Column;
import io.jpointdb.core.column.I32Column;
import io.jpointdb.core.column.I64Column;
import io.jpointdb.core.column.StringColumn;
import io.jpointdb.core.schema.ColumnType;
import io.jpointdb.core.schema.Encoding;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A read-only mmapped view of a JPointDB table directory produced by
 * {@link io.jpointdb.core.convert.TsvConverter}. Holds type-specific column
 * readers; {@link #close()} releases them all.
 */
public final class Table implements AutoCloseable {

    private final Path dir;
    private final TableMeta meta;
    private final List<AutoCloseable> columns;
    private final Map<String, Integer> nameToIndex;

    private Table(Path dir, TableMeta meta, List<AutoCloseable> columns) {
        this.dir = dir;
        this.meta = meta;
        this.columns = columns;
        this.nameToIndex = new HashMap<>();
        for (int i = 0; i < meta.columns().size(); i++) {
            nameToIndex.put(meta.columns().get(i).name(), i);
        }
    }

    public static Table open(Path dir) throws IOException {
        TableMeta meta = TableMeta.load(dir.resolve("meta.json"));
        List<AutoCloseable> opened = new ArrayList<>(meta.columns().size());
        try {
            for (ColumnMeta c : meta.columns()) {
                opened.add(openColumn(dir, c));
            }
        } catch (IOException | RuntimeException e) {
            closeAllQuietly(opened);
            throw e;
        }
        return new Table(dir, meta, opened);
    }

    private static AutoCloseable openColumn(Path dir, ColumnMeta c) throws IOException {
        Path dataFile = dir.resolve(c.dataFile());
        return switch (c.type()) {
            case I32 -> I32Column.open(dataFile);
            case I64 -> I64Column.open(dataFile);
            case F64 -> F64Column.open(dataFile);
            case STRING -> (c.encoding() == Encoding.DICT) ? StringColumn.openDict(dataFile, dir.resolve(c.dictFile()))
                                                           : StringColumn.openRaw(dataFile);
        };
    }

    public Path dir() {
        return dir;
    }

    public TableMeta meta() {
        return meta;
    }

    public long rowCount() {
        return meta.rowCount();
    }

    public int columnCount() {
        return meta.columns().size();
    }

    public ColumnMeta columnMeta(int index) {
        return meta.columns().get(index);
    }

    public ColumnMeta columnMeta(String name) {
        return meta.columns().get(columnIndex(name));
    }

    public int columnIndex(String name) {
        Integer i = nameToIndex.get(name);
        if (i == null)
            throw new IllegalArgumentException("no column named: " + name);
        return i;
    }

    public boolean hasColumn(String name) {
        return nameToIndex.containsKey(name);
    }

    public I32Column i32(int index) {
        requireType(index, ColumnType.I32);
        return (I32Column) columns.get(index);
    }

    public I32Column i32(String name) {
        return i32(columnIndex(name));
    }

    public I64Column i64(int index) {
        requireType(index, ColumnType.I64);
        return (I64Column) columns.get(index);
    }

    public I64Column i64(String name) {
        return i64(columnIndex(name));
    }

    public F64Column f64(int index) {
        requireType(index, ColumnType.F64);
        return (F64Column) columns.get(index);
    }

    public F64Column f64(String name) {
        return f64(columnIndex(name));
    }

    public StringColumn string(int index) {
        requireType(index, ColumnType.STRING);
        return (StringColumn) columns.get(index);
    }

    public StringColumn string(String name) {
        return string(columnIndex(name));
    }

    private void requireType(int index, ColumnType expected) {
        ColumnType actual = meta.columns().get(index).type();
        if (actual != expected) {
            throw new IllegalStateException(
                    "column '" + meta.columns().get(index).name() + "' is " + actual + ", not " + expected);
        }
    }

    @Override
    public void close() throws IOException {
        IOException first = null;
        for (AutoCloseable c : columns) {
            try {
                c.close();
            } catch (Exception e) {
                if (first == null) {
                    first = e instanceof IOException io ? io : new IOException(e);
                } else {
                    first.addSuppressed(e);
                }
            }
        }
        if (first != null)
            throw first;
    }

    private static void closeAllQuietly(List<AutoCloseable> opened) {
        for (AutoCloseable c : opened) {
            try {
                c.close();
            } catch (Exception ignored) {
                // Best-effort cleanup path invoked only when the primary open() call already
                // failed — we swallow secondary errors so the original is what the caller sees.
            }
        }
    }
}
