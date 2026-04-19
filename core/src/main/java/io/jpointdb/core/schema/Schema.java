package io.jpointdb.core.schema;

import java.util.List;

public record Schema(List<Column> columns) {

    public Schema {
        columns = List.copyOf(columns);
    }

    public int columnCount() {
        return columns.size();
    }

    public Column column(int i) {
        return columns.get(i);
    }

    public record Column(String name, ColumnType type, boolean nullable) {
    }
}
