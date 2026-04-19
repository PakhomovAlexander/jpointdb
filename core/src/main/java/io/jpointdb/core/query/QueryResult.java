package io.jpointdb.core.query;

import io.jpointdb.core.sql.ValueType;
import org.jspecify.annotations.Nullable;

import java.util.List;

public record QueryResult(List<String> columnNames, List<ValueType> columnTypes, List<@Nullable Object[]> rows) {

    public QueryResult {
        columnNames = List.copyOf(columnNames);
        columnTypes = List.copyOf(columnTypes);
        rows = List.copyOf(rows);
    }

    public int columnCount() {
        return columnNames.size();
    }

    public int rowCount() {
        return rows.size();
    }
}
