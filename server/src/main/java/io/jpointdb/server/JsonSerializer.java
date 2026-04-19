package io.jpointdb.server;

import io.jpointdb.core.json.Json;
import io.jpointdb.core.query.QueryResult;
import io.jpointdb.core.table.ColumnMeta;
import io.jpointdb.core.table.Table;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** JSON serialization for REST responses. */
final class JsonSerializer {

    private JsonSerializer() {
    }

    static String schema(Table table) {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("name", tableName(table));
        root.put("rowCount", table.rowCount());
        List<Object> cols = new ArrayList<>(table.columnCount());
        for (int i = 0; i < table.columnCount(); i++) {
            ColumnMeta m = table.columnMeta(i);
            Map<String, Object> c = new LinkedHashMap<>();
            c.put("name", m.name());
            c.put("type", m.type().name());
            c.put("nullable", m.nullable());
            c.put("encoding", m.encoding().name());
            cols.add(c);
        }
        root.put("columns", cols);
        return Json.write(root);
    }

    private static String tableName(Table t) {
        String dn = t.dir().getFileName().toString();
        return dn.endsWith(".jpdb") ? dn.substring(0, dn.length() - 5) : dn;
    }

    static String queryResult(QueryResult r, long elapsedMs) {
        Map<String, Object> root = new LinkedHashMap<>();
        List<Object> cols = new ArrayList<>(r.columnCount());
        for (int i = 0; i < r.columnCount(); i++) {
            Map<String, Object> c = new LinkedHashMap<>();
            c.put("name", r.columnNames().get(i));
            c.put("type", r.columnTypes().get(i).name());
            cols.add(c);
        }
        root.put("columns", cols);
        List<Object> rows = new ArrayList<>(r.rowCount());
        for (Object[] row : r.rows()) {
            List<Object> jRow = new ArrayList<>(row.length);
            for (Object v : row)
                jRow.add(v);
            rows.add(jRow);
        }
        root.put("rows", rows);
        root.put("rowCount", r.rowCount());
        root.put("elapsedMs", elapsedMs);
        return Json.write(root);
    }

    static String error(String message) {
        return Json.write(Map.of("error", message == null ? "unknown" : message));
    }
}
