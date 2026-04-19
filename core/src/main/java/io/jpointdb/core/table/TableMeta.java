package io.jpointdb.core.table;

import io.jpointdb.core.json.Json;
import io.jpointdb.core.schema.ColumnType;
import io.jpointdb.core.schema.Encoding;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Table-level metadata persisted as {@code meta.json} in the .jpdb directory.
 */
public record TableMeta(int version, long rowCount, List<ColumnMeta> columns) {

    public static final int CURRENT_VERSION = 1;

    public TableMeta {
        columns = List.copyOf(columns);
    }

    public void save(Path file) throws IOException {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("version", (long) version);
        root.put("rowCount", rowCount);
        List<Object> cols = new ArrayList<>(columns.size());
        for (ColumnMeta c : columns) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("name", c.name());
            m.put("type", c.type().name());
            m.put("nullable", c.nullable());
            m.put("encoding", c.encoding().name());
            m.put("dataFile", c.dataFile());
            if (c.dictFile() != null)
                m.put("dictFile", c.dictFile());
            cols.add(m);
        }
        root.put("columns", cols);
        Files.writeString(file, Json.write(root), StandardCharsets.UTF_8);
    }

    @SuppressWarnings("unchecked")
    public static TableMeta load(Path file) throws IOException {
        String text = Files.readString(file, StandardCharsets.UTF_8);
        Object parsed = Json.parse(text);
        if (!(parsed instanceof Map<?, ?> r)) {
            throw new IOException("meta.json: root is not an object");
        }
        Map<String, Object> root = (Map<String, Object>) r;
        int version = ((Number) req(root, "version")).intValue();
        long rowCount = ((Number) req(root, "rowCount")).longValue();
        Object colsObj = req(root, "columns");
        if (!(colsObj instanceof List<?>))
            throw new IOException("meta.json: columns is not a list");
        List<Object> colList = (List<Object>) colsObj;
        List<ColumnMeta> cols = new ArrayList<>(colList.size());
        for (Object co : colList) {
            if (!(co instanceof Map<?, ?> cm))
                throw new IOException("meta.json: column is not an object");
            Map<String, Object> m = (Map<String, Object>) cm;
            cols.add(new ColumnMeta((String) req(m, "name"), ColumnType.valueOf((String) req(m, "type")),
                    (Boolean) req(m, "nullable"), Encoding.valueOf((String) req(m, "encoding")),
                    (String) req(m, "dataFile"), (String) m.get("dictFile")));
        }
        return new TableMeta(version, rowCount, cols);
    }

    private static Object req(Map<String, Object> m, String k) throws IOException {
        Object v = m.get(k);
        if (v == null)
            throw new IOException("meta.json: missing '" + k + "'");
        return v;
    }
}
