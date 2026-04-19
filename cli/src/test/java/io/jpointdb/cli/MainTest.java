package io.jpointdb.cli;

import io.jpointdb.core.json.Json;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static io.jpointdb.cli.Main.MetaCommand.*;

class MainTest {

    @Test
    void printTableFormatsHeadersAndRows() throws Exception {
        String json = Json.write(Map.of("columns",
                List.of(Map.of("name", "name", "type", "STRING"), Map.of("name", "count", "type", "I64")), "rows",
                List.of(List.of("alice", 42L), List.of("bob", 7L)), "rowCount", 2L, "elapsedMs", 3L));
        Object parsed = Json.parse(json);
        String out = captureStdout(() -> Main.printTable(parsed));
        assertTrue(out.contains("name"));
        assertTrue(out.contains("count"));
        assertTrue(out.contains("alice"));
        assertTrue(out.contains("bob"));
        assertTrue(out.contains("42"));
        assertTrue(out.contains("(2 rows in 3 ms)"));
    }

    @Test
    void printTableRendersNullAsPlaceholder() throws Exception {
        String json = Json.write(Map.of("columns", List.of(Map.of("name", "x", "type", "STRING")), "rows",
                List.of(java.util.Arrays.asList(new Object[]{null})), "rowCount", 1L, "elapsedMs", 0L));
        Object parsed = Json.parse(json);
        String out = captureStdout(() -> Main.printTable(parsed));
        assertTrue(out.contains("∅"));
    }

    @Test
    void parseMetaRecognizesExitVariants() {
        assertEquals(EXIT, Main.parseMeta("\\q"));
        assertEquals(EXIT, Main.parseMeta("exit"));
        assertEquals(EXIT, Main.parseMeta("QUIT"));
    }

    @Test
    void parseMetaRecognizesClearVariants() {
        assertEquals(CLEAR, Main.parseMeta("\\c"));
        assertEquals(CLEAR, Main.parseMeta("clear"));
        assertEquals(CLEAR, Main.parseMeta("CLS"));
    }

    @Test
    void parseMetaRecognizesSchemaAndHelp() {
        assertEquals(SCHEMA, Main.parseMeta("\\s"));
        assertEquals(SCHEMA, Main.parseMeta("schema"));
        assertEquals(HELP, Main.parseMeta("\\h"));
        assertEquals(HELP, Main.parseMeta("help"));
        assertEquals(HELP, Main.parseMeta("?"));
    }

    @Test
    void parseMetaReturnsNoneForSql() {
        assertEquals(NONE, Main.parseMeta("SELECT 1"));
        assertEquals(NONE, Main.parseMeta("SELECT * FROM t"));
    }

    @Test
    void parseMetaReturnsEmptyForBlank() {
        assertEquals(EMPTY, Main.parseMeta(""));
    }

    private static String captureStdout(Runnable r) {
        PrintStream original = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (PrintStream ps = new PrintStream(baos, true, StandardCharsets.UTF_8)) {
            System.setOut(ps);
            r.run();
        } finally {
            System.setOut(original);
        }
        return baos.toString(StandardCharsets.UTF_8);
    }
}
