package io.jpointdb.core.table;

import io.jpointdb.core.convert.TsvConverter;
import io.jpointdb.core.schema.ColumnType;
import io.jpointdb.core.schema.Encoding;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class TableTest {

    private static Path convert(Path dir, String tsv, TsvConverter.Options opts) throws IOException {
        Path src = dir.resolve("in.tsv");
        Files.writeString(src, tsv);
        Path out = dir.resolve("out.jpdb");
        new TsvConverter(src, out, opts).convert();
        return out;
    }

    @Test
    void openReportsMetadataAndRowCount(@TempDir Path dir) throws IOException {
        Path out = convert(dir, "1\ta\n2\tb\n3\ta\n",
                TsvConverter.Options.defaults().withColumnNames(java.util.List.of("id", "ch")));
        try (Table t = Table.open(out)) {
            assertEquals(3L, t.rowCount());
            assertEquals(2, t.columnCount());
            assertEquals("id", t.columnMeta(0).name());
            assertEquals("ch", t.columnMeta(1).name());
            assertEquals(0, t.columnIndex("id"));
            assertEquals(1, t.columnIndex("ch"));
            assertTrue(t.hasColumn("id"));
            assertFalse(t.hasColumn("nope"));
        }
    }

    @Test
    void readsPrimitiveColumns(@TempDir Path dir) throws IOException {
        Path out = convert(dir, "10\t1.5\n20\t2.5\n30\t3.5\n",
                TsvConverter.Options.defaults().withColumnNames(java.util.List.of("x", "y")));
        try (Table t = Table.open(out)) {
            assertEquals(ColumnType.I32, t.columnMeta("x").type());
            assertEquals(ColumnType.F64, t.columnMeta("y").type());

            assertEquals(10, t.i32("x").get(0));
            assertEquals(30, t.i32("x").get(2));
            assertEquals(1.5, t.f64("y").get(0));
            assertEquals(3.5, t.f64("y").get(2));
        }
    }

    @Test
    void readsDictStringColumn(@TempDir Path dir) throws IOException {
        Path out = convert(dir, "a\nb\na\nb\nc\n", TsvConverter.Options.defaults());
        try (Table t = Table.open(out)) {
            assertEquals(ColumnType.STRING, t.columnMeta(0).type());
            assertEquals(Encoding.DICT, t.columnMeta(0).encoding());
            assertEquals("a", t.string(0).valueAsString(0));
            assertEquals("c", t.string(0).valueAsString(4));
        }
    }

    @Test
    void readsRawStringColumn(@TempDir Path dir) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10; i++)
            sb.append("val").append(i).append('\n');
        Path out = convert(dir, sb.toString(), TsvConverter.Options.defaults().withDictThreshold(3));
        try (Table t = Table.open(out)) {
            assertEquals(Encoding.RAW, t.columnMeta(0).encoding());
            assertEquals("val0", t.string(0).valueAsString(0));
            assertEquals("val9", t.string(0).valueAsString(9));
        }
    }

    @Test
    void wrongTypeAccessThrows(@TempDir Path dir) throws IOException {
        Path out = convert(dir, "1\n2\n", TsvConverter.Options.defaults());
        try (Table t = Table.open(out)) {
            assertThrows(IllegalStateException.class, () -> t.f64(0));
            assertThrows(IllegalStateException.class, () -> t.string(0));
        }
    }

    @Test
    void unknownColumnNameThrows(@TempDir Path dir) throws IOException {
        Path out = convert(dir, "1\n", TsvConverter.Options.defaults());
        try (Table t = Table.open(out)) {
            assertThrows(IllegalArgumentException.class, () -> t.columnIndex("nope"));
        }
    }

    @Test
    void endToEndRoundTripWithMixedTypes(@TempDir Path dir) throws IOException {
        Path out = convert(dir,
                "id\tname\tscore\thits\n" + "1\tAlice\t3.14\t10\n" + "2\tBob\t2.71\t99\n" + "3\tAlice\t1.41\t50\n",
                TsvConverter.Options.defaults().withHeader(true));
        try (Table t = Table.open(out)) {
            assertEquals(3L, t.rowCount());
            assertEquals(4, t.columnCount());

            assertEquals(1, t.i32("id").get(0));
            assertEquals("Alice", t.string("name").valueAsString(0));
            assertEquals(3.14, t.f64("score").get(0));
            assertEquals(10, t.i32("hits").get(0));

            assertEquals(99, t.i32("hits").get(1));
            assertEquals("Alice", t.string("name").valueAsString(2));
        }
    }
}
