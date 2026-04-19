package io.jpointdb.core.convert;

import io.jpointdb.core.column.F64Column;
import io.jpointdb.core.column.I32Column;
import io.jpointdb.core.column.I64Column;
import io.jpointdb.core.column.StringColumn;
import io.jpointdb.core.schema.ColumnType;
import io.jpointdb.core.schema.Encoding;
import io.jpointdb.core.table.TableMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TsvConverterTest {

    private static Path writeTsv(Path dir, String name, String content) throws IOException {
        Path f = dir.resolve(name);
        Files.writeString(f, content);
        return f;
    }

    @Test
    void convertNumericColumnsProducesMetaAndColumnFiles(@TempDir Path dir) throws IOException {
        Path tsv = writeTsv(dir, "input.tsv", "1\t2.5\n3\t4.5\n5\t6.5\n");
        Path out = dir.resolve("out.jpdb");

        new TsvConverter(tsv, out, TsvConverter.Options.defaults()).convert();

        TableMeta meta = TableMeta.load(out.resolve("meta.json"));
        assertEquals(3L, meta.rowCount());
        assertEquals(2, meta.columns().size());
        assertEquals(ColumnType.I32, meta.columns().get(0).type());
        assertEquals(ColumnType.F64, meta.columns().get(1).type());
        assertEquals(Encoding.PLAIN, meta.columns().get(0).encoding());

        try (I32Column c = I32Column.open(out.resolve("col_0.bin"))) {
            assertEquals(3L, c.rowCount());
            assertEquals(1, c.get(0));
            assertEquals(3, c.get(1));
            assertEquals(5, c.get(2));
        }
        try (F64Column c = F64Column.open(out.resolve("col_1.bin"))) {
            assertEquals(3L, c.rowCount());
            assertEquals(2.5, c.get(0));
            assertEquals(6.5, c.get(2));
        }
    }

    @Test
    void convertRespectsHeaderRow(@TempDir Path dir) throws IOException {
        Path tsv = writeTsv(dir, "input.tsv", "id\tname\n1\tAlice\n2\tBob\n");
        Path out = dir.resolve("out.jpdb");

        new TsvConverter(tsv, out, TsvConverter.Options.defaults().withHeader(true)).convert();

        TableMeta meta = TableMeta.load(out.resolve("meta.json"));
        assertEquals(2L, meta.rowCount());
        assertEquals("id", meta.columns().get(0).name());
        assertEquals("name", meta.columns().get(1).name());
        assertEquals(ColumnType.STRING, meta.columns().get(1).type());
        assertEquals(Encoding.DICT, meta.columns().get(1).encoding());
    }

    @Test
    void convertWithProvidedNames(@TempDir Path dir) throws IOException {
        Path tsv = writeTsv(dir, "input.tsv", "1\tA\n2\tB\n");
        Path out = dir.resolve("out.jpdb");

        new TsvConverter(tsv, out, TsvConverter.Options.defaults().withColumnNames(List.of("id", "ch"))).convert();

        TableMeta meta = TableMeta.load(out.resolve("meta.json"));
        assertEquals("id", meta.columns().get(0).name());
        assertEquals("ch", meta.columns().get(1).name());
    }

    @Test
    void stringColumnWithLowCardinalityIsDictEncoded(@TempDir Path dir) throws IOException {
        Path tsv = writeTsv(dir, "input.tsv", "Chrome\nFirefox\nChrome\nChrome\nSafari\nFirefox\n");
        Path out = dir.resolve("out.jpdb");

        new TsvConverter(tsv, out, TsvConverter.Options.defaults()).convert();

        TableMeta meta = TableMeta.load(out.resolve("meta.json"));
        assertEquals(Encoding.DICT, meta.columns().get(0).encoding());
        assertNotNull(meta.columns().get(0).dictFile());

        try (StringColumn c = StringColumn.openDict(out.resolve(meta.columns().get(0).dataFile()),
                out.resolve(meta.columns().get(0).dictFile()))) {
            assertEquals(6L, c.rowCount());
            assertEquals("Chrome", c.valueAsString(0));
            assertEquals("Safari", c.valueAsString(4));
        }
    }

    @Test
    void highCardinalityStringFallsBackToRaw(@TempDir Path dir) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 20; i++)
            sb.append("unique_").append(i).append('\n');
        Path tsv = writeTsv(dir, "input.tsv", sb.toString());
        Path out = dir.resolve("out.jpdb");

        new TsvConverter(tsv, out, TsvConverter.Options.defaults().withDictThreshold(5)).convert();

        TableMeta meta = TableMeta.load(out.resolve("meta.json"));
        assertEquals(Encoding.RAW, meta.columns().get(0).encoding());
        assertNull(meta.columns().get(0).dictFile());

        try (StringColumn c = StringColumn.openRaw(out.resolve(meta.columns().get(0).dataFile()))) {
            assertEquals(20L, c.rowCount());
            assertEquals("unique_0", c.valueAsString(0));
            assertEquals("unique_19", c.valueAsString(19));
        }
    }

    @Test
    void nullMarkerRoundTrips(@TempDir Path dir) throws IOException {
        Path tsv = writeTsv(dir, "input.tsv", "1\ta\n\\N\tb\n3\t\\N\n");
        Path out = dir.resolve("out.jpdb");

        new TsvConverter(tsv, out, TsvConverter.Options.defaults()).convert();

        TableMeta meta = TableMeta.load(out.resolve("meta.json"));
        assertTrue(meta.columns().get(0).nullable());
        assertTrue(meta.columns().get(1).nullable());

        try (I32Column c = I32Column.open(out.resolve("col_0.bin"))) {
            assertEquals(3L, c.rowCount());
            assertFalse(c.isNullAt(0));
            assertEquals(1, c.get(0));
            assertTrue(c.isNullAt(1));
            assertFalse(c.isNullAt(2));
            assertEquals(3, c.get(2));
        }
    }

    @Test
    void mixedTypesAcrossColumns(@TempDir Path dir) throws IOException {
        Path tsv = writeTsv(dir, "input.tsv", "1\t100\t1.5\tfoo\n" + "2\t200\t2.5\tbar\n" + "3\t300\t3.5\tfoo\n");
        Path out = dir.resolve("out.jpdb");

        new TsvConverter(tsv, out, TsvConverter.Options.defaults()).convert();

        TableMeta meta = TableMeta.load(out.resolve("meta.json"));
        assertEquals(4, meta.columns().size());
        assertEquals(ColumnType.I32, meta.columns().get(0).type());
        assertEquals(ColumnType.I32, meta.columns().get(1).type());
        assertEquals(ColumnType.F64, meta.columns().get(2).type());
        assertEquals(ColumnType.STRING, meta.columns().get(3).type());

        try (I32Column c = I32Column.open(out.resolve("col_0.bin"))) {
            assertEquals(3L, c.rowCount());
            assertEquals(1, c.get(0));
            assertEquals(3, c.get(2));
        }
    }

    @Test
    void i64ColumnHandledWhenValueOverflowsI32(@TempDir Path dir) throws IOException {
        Path tsv = writeTsv(dir, "input.tsv", "1\n2147483648\n-1\n");
        Path out = dir.resolve("out.jpdb");

        new TsvConverter(tsv, out, TsvConverter.Options.defaults()).convert();

        TableMeta meta = TableMeta.load(out.resolve("meta.json"));
        assertEquals(ColumnType.I64, meta.columns().get(0).type());

        try (I64Column c = I64Column.open(out.resolve("col_0.bin"))) {
            assertEquals(1L, c.get(0));
            assertEquals(2147483648L, c.get(1));
            assertEquals(-1L, c.get(2));
        }
    }

    @Test
    void idempotentConvertOverwrites(@TempDir Path dir) throws IOException {
        Path tsv = writeTsv(dir, "input.tsv", "1\n2\n");
        Path out = dir.resolve("out.jpdb");

        new TsvConverter(tsv, out, TsvConverter.Options.defaults()).convert();
        new TsvConverter(tsv, out, TsvConverter.Options.defaults()).convert();

        TableMeta meta = TableMeta.load(out.resolve("meta.json"));
        assertEquals(2L, meta.rowCount());
    }
}
