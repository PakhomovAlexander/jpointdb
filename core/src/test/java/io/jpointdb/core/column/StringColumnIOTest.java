package io.jpointdb.core.column;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class StringColumnIOTest {

    private static MemorySegment utf8(String s) {
        return MemorySegment.ofArray(s.getBytes(StandardCharsets.UTF_8));
    }

    private static void appendString(StringColumnWriter w, String s) throws IOException {
        MemorySegment seg = utf8(s);
        w.appendNonNull(seg, 0, (int) seg.byteSize());
    }

    @Test
    void dictRoundTrip(@TempDir Path dir) throws IOException {
        Path col = dir.resolve("col.bin");
        Path dictFile = dir.resolve("col.dict");
        DictionaryBuilder dict = new DictionaryBuilder();

        try (StringColumnWriter w = StringColumnWriter.createDict(col, false, dict)) {
            appendString(w, "Chrome");
            appendString(w, "Firefox");
            appendString(w, "Chrome");
            appendString(w, "Safari");
            appendString(w, "Chrome");
        }
        dict.writeToFile(dictFile);

        try (StringColumn c = StringColumn.openDict(col, dictFile)) {
            assertEquals(5L, c.rowCount());
            assertEquals(StringColumnWriter.Mode.DICT, c.mode());
            assertEquals("Chrome", c.valueAsString(0));
            assertEquals("Firefox", c.valueAsString(1));
            assertEquals("Chrome", c.valueAsString(2));
            assertEquals("Safari", c.valueAsString(3));
            assertEquals("Chrome", c.valueAsString(4));
            assertEquals(c.idAt(0), c.idAt(2));
            assertNotEquals(c.idAt(0), c.idAt(1));
        }
    }

    @Test
    void dictWithNulls(@TempDir Path dir) throws IOException {
        Path col = dir.resolve("col.bin");
        Path dictFile = dir.resolve("col.dict");
        DictionaryBuilder dict = new DictionaryBuilder();

        try (StringColumnWriter w = StringColumnWriter.createDict(col, true, dict)) {
            appendString(w, "a");
            w.appendNull();
            appendString(w, "b");
            w.appendNull();
            appendString(w, "a");
        }
        dict.writeToFile(dictFile);

        try (StringColumn c = StringColumn.openDict(col, dictFile)) {
            assertEquals(5L, c.rowCount());
            assertTrue(c.nullable());
            assertFalse(c.isNullAt(0));
            assertEquals("a", c.valueAsString(0));
            assertTrue(c.isNullAt(1));
            assertFalse(c.isNullAt(2));
            assertEquals("b", c.valueAsString(2));
            assertTrue(c.isNullAt(3));
            assertFalse(c.isNullAt(4));
            assertEquals("a", c.valueAsString(4));
        }
    }

    @Test
    void rawRoundTrip(@TempDir Path dir) throws IOException {
        Path col = dir.resolve("col.bin");

        try (StringColumnWriter w = StringColumnWriter.createRaw(col, false)) {
            appendString(w, "hello");
            appendString(w, "");
            appendString(w, "привет");
            appendString(w, "world");
        }

        try (StringColumn c = StringColumn.openRaw(col)) {
            assertEquals(4L, c.rowCount());
            assertEquals(StringColumnWriter.Mode.RAW, c.mode());
            assertEquals("hello", c.valueAsString(0));
            assertEquals("", c.valueAsString(1));
            assertEquals("привет", c.valueAsString(2));
            assertEquals("world", c.valueAsString(3));
        }
    }

    @Test
    void rawWithNulls(@TempDir Path dir) throws IOException {
        Path col = dir.resolve("col.bin");

        try (StringColumnWriter w = StringColumnWriter.createRaw(col, true)) {
            appendString(w, "first");
            w.appendNull();
            appendString(w, "");
            appendString(w, "fourth");
            w.appendNull();
        }

        try (StringColumn c = StringColumn.openRaw(col)) {
            assertEquals(5L, c.rowCount());
            assertTrue(c.nullable());
            assertFalse(c.isNullAt(0));
            assertEquals("first", c.valueAsString(0));
            assertTrue(c.isNullAt(1));
            assertFalse(c.isNullAt(2));
            assertEquals("", c.valueAsString(2));
            assertFalse(c.isNullAt(3));
            assertEquals("fourth", c.valueAsString(3));
            assertTrue(c.isNullAt(4));
        }
    }

    @Test
    void emptyStringColumnRaw(@TempDir Path dir) throws IOException {
        Path col = dir.resolve("col.bin");
        try (StringColumnWriter ignored = StringColumnWriter.createRaw(col, false)) {}
        try (StringColumn c = StringColumn.openRaw(col)) {
            assertEquals(0L, c.rowCount());
        }
    }

    @Test
    void dictWriterRejectsDictOverflow(@TempDir Path dir) throws IOException {
        Path col = dir.resolve("col.bin");
        DictionaryBuilder dict = new DictionaryBuilder(1);

        try (StringColumnWriter w = StringColumnWriter.createDict(col, false, dict)) {
            appendString(w, "a");
            assertThrows(IllegalStateException.class, () -> appendString(w, "b"));
        }
    }

    @Test
    void largeRawStringColumn(@TempDir Path dir) throws IOException {
        Path col = dir.resolve("col.bin");
        int n = 10_000;
        try (StringColumnWriter w = StringColumnWriter.createRaw(col, false)) {
            for (int i = 0; i < n; i++)
                appendString(w, "row_" + i);
        }
        try (StringColumn c = StringColumn.openRaw(col)) {
            assertEquals(n, c.rowCount());
            assertEquals("row_0", c.valueAsString(0));
            assertEquals("row_" + (n - 1), c.valueAsString(n - 1));
            assertEquals("row_5000", c.valueAsString(5000));
        }
    }

    @Test
    void wrongMagicRejected(@TempDir Path dir) throws IOException {
        Path col = dir.resolve("col.bin");
        byte[] fake = new byte[StringColumnFormat.HEADER_SIZE];
        fake[0] = 'X';
        fake[1] = 'X';
        fake[2] = 'X';
        fake[3] = 'X';
        java.nio.file.Files.write(col, fake);
        assertThrows(IOException.class, () -> StringColumn.openRaw(col));
    }
}
