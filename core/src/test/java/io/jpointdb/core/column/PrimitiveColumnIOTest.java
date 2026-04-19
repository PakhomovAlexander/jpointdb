package io.jpointdb.core.column;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class PrimitiveColumnIOTest {

    @Test
    void i64RoundTripNonNullable(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("col.bin");
        try (I64ColumnWriter w = new I64ColumnWriter(file, false)) {
            for (int i = 0; i < 10; i++)
                w.appendNonNull(i * 100L);
        }
        try (I64Column c = I64Column.open(file)) {
            assertEquals(10L, c.rowCount());
            assertFalse(c.nullable());
            for (int i = 0; i < 10; i++) {
                assertEquals(i * 100L, c.get(i));
                assertFalse(c.isNullAt(i));
            }
        }
    }

    @Test
    void i64RoundTripWithNulls(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("col.bin");
        try (I64ColumnWriter w = new I64ColumnWriter(file, true)) {
            w.appendNonNull(1L);
            w.appendNull();
            w.appendNonNull(3L);
            w.appendNull();
            w.appendNonNull(5L);
        }
        try (I64Column c = I64Column.open(file)) {
            assertEquals(5L, c.rowCount());
            assertTrue(c.nullable());
            assertFalse(c.isNullAt(0));
            assertEquals(1L, c.get(0));
            assertTrue(c.isNullAt(1));
            assertFalse(c.isNullAt(2));
            assertEquals(3L, c.get(2));
            assertTrue(c.isNullAt(3));
            assertFalse(c.isNullAt(4));
            assertEquals(5L, c.get(4));
        }
    }

    @Test
    void i64EmptyColumn(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("col.bin");
        try (I64ColumnWriter ignored = new I64ColumnWriter(file, false)) {}
        try (I64Column c = I64Column.open(file)) {
            assertEquals(0L, c.rowCount());
        }
    }

    @Test
    void i64NonNullableRejectsNull(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("col.bin");
        try (I64ColumnWriter w = new I64ColumnWriter(file, false)) {
            assertThrows(IllegalStateException.class, w::appendNull);
        }
    }

    @Test
    void i64LargeColumn(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("col.bin");
        int n = 100_000;
        try (I64ColumnWriter w = new I64ColumnWriter(file, false)) {
            for (int i = 0; i < n; i++)
                w.appendNonNull((long) i * 7);
        }
        try (I64Column c = I64Column.open(file)) {
            assertEquals(n, c.rowCount());
            assertEquals(0L, c.get(0));
            assertEquals(7L * (n - 1), c.get(n - 1));
            assertEquals(7L * 12345, c.get(12345));
        }
    }

    @Test
    void i32RoundTrip(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("col.bin");
        try (I32ColumnWriter w = new I32ColumnWriter(file, false)) {
            w.appendNonNull(Integer.MIN_VALUE);
            w.appendNonNull(-1);
            w.appendNonNull(0);
            w.appendNonNull(1);
            w.appendNonNull(Integer.MAX_VALUE);
        }
        try (I32Column c = I32Column.open(file)) {
            assertEquals(5L, c.rowCount());
            assertEquals(Integer.MIN_VALUE, c.get(0));
            assertEquals(-1, c.get(1));
            assertEquals(0, c.get(2));
            assertEquals(1, c.get(3));
            assertEquals(Integer.MAX_VALUE, c.get(4));
        }
    }

    @Test
    void f64RoundTrip(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("col.bin");
        try (F64ColumnWriter w = new F64ColumnWriter(file, true)) {
            w.appendNonNull(0.0);
            w.appendNonNull(-1.5);
            w.appendNonNull(Math.PI);
            w.appendNull();
            w.appendNonNull(Double.POSITIVE_INFINITY);
            w.appendNonNull(Double.NaN);
        }
        try (F64Column c = F64Column.open(file)) {
            assertEquals(6L, c.rowCount());
            assertEquals(0.0, c.get(0));
            assertEquals(-1.5, c.get(1));
            assertEquals(Math.PI, c.get(2));
            assertTrue(c.isNullAt(3));
            assertEquals(Double.POSITIVE_INFINITY, c.get(4));
            assertTrue(Double.isNaN(c.get(5)));
        }
    }

    @Test
    void wrongTypeOnOpenFails(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("col.bin");
        try (I64ColumnWriter w = new I64ColumnWriter(file, false)) {
            w.appendNonNull(1L);
        }
        assertThrows(IOException.class, () -> I32Column.open(file));
    }
}
