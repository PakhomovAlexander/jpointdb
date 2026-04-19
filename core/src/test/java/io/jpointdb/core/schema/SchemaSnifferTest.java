package io.jpointdb.core.schema;

import io.jpointdb.core.tsv.TsvScanner;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SchemaSnifferTest {

    private static TsvScanner scanner(String s) {
        return new TsvScanner(MemorySegment.ofArray(s.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void twoI32Columns() {
        Schema s = SchemaSniffer.sniff(scanner("1\t2\n3\t4\n"), null);
        assertEquals(2, s.columnCount());
        assertEquals(ColumnType.I32, s.column(0).type());
        assertEquals(ColumnType.I32, s.column(1).type());
        assertFalse(s.column(0).nullable());
        assertFalse(s.column(1).nullable());
    }

    @Test
    void valueOverflowsIntButFitsLong() {
        Schema s = SchemaSniffer.sniff(scanner("1\n2147483648\n"), null);
        assertEquals(ColumnType.I64, s.column(0).type());
    }

    @Test
    void valueOverflowsLongPromotesToString() {
        Schema s = SchemaSniffer.sniff(scanner("99999999999999999999\n"), null);
        assertEquals(ColumnType.STRING, s.column(0).type());
    }

    @Test
    void decimalPromotesToF64() {
        Schema s = SchemaSniffer.sniff(scanner("1\n2.5\n"), null);
        assertEquals(ColumnType.F64, s.column(0).type());
    }

    @Test
    void exponentialFormIsF64() {
        Schema s = SchemaSniffer.sniff(scanner("1.5e10\n"), null);
        assertEquals(ColumnType.F64, s.column(0).type());
    }

    @Test
    void mixedNumericAndTextIsString() {
        Schema s = SchemaSniffer.sniff(scanner("1\nhello\n"), null);
        assertEquals(ColumnType.STRING, s.column(0).type());
    }

    @Test
    void allEmptyIsString() {
        Schema s = SchemaSniffer.sniff(scanner("\n\n"), null);
        assertEquals(ColumnType.STRING, s.column(0).type());
        assertTrue(s.column(0).nullable());
    }

    @Test
    void nullMarkerMakesColumnNullable() {
        Schema s = SchemaSniffer.sniff(scanner("\\N\n1\n"), null);
        assertEquals(ColumnType.I32, s.column(0).type());
        assertTrue(s.column(0).nullable());
    }

    @Test
    void emptyFieldMakesColumnNullable() {
        Schema s = SchemaSniffer.sniff(scanner("1\n\n2\n"), null);
        assertEquals(ColumnType.I32, s.column(0).type());
        assertTrue(s.column(0).nullable());
    }

    @Test
    void providedColumnNamesAreUsed() {
        Schema s = SchemaSniffer.sniff(scanner("1\t2\n"), List.of("id", "count"));
        assertEquals("id", s.column(0).name());
        assertEquals("count", s.column(1).name());
    }

    @Test
    void defaultColumnNamesAreColN() {
        Schema s = SchemaSniffer.sniff(scanner("1\t2\t3\n"), null);
        assertEquals("col0", s.column(0).name());
        assertEquals("col1", s.column(1).name());
        assertEquals("col2", s.column(2).name());
    }

    @Test
    void negativeIntegersRecognized() {
        Schema s = SchemaSniffer.sniff(scanner("-1\n-100\n"), null);
        assertEquals(ColumnType.I32, s.column(0).type());
    }

    @Test
    void minLongFitsI64() {
        Schema s = SchemaSniffer.sniff(scanner("-9223372036854775808\n1\n"), null);
        assertEquals(ColumnType.I64, s.column(0).type());
    }

    @Test
    void onlySignIsString() {
        Schema s = SchemaSniffer.sniff(scanner("-\n1\n"), null);
        assertEquals(ColumnType.STRING, s.column(0).type());
    }

    @Test
    void leadingPlusIsAccepted() {
        Schema s = SchemaSniffer.sniff(scanner("+1\n+2\n"), null);
        assertEquals(ColumnType.I32, s.column(0).type());
    }

    @Test
    void floatWithoutIntegerPartIsF64() {
        Schema s = SchemaSniffer.sniff(scanner(".5\n1.5\n"), null);
        assertEquals(ColumnType.F64, s.column(0).type());
    }

    @Test
    void floatWithoutFractionalPartIsF64() {
        Schema s = SchemaSniffer.sniff(scanner("1.\n2.\n"), null);
        assertEquals(ColumnType.F64, s.column(0).type());
    }
}
