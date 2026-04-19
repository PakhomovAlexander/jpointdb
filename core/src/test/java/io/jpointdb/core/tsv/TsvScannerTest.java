package io.jpointdb.core.tsv;

import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class TsvScannerTest {

    private static TsvScanner scanner(String s) {
        return new TsvScanner(MemorySegment.ofArray(s.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void emptyInputReturnsFalseImmediately() {
        TsvScanner s = scanner("");
        assertFalse(s.nextRecord());
    }

    @Test
    void singleFieldSingleRecord() {
        TsvScanner s = scanner("hello");
        assertTrue(s.nextRecord());
        assertEquals(1, s.fieldCount());
        assertEquals("hello", s.fieldAsString(0));
        assertFalse(s.nextRecord());
    }

    @Test
    void threeFieldsTabSeparated() {
        TsvScanner s = scanner("a\tb\tc");
        assertTrue(s.nextRecord());
        assertEquals(3, s.fieldCount());
        assertEquals("a", s.fieldAsString(0));
        assertEquals("b", s.fieldAsString(1));
        assertEquals("c", s.fieldAsString(2));
    }

    @Test
    void twoRecordsSeparatedByNewline() {
        TsvScanner s = scanner("a\tb\nc\td");
        assertTrue(s.nextRecord());
        assertEquals(2, s.fieldCount());
        assertEquals("a", s.fieldAsString(0));
        assertEquals("b", s.fieldAsString(1));
        assertTrue(s.nextRecord());
        assertEquals(2, s.fieldCount());
        assertEquals("c", s.fieldAsString(0));
        assertEquals("d", s.fieldAsString(1));
        assertFalse(s.nextRecord());
    }

    @Test
    void trailingNewlineDoesNotCreateExtraRecord() {
        TsvScanner s = scanner("a\tb\n");
        assertTrue(s.nextRecord());
        assertEquals(2, s.fieldCount());
        assertFalse(s.nextRecord());
    }

    @Test
    void crlfLineEndingStripsCrFromLastField() {
        TsvScanner s = scanner("a\tb\r\nc\td");
        assertTrue(s.nextRecord());
        assertEquals("a", s.fieldAsString(0));
        assertEquals("b", s.fieldAsString(1));
        assertTrue(s.nextRecord());
        assertEquals("c", s.fieldAsString(0));
        assertEquals("d", s.fieldAsString(1));
    }

    @Test
    void emptyFieldBetweenTabsIsNotNull() {
        TsvScanner s = scanner("a\t\tb");
        assertTrue(s.nextRecord());
        assertEquals(3, s.fieldCount());
        assertEquals("a", s.fieldAsString(0));
        assertEquals("", s.fieldAsString(1));
        assertFalse(s.fieldIsNull(1));
        assertEquals(0, s.fieldLength(1));
        assertEquals("b", s.fieldAsString(2));
    }

    @Test
    void backslashNMarksNull() {
        TsvScanner s = scanner("\\N\ta");
        assertTrue(s.nextRecord());
        assertTrue(s.fieldIsNull(0));
        assertFalse(s.fieldIsNull(1));
        assertEquals("a", s.fieldAsString(1));
    }

    @Test
    void emptyLastFieldOnLineBeforeNewline() {
        TsvScanner s = scanner("a\t\nb\t");
        assertTrue(s.nextRecord());
        assertEquals(2, s.fieldCount());
        assertEquals("a", s.fieldAsString(0));
        assertEquals("", s.fieldAsString(1));
        assertTrue(s.nextRecord());
        assertEquals(2, s.fieldCount());
        assertEquals("b", s.fieldAsString(0));
        assertEquals("", s.fieldAsString(1));
        assertFalse(s.nextRecord());
    }

    @Test
    void justNewlineYieldsOneRecordWithOneEmptyField() {
        TsvScanner s = scanner("\n");
        assertTrue(s.nextRecord());
        assertEquals(1, s.fieldCount());
        assertEquals(0, s.fieldLength(0));
        assertFalse(s.nextRecord());
    }

    @Test
    void manyFieldsExpandsBuffer() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            if (i > 0)
                sb.append('\t');
            sb.append("f").append(i);
        }
        TsvScanner s = scanner(sb.toString());
        assertTrue(s.nextRecord());
        assertEquals(100, s.fieldCount());
        assertEquals("f0", s.fieldAsString(0));
        assertEquals("f99", s.fieldAsString(99));
    }

    @Test
    void fieldOffsetsPointIntoOriginalSegment() {
        TsvScanner s = scanner("abc\tdef");
        assertTrue(s.nextRecord());
        assertEquals(0, s.fieldOffset(0));
        assertEquals(3, s.fieldLength(0));
        assertEquals(4, s.fieldOffset(1));
        assertEquals(3, s.fieldLength(1));
    }
}
