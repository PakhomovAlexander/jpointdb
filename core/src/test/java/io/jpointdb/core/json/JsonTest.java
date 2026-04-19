package io.jpointdb.core.json;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonTest {

    @Test
    void emptyObject() {
        assertEquals(Map.of(), Json.parse("{}"));
        assertEquals("{}", Json.write(Map.of()));
    }

    @Test
    void emptyArray() {
        assertEquals(List.of(), Json.parse("[]"));
        assertEquals("[]", Json.write(List.of()));
    }

    @Test
    void parseIntegerAsLong() {
        assertEquals(42L, Json.parse("42"));
        assertEquals(-7L, Json.parse("-7"));
    }

    @Test
    void parseDoubleWhenDotOrExponent() {
        assertEquals(1.5, Json.parse("1.5"));
        assertEquals(1.5e10, Json.parse("1.5e10"));
        assertEquals(-1.5E-3, Json.parse("-1.5E-3"));
    }

    @Test
    void parseString() {
        assertEquals("hello", Json.parse("\"hello\""));
    }

    @Test
    void parseStringWithEscapes() {
        assertEquals("a\nb\t\"c\\d", Json.parse("\"a\\nb\\t\\\"c\\\\d\""));
    }

    @Test
    void parseUnicodeEscape() {
        assertEquals("\u00e9", Json.parse("\"\\u00e9\""));
    }

    @Test
    void parseCyrillic() {
        assertEquals("привет", Json.parse("\"привет\""));
    }

    @Test
    void parseTrueFalseNull() {
        assertEquals(Boolean.TRUE, Json.parse("true"));
        assertEquals(Boolean.FALSE, Json.parse("false"));
        assertNull(Json.parse("null"));
    }

    @Test
    void parseSimpleObject() {
        Map<String, Object> expected = new LinkedHashMap<>();
        expected.put("a", 1L);
        expected.put("b", "two");
        expected.put("c", true);
        assertEquals(expected, Json.parse("{\"a\":1,\"b\":\"two\",\"c\":true}"));
    }

    @Test
    void parseSimpleArray() {
        assertEquals(List.of(1L, 2L, 3L), Json.parse("[1,2,3]"));
    }

    @Test
    void parseNested() {
        Map<String, Object> expected = new LinkedHashMap<>();
        expected.put("name", "t");
        expected.put("rows", List.of(Map.of("a", 1L), Map.of("a", 2L)));
        assertEquals(expected, Json.parse("{\"name\":\"t\",\"rows\":[{\"a\":1},{\"a\":2}]}"));
    }

    @Test
    void whitespaceBetweenTokensIsIgnored() {
        assertEquals(Map.of("a", 1L), Json.parse("  { \"a\" : 1 } "));
    }

    @Test
    void writeObjectPreservesInsertionOrder() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("z", 1L);
        m.put("a", 2L);
        assertEquals("{\"z\":1,\"a\":2}", Json.write(m));
    }

    @Test
    void writeEscapesSpecialChars() {
        assertEquals("\"a\\nb\\\"c\\\\\"", Json.write("a\nb\"c\\"));
    }

    @Test
    void writeControlCharAsUnicodeEscape() {
        assertEquals("\"\\u0001\"", Json.write("\u0001"));
    }

    @Test
    void writeRoundTripsTypedMeta() {
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("version", 1L);
        meta.put("rowCount", 1_000_000L);
        Map<String, Object> col = new LinkedHashMap<>();
        col.put("name", "UserID");
        col.put("type", "I64");
        col.put("nullable", false);
        meta.put("columns", List.of(col));
        String s = Json.write(meta);
        assertEquals(meta, Json.parse(s));
    }

    @Test
    void writeSupportsIntAndDoubleBoxed() {
        assertEquals("42", Json.write(42));
        assertEquals("1.5", Json.write(1.5));
    }

    @Test
    void trailingCharactersAreRejected() {
        assertThrows(Json.JsonException.class, () -> Json.parse("1 2"));
    }

    @Test
    void unterminatedStringThrows() {
        assertThrows(Json.JsonException.class, () -> Json.parse("\"abc"));
    }

    @Test
    void unknownEscapeThrows() {
        assertThrows(Json.JsonException.class, () -> Json.parse("\"\\z\""));
    }
}
