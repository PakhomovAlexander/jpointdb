package io.jpointdb.core.query;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ScalarFnsTest {

    @Test
    void extractAllFieldsFromTimestamp() {
        String ts = "2013-07-14 20:38:47";
        assertEquals(2013L, ScalarFns.extract("year", ts));
        assertEquals(7L, ScalarFns.extract("month", ts));
        assertEquals(14L, ScalarFns.extract("day", ts));
        assertEquals(20L, ScalarFns.extract("hour", ts));
        assertEquals(38L, ScalarFns.extract("minute", ts));
        assertEquals(47L, ScalarFns.extract("second", ts));
    }

    @Test
    void extractFromDateOnly() {
        String d = "2013-07-14";
        assertEquals(2013L, ScalarFns.extract("year", d));
        assertEquals(7L, ScalarFns.extract("month", d));
        assertEquals(14L, ScalarFns.extract("day", d));
        // Time fields default to 0 for date-only strings.
        assertEquals(0L, ScalarFns.extract("hour", d));
        assertEquals(0L, ScalarFns.extract("minute", d));
    }

    @Test
    void extractReturnsNullOnGarbage() {
        assertNull(ScalarFns.extract("year", "not-a-date"));
    }

    @Test
    void dateTruncMinute() {
        assertEquals("2013-07-14 20:38:00", ScalarFns.dateTrunc("minute", "2013-07-14 20:38:47"));
    }

    @Test
    void dateTruncHour() {
        assertEquals("2013-07-14 20:00:00", ScalarFns.dateTrunc("hour", "2013-07-14 20:38:47"));
    }

    @Test
    void dateTruncDay() {
        assertEquals("2013-07-14 00:00:00", ScalarFns.dateTrunc("day", "2013-07-14 20:38:47"));
    }

    @Test
    void dateTruncMonth() {
        assertEquals("2013-07-01 00:00:00", ScalarFns.dateTrunc("month", "2013-07-14 20:38:47"));
    }

    @Test
    void dateTruncYear() {
        assertEquals("2013-01-01 00:00:00", ScalarFns.dateTrunc("year", "2013-07-14 20:38:47"));
    }

    @Test
    void dateTruncExtendsDateToTimestamp() {
        // Input is "YYYY-MM-DD" — DATE_TRUNC still returns the timestamp form.
        assertEquals("2013-07-14 00:00:00", ScalarFns.dateTrunc("day", "2013-07-14"));
    }

    @Test
    void regexpReplaceCaptureGroup() {
        // Extract host from URL — the Q28 pattern.
        String pattern = "^https?://(?:www\\.)?([^/]+)/.*$";
        assertEquals("example.com", ScalarFns.regexpReplace("http://www.example.com/path/to/page", pattern, "\\1"));
        assertEquals("foo.org", ScalarFns.regexpReplace("https://foo.org/x", pattern, "\\1"));
    }

    @Test
    void regexpReplaceNoMatchReturnsOriginal() {
        assertEquals("just text", ScalarFns.regexpReplace("just text", "[0-9]+", "X"));
    }

    @Test
    void regexpReplaceMultipleOccurrences() {
        assertEquals("X-X-X", ScalarFns.regexpReplace("a-b-c", "[a-z]", "X"));
    }

    @Test
    void translateReplacementBackrefsAndDollars() {
        assertEquals("$1 $2", ScalarFns.translateReplacement("\\1 \\2"));
        assertEquals("\\$5", ScalarFns.translateReplacement("$5"));
        assertEquals("\\\\", ScalarFns.translateReplacement("\\\\"));
    }
}
