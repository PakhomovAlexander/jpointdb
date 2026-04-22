package io.jpointdb.core.query;

import io.jpointdb.core.convert.TsvConverter;
import io.jpointdb.core.table.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class QueryEngineTest {

    private static Table convertAndOpen(Path dir, String tsv, List<String> names) throws IOException {
        Path src = dir.resolve("in.tsv");
        Files.writeString(src, tsv);
        Path out = dir.resolve("out.jpdb");
        new TsvConverter(src, out, TsvConverter.Options.defaults().withColumnNames(names)).convert();
        return Table.open(out);
    }

    @Test
    void countStar(@TempDir Path dir) throws IOException {
        try (Table t = convertAndOpen(dir, "1\n2\n3\n4\n", List.of("x"))) {
            QueryResult r = QueryEngine.run(t, "SELECT COUNT(*) FROM " + tableName(t));
            assertEquals(1, r.rowCount());
            assertEquals(4L, r.rows().get(0)[0]);
            assertEquals("count_star()", r.columnNames().get(0));
        }
    }

    @Test
    void countWithFilter(@TempDir Path dir) throws IOException {
        try (Table t = convertAndOpen(dir, "0\n1\n2\n0\n3\n", List.of("x"))) {
            QueryResult r = QueryEngine.run(t, "SELECT COUNT(*) FROM " + tableName(t) + " WHERE x <> 0");
            assertEquals(3L, r.rows().get(0)[0]);
        }
    }

    @Test
    void sumAvgAndCountTogether(@TempDir Path dir) throws IOException {
        try (Table t = convertAndOpen(dir, "1\t10.0\n2\t20.0\n3\t30.0\n", List.of("a", "b"))) {
            QueryResult r = QueryEngine.run(t, "SELECT SUM(a), COUNT(*), AVG(b) FROM " + tableName(t));
            assertEquals(1, r.rowCount());
            assertEquals(6L, r.rows().get(0)[0]);
            assertEquals(3L, r.rows().get(0)[1]);
            assertEquals(20.0, (Double) r.rows().get(0)[2], 1e-9);
        }
    }

    @Test
    void minMax(@TempDir Path dir) throws IOException {
        try (Table t = convertAndOpen(dir, "5\n2\n8\n1\n9\n", List.of("x"))) {
            QueryResult r = QueryEngine.run(t, "SELECT MIN(x), MAX(x) FROM " + tableName(t));
            assertEquals(1L, r.rows().get(0)[0]);
            assertEquals(9L, r.rows().get(0)[1]);
        }
    }

    @Test
    void countDistinct(@TempDir Path dir) throws IOException {
        try (Table t = convertAndOpen(dir, "a\nb\na\nc\nb\na\n", List.of("ch"))) {
            QueryResult r = QueryEngine.run(t, "SELECT COUNT(DISTINCT ch) FROM " + tableName(t));
            assertEquals(3L, r.rows().get(0)[0]);
        }
    }

    @Test
    void groupByWithOrderLimitAndCount(@TempDir Path dir) throws IOException {
        try (Table t =
                convertAndOpen(dir, "us\n" + "us\n" + "de\n" + "us\n" + "fr\n" + "de\n" + "us\n", List.of("country"))) {
            QueryResult r = QueryEngine.run(t,
                    "SELECT country, COUNT(*) AS c FROM " + tableName(t) + " GROUP BY country ORDER BY c DESC LIMIT 2");
            assertEquals(2, r.rowCount());
            assertEquals("us", r.rows().get(0)[0]);
            assertEquals(4L, r.rows().get(0)[1]);
            assertEquals("de", r.rows().get(1)[0]);
            assertEquals(2L, r.rows().get(1)[1]);
        }
    }

    @Test
    void havingFiltersGroups(@TempDir Path dir) throws IOException {
        try (Table t = convertAndOpen(dir, "a\na\na\nb\nb\nc\n", List.of("ch"))) {
            QueryResult r = QueryEngine.run(t, "SELECT ch, COUNT(*) AS c FROM " + tableName(t)
                    + " GROUP BY ch HAVING COUNT(*) >= 2 ORDER BY c DESC");
            assertEquals(2, r.rowCount());
            assertEquals("a", r.rows().get(0)[0]);
            assertEquals("b", r.rows().get(1)[0]);
        }
    }

    @Test
    void whereWithLike(@TempDir Path dir) throws IOException {
        try (Table t = convertAndOpen(dir,
                "http://google.com\nhttp://yandex.ru\nhttps://www.google.com/q\nhttps://bing.com\n", List.of("URL"))) {
            QueryResult r = QueryEngine.run(t, "SELECT COUNT(*) FROM " + tableName(t) + " WHERE URL LIKE '%google%'");
            assertEquals(2L, r.rows().get(0)[0]);
        }
    }

    @Test
    void likeShapes(@TempDir Path dir) throws IOException {
        try (Table t = convertAndOpen(dir, "abc\nabcdef\nxyzabc\nexact\nfoo_bar\n", List.of("s"))) {
            String table = tableName(t);
            assertEquals(1L,
                    QueryEngine.run(t, "SELECT COUNT(*) FROM " + table + " WHERE s LIKE 'exact'").rows().get(0)[0]);
            assertEquals(2L,
                    QueryEngine.run(t, "SELECT COUNT(*) FROM " + table + " WHERE s LIKE 'abc%'").rows().get(0)[0]);
            assertEquals(2L,
                    QueryEngine.run(t, "SELECT COUNT(*) FROM " + table + " WHERE s LIKE '%abc'").rows().get(0)[0]);
            assertEquals(3L,
                    QueryEngine.run(t, "SELECT COUNT(*) FROM " + table + " WHERE s LIKE '%abc%'").rows().get(0)[0]);
            assertEquals(1L,
                    QueryEngine.run(t, "SELECT COUNT(*) FROM " + table + " WHERE s LIKE 'foo_bar'").rows().get(0)[0]);
            assertEquals(2L,
                    QueryEngine.run(t, "SELECT COUNT(*) FROM " + table + " WHERE s NOT LIKE '%abc%'").rows().get(0)[0]);
        }
    }

    @Test
    void filterOnInteger(@TempDir Path dir) throws IOException {
        try (Table t = convertAndOpen(dir, "1\n2\n3\n4\n5\n", List.of("x"))) {
            QueryResult r = QueryEngine.run(t, "SELECT COUNT(*) FROM " + tableName(t) + " WHERE x BETWEEN 2 AND 4");
            assertEquals(3L, r.rows().get(0)[0]);
        }
    }

    @Test
    void inList(@TempDir Path dir) throws IOException {
        try (Table t = convertAndOpen(dir, "1\n2\n3\n4\n5\n", List.of("x"))) {
            QueryResult r = QueryEngine.run(t, "SELECT COUNT(*) FROM " + tableName(t) + " WHERE x IN (1, 3, 5)");
            assertEquals(3L, r.rows().get(0)[0]);
        }
    }

    @Test
    void strlenCountsCodepoints(@TempDir Path dir) throws IOException {
        try (Table t = convertAndOpen(dir, "abc\nпривет\n\nx\n", List.of("s"))) {
            QueryResult r = QueryEngine.run(t, "SELECT STRLEN(s) FROM " + tableName(t) + " WHERE s <> '';");
            assertEquals(3, r.rowCount());
            assertEquals(3L, r.rows().get(0)[0]);   // "abc"
            assertEquals(6L, r.rows().get(1)[0]);   // "привет"
            assertEquals(1L, r.rows().get(2)[0]);   // "x"
        }
    }

    @Test
    void strlenInsideAggregate(@TempDir Path dir) throws IOException {
        try (Table t = convertAndOpen(dir, "a\tab\na\tabcd\nb\txy\n", List.of("g", "s"))) {
            QueryResult r = QueryEngine.run(t,
                    "SELECT g, AVG(LENGTH(s)) AS l FROM " + tableName(t) + " GROUP BY g ORDER BY l DESC;");
            assertEquals(2, r.rowCount());
            assertEquals("a", r.rows().get(0)[0]);
            assertEquals(3.0, (Double) r.rows().get(0)[1], 1e-9);  // (2+4)/2
            assertEquals("b", r.rows().get(1)[0]);
            assertEquals(2.0, (Double) r.rows().get(1)[1], 1e-9);
        }
    }

    @Test
    void extractMinuteFromTimestampString(@TempDir Path dir) throws IOException {
        try (Table t = convertAndOpen(dir, "2013-07-14 20:38:47\n2013-07-14 20:39:05\n2013-07-14 20:38:12\n",
                List.of("event"))) {
            QueryResult r = QueryEngine.run(t, "SELECT extract(minute FROM event) AS m, COUNT(*) AS c FROM "
                    + tableName(t) + " GROUP BY m ORDER BY m;");
            assertEquals(2, r.rowCount());
            assertEquals(38L, r.rows().get(0)[0]);
            assertEquals(2L, r.rows().get(0)[1]);
            assertEquals(39L, r.rows().get(1)[0]);
            assertEquals(1L, r.rows().get(1)[1]);
        }
    }

    @Test
    void dateTruncMinuteGroupsByMinuteBuckets(@TempDir Path dir) throws IOException {
        try (Table t = convertAndOpen(dir, "2013-07-14 20:38:47\n2013-07-14 20:38:05\n2013-07-14 20:39:00\n",
                List.of("event"))) {
            QueryResult r = QueryEngine.run(t, "SELECT DATE_TRUNC('minute', event) AS m, COUNT(*) AS c FROM "
                    + tableName(t) + " GROUP BY DATE_TRUNC('minute', event) ORDER BY m;");
            assertEquals(2, r.rowCount());
            assertEquals("2013-07-14 20:38:00", r.rows().get(0)[0]);
            assertEquals(2L, r.rows().get(0)[1]);
            assertEquals("2013-07-14 20:39:00", r.rows().get(1)[0]);
            assertEquals(1L, r.rows().get(1)[1]);
        }
    }

    @Test
    void regexpReplaceExtractsHost(@TempDir Path dir) throws IOException {
        try (Table t = convertAndOpen(dir, "http://www.example.com/a\nhttps://foo.org/x/y\nhttp://example.com/z\n",
                List.of("url"))) {
            QueryResult r = QueryEngine.run(t,
                    "SELECT REGEXP_REPLACE(url, '^https?://(?:www\\.)?([^/]+)/.*$', '\\1') AS host FROM " + tableName(t)
                            + " ORDER BY host;");
            assertEquals(3, r.rowCount());
            assertEquals("example.com", r.rows().get(0)[0]);
            assertEquals("example.com", r.rows().get(1)[0]);
            assertEquals("foo.org", r.rows().get(2)[0]);
        }
    }

    @Test
    void grandTotalOnEmptyTableEmitsZeros(@TempDir Path dir) throws IOException {
        try (Table t = convertAndOpen(dir, "1\n2\n3\n", List.of("x"))) {
            QueryResult r = QueryEngine.run(t, "SELECT COUNT(*), SUM(x) FROM " + tableName(t) + " WHERE x > 100");
            assertEquals(1, r.rowCount());
            assertEquals(0L, r.rows().get(0)[0]);
            assertNull(r.rows().get(0)[1]);
        }
    }

    @Test
    void inlinePrimitiveAggPathQ33Shape(@TempDir Path dir) throws IOException {
        // Exercises the PrimitiveAggMap fast path: two-integer-column GROUP BY,
        // COUNT_STAR + SUM + AVG over non-nullable integer columns, ORDER BY
        // with primitive resolvers, LIMIT.
        StringBuilder tsv = new StringBuilder();
        tsv.append("1\t10\t1\t100\n").append("1\t10\t0\t200\n").append("1\t20\t1\t50\n").append("2\t30\t0\t10\n")
                .append("1\t10\t1\t300\n");
        try (Table t = convertAndOpen(dir, tsv.toString(), List.of("a", "b", "flag", "v"))) {
            QueryResult r = QueryEngine.run(t, "SELECT a, b, COUNT(*) AS c, SUM(flag), AVG(v) FROM " + tableName(t)
                    + " GROUP BY a, b ORDER BY c DESC, a, b LIMIT 10;");
            assertEquals(3, r.rowCount());
            // (1,10) appears 3× → c=3, sum(flag)=2, avg(v)=(100+200+300)/3=200
            assertEquals(1L, r.rows().get(0)[0]);
            assertEquals(10L, r.rows().get(0)[1]);
            assertEquals(3L, r.rows().get(0)[2]);
            assertEquals(2L, r.rows().get(0)[3]);
            assertEquals(200.0, (Double) r.rows().get(0)[4], 1e-9);
            // Remaining two rows each have c=1; sort is by (c DESC, a, b).
            assertEquals(1L, r.rows().get(1)[0]);
            assertEquals(20L, r.rows().get(1)[1]);
            assertEquals(1L, r.rows().get(1)[2]);
            assertEquals(2L, r.rows().get(2)[0]);
            assertEquals(30L, r.rows().get(2)[1]);
        }
    }

    @Test
    void vectorBatchGrandTotalAllKinds(@TempDir Path dir) throws IOException {
        // Grand-total over primitive columns — hits the VectorBatchAgg SIMD path.
        try (Table t =
                convertAndOpen(dir, "1\t10\t1.5\n2\t20\t2.5\n3\t30\t3.5\n4\t40\t4.5\n", List.of("a", "b", "c"))) {
            QueryResult r = QueryEngine.run(t,
                    "SELECT COUNT(*), SUM(a), AVG(a), MIN(a), MAX(a), SUM(b), MIN(b), MAX(b), SUM(c), AVG(c) FROM "
                            + tableName(t));
            assertEquals(1, r.rowCount());
            assertEquals(4L, r.rows().get(0)[0]);   // COUNT(*)
            assertEquals(10L, r.rows().get(0)[1]);  // SUM(a) = 1+2+3+4
            assertEquals(2.5, (Double) r.rows().get(0)[2], 1e-9); // AVG(a)
            assertEquals(1L, r.rows().get(0)[3]);   // MIN(a)
            assertEquals(4L, r.rows().get(0)[4]);   // MAX(a)
            assertEquals(100L, r.rows().get(0)[5]); // SUM(b) = 10+20+30+40
            assertEquals(10L, r.rows().get(0)[6]);  // MIN(b)
            assertEquals(40L, r.rows().get(0)[7]);  // MAX(b)
            assertEquals(12.0, (Double) r.rows().get(0)[8], 1e-9); // SUM(c)
            assertEquals(3.0, (Double) r.rows().get(0)[9], 1e-9);  // AVG(c)
        }
    }

    @Test
    void inlinePrimitiveAvgOverF64(@TempDir Path dir) throws IOException {
        // AVG path over F64 non-nullable column — exercises doubleField route
        // and the AVG_DOUBLE kind in PrimitiveAggMap.
        try (Table t = convertAndOpen(dir, "1\t10.5\n1\t20.5\n2\t5.0\n", List.of("k", "v"))) {
            QueryResult r =
                    QueryEngine.run(t, "SELECT k, COUNT(*), AVG(v) FROM " + tableName(t) + " GROUP BY k ORDER BY k;");
            assertEquals(2, r.rowCount());
            assertEquals(1L, r.rows().get(0)[0]);
            assertEquals(2L, r.rows().get(0)[1]);
            assertEquals(15.5, (Double) r.rows().get(0)[2], 1e-9);
            assertEquals(2L, r.rows().get(1)[0]);
            assertEquals(1L, r.rows().get(1)[1]);
            assertEquals(5.0, (Double) r.rows().get(1)[2], 1e-9);
        }
    }

    @Test
    void inlinePrimitiveSumF64(@TempDir Path dir) throws IOException {
        // SUM(F64) → SUM_DOUBLE in the PrimitiveAggMap layout.
        try (Table t = convertAndOpen(dir, "1\t1.5\n1\t2.5\n2\t3.0\n", List.of("k", "v"))) {
            QueryResult r = QueryEngine.run(t, "SELECT k, SUM(v) FROM " + tableName(t) + " GROUP BY k ORDER BY k;");
            assertEquals(2, r.rowCount());
            assertEquals(4.0, (Double) r.rows().get(0)[1], 1e-9);
            assertEquals(3.0, (Double) r.rows().get(1)[1], 1e-9);
        }
    }

    @Test
    void inlinePrimitiveSingleColumnCount(@TempDir Path dir) throws IOException {
        // width=1 primitive path — GROUP BY single I64 column.
        try (Table t = convertAndOpen(dir, "1\n2\n1\n3\n1\n2\n", List.of("k"))) {
            QueryResult r = QueryEngine.run(t,
                    "SELECT k, COUNT(*) AS c FROM " + tableName(t) + " GROUP BY k ORDER BY c DESC, k LIMIT 3;");
            assertEquals(3, r.rowCount());
            assertEquals(1L, r.rows().get(0)[0]);
            assertEquals(3L, r.rows().get(0)[1]);
            assertEquals(2L, r.rows().get(1)[0]);
            assertEquals(2L, r.rows().get(1)[1]);
            assertEquals(3L, r.rows().get(2)[0]);
            assertEquals(1L, r.rows().get(2)[1]);
        }
    }

    private static String tableName(Table t) {
        String name = t.dir().getFileName().toString();
        return name.endsWith(".jpdb") ? name.substring(0, name.length() - 5) : name;
    }
}
