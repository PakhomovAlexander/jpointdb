package io.jpointdb.core.sql;

import io.jpointdb.core.sql.SqlAst.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SqlParserTest {

    private static Select parse(String sql) {
        return SqlParser.parseSelect(sql);
    }

    @Test
    void countStar() {
        Select s = parse("SELECT COUNT(*) FROM hits");
        assertEquals(1, s.items().size());
        assertEquals("hits", s.fromTable());
        Expr e = s.items().get(0).expr();
        assertInstanceOf(FunctionCall.class, e);
        FunctionCall f = (FunctionCall) e;
        assertEquals("count", f.name());
        assertFalse(f.distinct());
        assertEquals(1, f.args().size());
        assertInstanceOf(Star.class, f.args().get(0));
    }

    @Test
    void countDistinct() {
        Select s = parse("SELECT COUNT(DISTINCT UserID) FROM hits");
        FunctionCall f = (FunctionCall) s.items().get(0).expr();
        assertEquals("count", f.name());
        assertTrue(f.distinct());
        assertInstanceOf(ColumnRef.class, f.args().get(0));
        assertEquals("UserID", ((ColumnRef) f.args().get(0)).name());
    }

    @Test
    void selectStar() {
        Select s = parse("SELECT * FROM t");
        assertInstanceOf(Star.class, s.items().get(0).expr());
    }

    @Test
    void selectMultipleColumnsWithAliases() {
        Select s = parse("SELECT a, b AS beta, c gamma FROM t");
        assertEquals(3, s.items().size());
        assertNull(s.items().get(0).alias());
        assertEquals("beta", s.items().get(1).alias());
        assertEquals("gamma", s.items().get(2).alias());
    }

    @Test
    void whereClauseWithComparison() {
        Select s = parse("SELECT x FROM t WHERE AdvEngineID <> 0");
        Binary w = (Binary) s.where();
        assertEquals(BinaryOp.NEQ, w.op());
        assertEquals("AdvEngineID", ((ColumnRef) w.left()).name());
        assertEquals(0L, ((Literal) w.right()).value());
    }

    @Test
    void whereWithAndOr() {
        Select s = parse("SELECT x FROM t WHERE a = 1 AND b > 2 OR c IS NULL");
        // OR (lowest): AND (...) OR IS NULL
        Binary root = (Binary) s.where();
        assertEquals(BinaryOp.OR, root.op());
        Binary and = (Binary) root.left();
        assertEquals(BinaryOp.AND, and.op());
        assertInstanceOf(IsNull.class, root.right());
    }

    @Test
    void arithmeticPrecedence() {
        Select s = parse("SELECT a + b * c FROM t");
        Binary plus = (Binary) s.items().get(0).expr();
        assertEquals(BinaryOp.PLUS, plus.op());
        assertInstanceOf(ColumnRef.class, plus.left());
        Binary mul = (Binary) plus.right();
        assertEquals(BinaryOp.MUL, mul.op());
    }

    @Test
    void parenthesesOverridePrecedence() {
        Select s = parse("SELECT (a + b) * c FROM t");
        Binary mul = (Binary) s.items().get(0).expr();
        assertEquals(BinaryOp.MUL, mul.op());
        Binary plus = (Binary) mul.left();
        assertEquals(BinaryOp.PLUS, plus.op());
    }

    @Test
    void unaryMinus() {
        Select s = parse("SELECT -42 FROM t");
        Unary u = (Unary) s.items().get(0).expr();
        assertEquals(UnaryOp.NEG, u.op());
        assertEquals(42L, ((Literal) u.operand()).value());
    }

    @Test
    void stringLiteral() {
        Select s = parse("SELECT 'hello' FROM t");
        Literal l = (Literal) s.items().get(0).expr();
        assertEquals(LiteralKind.STRING, l.kind());
        assertEquals("hello", l.value());
    }

    @Test
    void floatLiteral() {
        Select s = parse("SELECT 1.5 FROM t");
        Literal l = (Literal) s.items().get(0).expr();
        assertEquals(LiteralKind.FLOAT, l.kind());
        assertEquals(1.5, l.value());
    }

    @Test
    void likePattern() {
        Select s = parse("SELECT x FROM t WHERE URL LIKE '%google%'");
        Like l = (Like) s.where();
        assertFalse(l.negated());
        assertEquals("URL", ((ColumnRef) l.value()).name());
        assertEquals("%google%", ((Literal) l.pattern()).value());
    }

    @Test
    void notLikePattern() {
        Select s = parse("SELECT x FROM t WHERE URL NOT LIKE '%google%'");
        Like l = (Like) s.where();
        assertTrue(l.negated());
    }

    @Test
    void inList() {
        Select s = parse("SELECT x FROM t WHERE x IN (1, 2, 3)");
        InList il = (InList) s.where();
        assertFalse(il.negated());
        assertEquals(3, il.items().size());
    }

    @Test
    void notInList() {
        Select s = parse("SELECT x FROM t WHERE x NOT IN (1, 2)");
        InList il = (InList) s.where();
        assertTrue(il.negated());
    }

    @Test
    void isNullAndIsNotNull() {
        Select s1 = parse("SELECT x FROM t WHERE x IS NULL");
        IsNull n1 = (IsNull) s1.where();
        assertFalse(n1.negated());

        Select s2 = parse("SELECT x FROM t WHERE x IS NOT NULL");
        IsNull n2 = (IsNull) s2.where();
        assertTrue(n2.negated());
    }

    @Test
    void groupByOrderByLimit() {
        Select s = parse("SELECT a, COUNT(*) AS c FROM t WHERE b > 0 GROUP BY a ORDER BY c DESC LIMIT 10");
        assertEquals(1, s.groupBy().size());
        assertEquals("a", ((ColumnRef) s.groupBy().get(0)).name());
        assertEquals(1, s.orderBy().size());
        assertEquals(SortDirection.DESC, s.orderBy().get(0).direction());
        assertEquals(10L, s.limit());
        assertNull(s.offset());
    }

    @Test
    void limitWithOffset() {
        Select s = parse("SELECT x FROM t LIMIT 10 OFFSET 100");
        assertEquals(10L, s.limit());
        assertEquals(100L, s.offset());
    }

    @Test
    void havingAfterGroupBy() {
        Select s = parse("SELECT x, COUNT(*) FROM t GROUP BY x HAVING COUNT(*) > 100");
        assertNotNull(s.having());
        Binary h = (Binary) s.having();
        assertEquals(BinaryOp.GT, h.op());
    }

    @Test
    void betweenDesugarsToAndChain() {
        Select s = parse("SELECT x FROM t WHERE x BETWEEN 1 AND 10");
        Binary and = (Binary) s.where();
        assertEquals(BinaryOp.AND, and.op());
        Binary ge = (Binary) and.left();
        assertEquals(BinaryOp.GE, ge.op());
        Binary le = (Binary) and.right();
        assertEquals(BinaryOp.LE, le.op());
    }

    @Test
    void caseExpression() {
        Select s = parse("SELECT CASE WHEN a = 1 THEN 'one' WHEN a = 2 THEN 'two' ELSE 'other' END FROM t");
        CaseExpr c = (CaseExpr) s.items().get(0).expr();
        assertEquals(2, c.whens().size());
        assertEquals("other", ((Literal) c.elseExpr()).value());
    }

    @Test
    void functionCallWithMultipleArgs() {
        Select s = parse("SELECT REGEXP_REPLACE(x, 'a', 'b') FROM t");
        FunctionCall f = (FunctionCall) s.items().get(0).expr();
        assertEquals("regexp_replace", f.name());
        assertEquals(3, f.args().size());
    }

    @Test
    void trailingSemicolonAllowed() {
        Select s = parse("SELECT 1 FROM t;");
        assertEquals(1, s.items().size());
    }

    @Test
    void clickBenchQ02Parses() {
        Select s = parse("SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0");
        assertEquals("hits", s.fromTable());
        assertNotNull(s.where());
    }

    @Test
    void clickBenchQ09Parses() {
        Select s = parse(
                "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10");
        assertEquals(2, s.items().size());
        assertEquals("u", s.items().get(1).alias());
        assertEquals(1, s.orderBy().size());
        assertEquals(10L, s.limit());
    }

    @Test
    void clickBenchQ28Parses() {
        Select s = parse("SELECT CounterID, AVG(STRLEN(URL)) AS l, COUNT(*) AS c "
                + "FROM hits WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 "
                + "ORDER BY l DESC LIMIT 25");
        assertEquals("CounterID", ((ColumnRef) s.items().get(0).expr()).name());
        assertEquals("l", s.items().get(1).alias());
        assertNotNull(s.having());
        assertEquals(25L, s.limit());
    }

    @Test
    void malformedInputThrows() {
        assertThrows(SqlException.class, () -> parse("SELECT FROM t"));
        assertThrows(SqlException.class, () -> parse("SELECT x"));
        assertThrows(SqlException.class, () -> parse("SELECT x FROM"));
    }
}
