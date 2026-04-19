package io.jpointdb.core.sql;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TokenizerTest {

    private static List<Token> tokenize(String sql) {
        Tokenizer t = new Tokenizer(sql);
        List<Token> out = new ArrayList<>();
        while (true) {
            Token tok = t.next();
            out.add(tok);
            if (tok.type() == TokenType.EOF)
                break;
        }
        return out;
    }

    private static TokenType[] types(String sql) {
        List<Token> toks = tokenize(sql);
        TokenType[] out = new TokenType[toks.size()];
        for (int i = 0; i < toks.size(); i++)
            out[i] = toks.get(i).type();
        return out;
    }

    @Test
    void emptyInput() {
        assertArrayEquals(new TokenType[]{TokenType.EOF}, types(""));
    }

    @Test
    void whitespaceAndCommentsOnly() {
        assertArrayEquals(new TokenType[]{TokenType.EOF}, types(" \t\n-- line\n /* block */ "));
    }

    @Test
    void singleKeyword() {
        assertArrayEquals(new TokenType[]{TokenType.SELECT, TokenType.EOF}, types("SELECT"));
    }

    @Test
    void keywordsAreCaseInsensitive() {
        assertArrayEquals(new TokenType[]{TokenType.SELECT, TokenType.FROM, TokenType.EOF}, types("select from"));
        assertArrayEquals(new TokenType[]{TokenType.SELECT, TokenType.FROM, TokenType.EOF}, types("SeLeCt FrOm"));
    }

    @Test
    void identifierVsKeyword() {
        List<Token> toks = tokenize("SELECT foo FROM t");
        assertEquals(TokenType.SELECT, toks.get(0).type());
        assertEquals(TokenType.IDENT, toks.get(1).type());
        assertEquals("foo", toks.get(1).text());
        assertEquals(TokenType.FROM, toks.get(2).type());
        assertEquals("t", toks.get(3).text());
    }

    @Test
    void quotedIdentifierPreservesCase() {
        List<Token> toks = tokenize("\"SELECT\" \"has space\"");
        assertEquals(TokenType.IDENT, toks.get(0).type());
        assertEquals("SELECT", toks.get(0).text());
        assertEquals(TokenType.IDENT, toks.get(1).type());
        assertEquals("has space", toks.get(1).text());
    }

    @Test
    void integerLiteral() {
        List<Token> toks = tokenize("42");
        assertEquals(TokenType.INT_LITERAL, toks.get(0).type());
        assertEquals(42L, toks.get(0).asLong());
    }

    @Test
    void floatLiteralWithDot() {
        List<Token> toks = tokenize("42.5");
        assertEquals(TokenType.FLOAT_LITERAL, toks.get(0).type());
        assertEquals(42.5, toks.get(0).asDouble());
    }

    @Test
    void floatLiteralStartingWithDot() {
        List<Token> toks = tokenize(".25");
        assertEquals(TokenType.FLOAT_LITERAL, toks.get(0).type());
        assertEquals(0.25, toks.get(0).asDouble());
    }

    @Test
    void scientificNotation() {
        List<Token> toks = tokenize("1.5e10 2E-3");
        assertEquals(TokenType.FLOAT_LITERAL, toks.get(0).type());
        assertEquals(1.5e10, toks.get(0).asDouble());
        assertEquals(TokenType.FLOAT_LITERAL, toks.get(1).type());
        assertEquals(2e-3, toks.get(1).asDouble());
    }

    @Test
    void minusIsSeparateToken() {
        assertArrayEquals(new TokenType[]{TokenType.MINUS, TokenType.INT_LITERAL, TokenType.EOF}, types("-42"));
    }

    @Test
    void stringLiteralBasic() {
        List<Token> toks = tokenize("'hello'");
        assertEquals(TokenType.STRING_LITERAL, toks.get(0).type());
        assertEquals("hello", toks.get(0).text());
    }

    @Test
    void stringLiteralEmpty() {
        List<Token> toks = tokenize("''");
        assertEquals(TokenType.STRING_LITERAL, toks.get(0).type());
        assertEquals("", toks.get(0).text());
    }

    @Test
    void stringLiteralWithDoubledQuoteEscape() {
        List<Token> toks = tokenize("'it''s'");
        assertEquals(TokenType.STRING_LITERAL, toks.get(0).type());
        assertEquals("it's", toks.get(0).text());
    }

    @Test
    void comparisonOperators() {
        assertArrayEquals(new TokenType[]{TokenType.EQ, TokenType.NEQ, TokenType.NEQ, TokenType.LT, TokenType.LE,
                TokenType.GT, TokenType.GE, TokenType.EOF}, types("= <> != < <= > >="));
    }

    @Test
    void arithmeticOperators() {
        assertArrayEquals(new TokenType[]{TokenType.PLUS, TokenType.MINUS, TokenType.STAR, TokenType.SLASH,
                TokenType.PERCENT, TokenType.EOF}, types("+ - * / %"));
    }

    @Test
    void punctuation() {
        assertArrayEquals(new TokenType[]{TokenType.LPAREN, TokenType.RPAREN, TokenType.COMMA, TokenType.DOT,
                TokenType.SEMICOLON, TokenType.EOF}, types("( ) , . ;"));
    }

    @Test
    void lineCommentEndsAtNewline() {
        assertArrayEquals(new TokenType[]{TokenType.SELECT, TokenType.EOF}, types("-- comment\nSELECT"));
    }

    @Test
    void blockCommentSpansLines() {
        assertArrayEquals(new TokenType[]{TokenType.SELECT, TokenType.EOF}, types("/* multi\nline */ SELECT"));
    }

    @Test
    void unterminatedBlockCommentThrows() {
        assertThrows(SqlException.class, () -> tokenize("/* no end"));
    }

    @Test
    void unterminatedStringThrows() {
        assertThrows(SqlException.class, () -> tokenize("'abc"));
    }

    @Test
    void unknownCharacterThrows() {
        assertThrows(SqlException.class, () -> tokenize("@"));
    }

    @Test
    void peekDoesNotAdvance() {
        Tokenizer t = new Tokenizer("SELECT * FROM t");
        assertEquals(TokenType.SELECT, t.peek().type());
        assertEquals(TokenType.SELECT, t.peek().type());
        assertEquals(TokenType.SELECT, t.next().type());
        assertEquals(TokenType.STAR, t.peek().type());
        assertEquals(TokenType.STAR, t.next().type());
    }

    @Test
    void positionTracking() {
        List<Token> toks = tokenize("SELECT x");
        assertEquals(0, toks.get(0).position());
        assertEquals(7, toks.get(1).position());
    }

    @Test
    void fullSelectTokenizes() {
        List<Token> toks =
                tokenize("SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY x ORDER BY y DESC LIMIT 10;");
        TokenType[] expected = {TokenType.SELECT, TokenType.IDENT, TokenType.LPAREN, TokenType.STAR, TokenType.RPAREN,
                TokenType.FROM, TokenType.IDENT, TokenType.WHERE, TokenType.IDENT, TokenType.NEQ, TokenType.INT_LITERAL,
                TokenType.GROUP, TokenType.BY, TokenType.IDENT, TokenType.ORDER, TokenType.BY, TokenType.IDENT,
                TokenType.DESC, TokenType.LIMIT, TokenType.INT_LITERAL, TokenType.SEMICOLON, TokenType.EOF};
        TokenType[] actual = new TokenType[toks.size()];
        for (int i = 0; i < toks.size(); i++)
            actual[i] = toks.get(i).type();
        assertArrayEquals(expected, actual);
    }
}
