package io.jpointdb.core.sql;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Hand-written SQL lexer. Keywords are case-insensitive; identifiers preserve
 * the case of their source text (unless double-quoted).
 *
 * <p>
 * Supported forms:
 * <ul>
 * <li>Keywords from {@link TokenType}.</li>
 * <li>Numeric literals: {@code 42}, {@code 42.5}, {@code .5}, {@code 1e10},
 * {@code 1.5E-3}.</li>
 * <li>Single-quoted strings with {@code ''} as an escaped quote:
 * {@code 'it''s'}.</li>
 * <li>Identifiers: starts with letter or {@code _}, then
 * alphanumerics/underscore.</li>
 * <li>Double-quoted identifiers: {@code "foo bar"}.</li>
 * <li>Comments: {@code -- line ...}\n and {@code /* block *}{@code /}.</li>
 * <li>Operators: {@code = <> != < <= > >= + - * / %}.</li>
 * </ul>
 */
public final class Tokenizer {

    private static final Map<String, TokenType> KEYWORDS = buildKeywords();

    private final String sql;
    private int pos;
    @Nullable
    private Token peeked;

    public Tokenizer(String sql) {
        this.sql = sql;
    }

    public Token next() {
        if (peeked != null) {
            Token t = peeked;
            peeked = null;
            return t;
        }
        return readToken();
    }

    public Token peek() {
        if (peeked == null) {
            peeked = readToken();
        }
        return peeked;
    }

    public boolean isEof() {
        return peek().type() == TokenType.EOF;
    }

    private Token readToken() {
        skipWhitespaceAndComments();
        if (pos >= sql.length()) {
            return new Token(TokenType.EOF, "", pos);
        }
        int start = pos;
        char c = sql.charAt(pos);
        if (isDigit(c))
            return readNumber(start);
        if (c == '.' && pos + 1 < sql.length() && isDigit(sql.charAt(pos + 1)))
            return readNumber(start);
        if (c == '\'')
            return readSingleQuotedString(start);
        if (c == '"')
            return readDoubleQuotedIdent(start);
        if (isIdentStart(c))
            return readIdentOrKeyword(start);
        return readOperatorOrPunct(start);
    }

    private void skipWhitespaceAndComments() {
        while (pos < sql.length()) {
            char c = sql.charAt(pos);
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                pos++;
                continue;
            }
            if (c == '-' && pos + 1 < sql.length() && sql.charAt(pos + 1) == '-') {
                while (pos < sql.length() && sql.charAt(pos) != '\n')
                    pos++;
                continue;
            }
            if (c == '/' && pos + 1 < sql.length() && sql.charAt(pos + 1) == '*') {
                pos += 2;
                while (pos + 1 < sql.length() && !(sql.charAt(pos) == '*' && sql.charAt(pos + 1) == '/')) {
                    pos++;
                }
                if (pos + 1 >= sql.length())
                    throw new SqlException("unterminated block comment", pos);
                pos += 2;
                continue;
            }
            break;
        }
    }

    private Token readNumber(int start) {
        boolean isFloat = false;
        while (pos < sql.length() && isDigit(sql.charAt(pos)))
            pos++;
        if (pos < sql.length() && sql.charAt(pos) == '.') {
            isFloat = true;
            pos++;
            while (pos < sql.length() && isDigit(sql.charAt(pos)))
                pos++;
        }
        if (pos < sql.length() && (sql.charAt(pos) == 'e' || sql.charAt(pos) == 'E')) {
            isFloat = true;
            pos++;
            if (pos < sql.length() && (sql.charAt(pos) == '+' || sql.charAt(pos) == '-'))
                pos++;
            int expStart = pos;
            while (pos < sql.length() && isDigit(sql.charAt(pos)))
                pos++;
            if (pos == expStart)
                throw new SqlException("empty exponent", start);
        }
        String text = sql.substring(start, pos);
        return new Token(isFloat ? TokenType.FLOAT_LITERAL : TokenType.INT_LITERAL, text, start);
    }

    private Token readSingleQuotedString(int start) {
        pos++;
        StringBuilder sb = new StringBuilder();
        while (pos < sql.length()) {
            char c = sql.charAt(pos);
            if (c == '\'') {
                if (pos + 1 < sql.length() && sql.charAt(pos + 1) == '\'') {
                    sb.append('\'');
                    pos += 2;
                    continue;
                }
                pos++;
                return new Token(TokenType.STRING_LITERAL, sb.toString(), start);
            }
            sb.append(c);
            pos++;
        }
        throw new SqlException("unterminated string literal", start);
    }

    private Token readDoubleQuotedIdent(int start) {
        pos++;
        StringBuilder sb = new StringBuilder();
        while (pos < sql.length()) {
            char c = sql.charAt(pos);
            if (c == '"') {
                if (pos + 1 < sql.length() && sql.charAt(pos + 1) == '"') {
                    sb.append('"');
                    pos += 2;
                    continue;
                }
                pos++;
                return new Token(TokenType.IDENT, sb.toString(), start);
            }
            sb.append(c);
            pos++;
        }
        throw new SqlException("unterminated quoted identifier", start);
    }

    private Token readIdentOrKeyword(int start) {
        while (pos < sql.length() && isIdentPart(sql.charAt(pos)))
            pos++;
        String text = sql.substring(start, pos);
        TokenType kw = KEYWORDS.get(text.toUpperCase(Locale.ROOT));
        if (kw != null)
            return new Token(kw, text, start);
        return new Token(TokenType.IDENT, text, start);
    }

    private Token readOperatorOrPunct(int start) {
        char c = sql.charAt(pos);
        switch (c) {
            case '(' :
                pos++;
                return new Token(TokenType.LPAREN, "(", start);
            case ')' :
                pos++;
                return new Token(TokenType.RPAREN, ")", start);
            case ',' :
                pos++;
                return new Token(TokenType.COMMA, ",", start);
            case '.' :
                pos++;
                return new Token(TokenType.DOT, ".", start);
            case ';' :
                pos++;
                return new Token(TokenType.SEMICOLON, ";", start);
            case '+' :
                pos++;
                return new Token(TokenType.PLUS, "+", start);
            case '-' :
                pos++;
                return new Token(TokenType.MINUS, "-", start);
            case '*' :
                pos++;
                return new Token(TokenType.STAR, "*", start);
            case '/' :
                pos++;
                return new Token(TokenType.SLASH, "/", start);
            case '%' :
                pos++;
                return new Token(TokenType.PERCENT, "%", start);
            case '=' :
                pos++;
                return new Token(TokenType.EQ, "=", start);
            case '<' :
                pos++;
                if (pos < sql.length() && sql.charAt(pos) == '=') {
                    pos++;
                    return new Token(TokenType.LE, "<=", start);
                }
                if (pos < sql.length() && sql.charAt(pos) == '>') {
                    pos++;
                    return new Token(TokenType.NEQ, "<>", start);
                }
                return new Token(TokenType.LT, "<", start);
            case '>' :
                pos++;
                if (pos < sql.length() && sql.charAt(pos) == '=') {
                    pos++;
                    return new Token(TokenType.GE, ">=", start);
                }
                return new Token(TokenType.GT, ">", start);
            case '!' :
                pos++;
                if (pos < sql.length() && sql.charAt(pos) == '=') {
                    pos++;
                    return new Token(TokenType.NEQ, "!=", start);
                }
                throw new SqlException("unexpected '!'", start);
            default :
                throw new SqlException("unexpected character '" + c + "'", start);
        }
    }

    private static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }

    private static boolean isIdentStart(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
    }

    private static boolean isIdentPart(char c) {
        return isIdentStart(c) || isDigit(c);
    }

    private static Map<String, TokenType> buildKeywords() {
        Map<String, TokenType> m = new HashMap<>();
        for (TokenType t : TokenType.values()) {
            switch (t) {
                case IDENT, INT_LITERAL, FLOAT_LITERAL, STRING_LITERAL, EQ, NEQ, LT, LE, GT, GE, PLUS, MINUS, STAR,
                        SLASH, PERCENT, LPAREN, RPAREN, COMMA, DOT, SEMICOLON, EOF -> {
                    // skip — not keywords
                }
                default -> m.put(t.name(), t);
            }
        }
        return m;
    }
}
