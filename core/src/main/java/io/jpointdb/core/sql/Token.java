package io.jpointdb.core.sql;

/**
 * One lexical token. For {@link TokenType#STRING_LITERAL} the {@code text}
 * holds the already-unescaped content (without surrounding quotes); for all
 * others it is the raw matched source text.
 */
public record Token(TokenType type, String text, int position) {

    public long asLong() {
        return Long.parseLong(text);
    }

    public double asDouble() {
        return Double.parseDouble(text);
    }

    public boolean is(TokenType t) {
        return type == t;
    }
}
