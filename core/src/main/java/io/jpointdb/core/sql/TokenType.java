package io.jpointdb.core.sql;

public enum TokenType {
    // Keywords
    SELECT, FROM, WHERE, GROUP, BY, HAVING, ORDER, LIMIT, OFFSET, AS, AND, OR, NOT, IN, LIKE, IS, NULL, TRUE, FALSE, DISTINCT, ASC, DESC, CASE, WHEN, THEN, ELSE, END, BETWEEN,

    // Literals and identifiers
    IDENT, INT_LITERAL, FLOAT_LITERAL, STRING_LITERAL,

    // Comparison operators
    EQ, NEQ, LT, LE, GT, GE,

    // Arithmetic operators
    PLUS, MINUS, STAR, SLASH, PERCENT,

    // Punctuation
    LPAREN, RPAREN, COMMA, DOT, SEMICOLON,

    // End of input
    EOF
}
