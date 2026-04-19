package io.jpointdb.cli;

import org.jline.reader.Highlighter;
import org.jline.reader.LineReader;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;

import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Lenient single-line SQL highlighter for JLine's live buffer.
 *
 * <p>
 * Colors: keywords cyan-bold, string literals green, numbers yellow, comments
 * dim gray. Doesn't reuse the core {@code Tokenizer} because that one is strict
 * and throws on partial/invalid input mid-typing.
 */
public final class SqlHighlighter implements Highlighter {

    private static final Set<String> KEYWORDS = Set.of("SELECT", "FROM", "WHERE", "GROUP", "BY", "HAVING", "ORDER",
            "LIMIT", "OFFSET", "AS", "AND", "OR", "NOT", "IN", "LIKE", "IS", "NULL", "DISTINCT", "ASC", "DESC", "CASE",
            "WHEN", "THEN", "ELSE", "END", "BETWEEN", "TRUE", "FALSE", "COUNT", "SUM", "AVG", "MIN", "MAX");

    private static final AttributedStyle KEYWORD = AttributedStyle.DEFAULT.foreground(AttributedStyle.CYAN).bold();
    private static final AttributedStyle STRING = AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN);
    private static final AttributedStyle NUMBER = AttributedStyle.DEFAULT.foreground(AttributedStyle.YELLOW);
    private static final AttributedStyle COMMENT = AttributedStyle.DEFAULT.foreground(AttributedStyle.BRIGHT);
    private static final AttributedStyle OPERATOR = AttributedStyle.DEFAULT.foreground(AttributedStyle.MAGENTA);

    @Override
    public AttributedString highlight(LineReader reader, String buffer) {
        AttributedStringBuilder sb = new AttributedStringBuilder();
        int n = buffer.length();
        int i = 0;
        while (i < n) {
            char c = buffer.charAt(i);
            if (Character.isWhitespace(c)) {
                sb.append(c);
                i++;
                continue;
            }
            // Line comment "-- ..."
            if (c == '-' && i + 1 < n && buffer.charAt(i + 1) == '-') {
                int end = buffer.indexOf('\n', i);
                int stop = end < 0 ? n : end;
                sb.append(buffer.substring(i, stop), COMMENT);
                i = stop;
                continue;
            }
            // Block comment "/* ... */"
            if (c == '/' && i + 1 < n && buffer.charAt(i + 1) == '*') {
                int end = buffer.indexOf("*/", i + 2);
                int stop = end < 0 ? n : end + 2;
                sb.append(buffer.substring(i, stop), COMMENT);
                i = stop;
                continue;
            }
            // String literal 'abc' (doubled '' as escape, partial strings allowed)
            if (c == '\'') {
                int j = i + 1;
                while (j < n) {
                    if (buffer.charAt(j) == '\'') {
                        if (j + 1 < n && buffer.charAt(j + 1) == '\'') {
                            j += 2;
                            continue;
                        }
                        j++;
                        break;
                    }
                    j++;
                }
                sb.append(buffer.substring(i, j), STRING);
                i = j;
                continue;
            }
            // Number (accepts a partial fragment like ".5e", never throws)
            if (isDigit(c) || (c == '.' && i + 1 < n && isDigit(buffer.charAt(i + 1)))) {
                int j = i;
                while (j < n) {
                    char x = buffer.charAt(j);
                    if (isDigit(x) || x == '.') {
                        j++;
                        continue;
                    }
                    if (x == 'e' || x == 'E') {
                        j++;
                        if (j < n && (buffer.charAt(j) == '+' || buffer.charAt(j) == '-'))
                            j++;
                        continue;
                    }
                    break;
                }
                sb.append(buffer.substring(i, j), NUMBER);
                i = j;
                continue;
            }
            // Identifier or keyword
            if (isIdentStart(c)) {
                int j = i;
                while (j < n && isIdentPart(buffer.charAt(j)))
                    j++;
                String word = buffer.substring(i, j);
                if (KEYWORDS.contains(word.toUpperCase(Locale.ROOT))) {
                    sb.append(word, KEYWORD);
                } else {
                    sb.append(word);
                }
                i = j;
                continue;
            }
            // Operators/punctuation
            if (OP_CHARS.indexOf(c) >= 0) {
                sb.append(String.valueOf(c), OPERATOR);
                i++;
                continue;
            }
            sb.append(c);
            i++;
        }
        return sb.toAttributedString();
    }

    @Override
    public void setErrorPattern(Pattern errorPattern) {
        /* unused */ }

    @Override
    public void setErrorIndex(int errorIndex) {
        /* unused */ }

    private static final String OP_CHARS = "=<>!+-*/%,;().";

    private static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }

    private static boolean isIdentStart(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
    }

    private static boolean isIdentPart(char c) {
        return isIdentStart(c) || isDigit(c);
    }
}
