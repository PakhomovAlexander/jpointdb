package io.jpointdb.core.query;

import io.jpointdb.core.sql.SqlException;
import org.jspecify.annotations.Nullable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Implementations of SQL scalar functions over the string representations
 * JPointDB uses for dates / timestamps (ISO format).
 */
final class ScalarFns {

    private ScalarFns() {
    }

    /**
     * Extract a field from {@code "YYYY-MM-DD"} or {@code "YYYY-MM-DD HH:MM:SS"}.
     */
    static @Nullable Long extract(String field, @Nullable String ts) {
        if (ts == null)
            return null;
        try {
            return switch (field) {
                case "year" -> parsePart(ts, 0, 4);
                case "month" -> parsePart(ts, 5, 7);
                case "day", "dayofmonth" -> parsePart(ts, 8, 10);
                case "hour" -> ts.length() >= 13 ? parsePart(ts, 11, 13) : 0L;
                case "minute" -> ts.length() >= 16 ? parsePart(ts, 14, 16) : 0L;
                case "second" -> ts.length() >= 19 ? parsePart(ts, 17, 19) : 0L;
                default -> throw new SqlException("unsupported EXTRACT field: " + field, 0);
            };
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static long parsePart(String s, int from, int to) {
        if (s.length() < to)
            throw new NumberFormatException("string too short");
        return Long.parseLong(s.substring(from, to));
    }

    /**
     * Truncate an ISO timestamp string to the given precision. Returns a timestamp
     * string with the lower-precision fields zeroed out, matching DuckDB's CSV
     * rendering of the resulting {@code TIMESTAMP}.
     */
    static @Nullable String dateTrunc(String precision, @Nullable String ts) {
        if (ts == null)
            return null;
        String normalized = ts.length() == 10 ? ts + " 00:00:00" : ts;
        if (normalized.length() < 19)
            return null;
        return switch (precision) {
            case "year" -> normalized.substring(0, 4) + "-01-01 00:00:00";
            case "month" -> normalized.substring(0, 7) + "-01 00:00:00";
            case "day" -> normalized.substring(0, 10) + " 00:00:00";
            case "hour" -> normalized.substring(0, 13) + ":00:00";
            case "minute" -> normalized.substring(0, 16) + ":00";
            case "second" -> normalized.substring(0, 19);
            default -> throw new SqlException("unsupported DATE_TRUNC precision: " + precision, 0);
        };
    }

    private static final ConcurrentHashMap<String, Pattern> PATTERN_CACHE = new ConcurrentHashMap<>();

    /**
     * DuckDB-style REGEXP_REPLACE: all matches are replaced. Backreferences in the
     * replacement use {@code \1} (PCRE/POSIX); we translate them to Java's
     * {@code $1} form before delegating.
     */
    static String regexpReplace(String input, String pattern, String replacement) {
        Pattern p = PATTERN_CACHE.computeIfAbsent(pattern, Pattern::compile);
        return p.matcher(input).replaceAll(translateReplacement(replacement));
    }

    /**
     * {@code \1..\9} → {@code $1..$9}; literal {@code $} is escaped; {@code \\}
     * stays as backslash.
     */
    static String translateReplacement(String repl) {
        StringBuilder sb = new StringBuilder(repl.length() + 8);
        int i = 0;
        while (i < repl.length()) {
            char c = repl.charAt(i);
            if (c == '\\' && i + 1 < repl.length()) {
                char next = repl.charAt(i + 1);
                if (next >= '0' && next <= '9') {
                    sb.append('$').append(next);
                    i += 2;
                    continue;
                }
                if (next == '\\') {
                    sb.append('\\').append('\\');
                    i += 2;
                    continue;
                }
                // Unknown escape — keep literal.
                sb.append(c).append(next);
                i += 2;
                continue;
            }
            if (c == '$') {
                sb.append('\\').append('$');
                i++;
                continue;
            }
            sb.append(c);
            i++;
        }
        return sb.toString();
    }
}
