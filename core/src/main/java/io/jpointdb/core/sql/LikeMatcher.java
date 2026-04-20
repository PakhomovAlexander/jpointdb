package io.jpointdb.core.sql;

import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Compiled matcher for a SQL LIKE pattern. Built once per distinct pattern
 * string and reused across rows.
 *
 * <p>For literal-only patterns ({@code literal}, {@code literal%},
 * {@code %literal}, {@code %literal%}, with no {@code _} wildcards and no
 * embedded {@code %}), the matcher reduces to {@link String#equals},
 * {@link String#startsWith}, {@link String#endsWith}, or
 * {@link String#contains} — each lowered by HotSpot to a byte-level SIMD
 * intrinsic on Latin-1 strings. Everything else compiles to a regex once.
 */
@FunctionalInterface
public interface LikeMatcher {

    boolean matches(String s);

    ConcurrentHashMap<String, LikeMatcher> CACHE = new ConcurrentHashMap<>();

    static LikeMatcher forPattern(String pat) {
        LikeMatcher m = CACHE.get(pat);
        if (m != null)
            return m;
        return CACHE.computeIfAbsent(pat, LikeMatcher::build);
    }

    static LikeMatcher build(String pat) {
        int len = pat.length();
        boolean hasPct = false;
        boolean hasUnderscore = false;
        for (int i = 0; i < len; i++) {
            char c = pat.charAt(i);
            if (c == '%')
                hasPct = true;
            else if (c == '_')
                hasUnderscore = true;
        }
        if (!hasPct && !hasUnderscore) {
            return s -> s.equals(pat);
        }
        if (!hasUnderscore) {
            int first = pat.indexOf('%');
            int last = pat.lastIndexOf('%');
            if (first == 0 && last == len - 1) {
                String mid = pat.substring(1, len - 1);
                if (mid.indexOf('%') < 0) {
                    if (mid.isEmpty())
                        return s -> true;
                    return s -> s.contains(mid);
                }
            } else if (first == 0 && last == 0) {
                String tail = pat.substring(1);
                return s -> s.endsWith(tail);
            } else if (first == len - 1 && last == len - 1) {
                String head = pat.substring(0, len - 1);
                return s -> s.startsWith(head);
            }
        }
        Pattern p = Pattern.compile(likeToRegex(pat), Pattern.DOTALL);
        return s -> p.matcher(s).matches();
    }

    /**
     * Translate a SQL LIKE pattern into a Java regex: {@code %} → {@code .*},
     * {@code _} → {@code .}, regex metacharacters escaped.
     */
    static String likeToRegex(String pattern) {
        StringBuilder sb = new StringBuilder(pattern.length() + 8);
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            switch (c) {
                case '%' -> sb.append(".*");
                case '_' -> sb.append('.');
                case '.', '\\', '+', '*', '?', '(', ')', '[', ']', '{', '}', '^', '$', '|' ->
                        sb.append('\\').append(c);
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }
}
