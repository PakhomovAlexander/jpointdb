package io.jpointdb.core.json;

import java.util.ArrayList;

import org.jspecify.annotations.Nullable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal JSON reader/writer sufficient for meta files.
 *
 * <p>
 * Mapping:
 * <ul>
 * <li>object → {@code LinkedHashMap<String, Object>} (preserves insertion
 * order)
 * <li>array → {@code ArrayList<Object>}
 * <li>string → {@code String}
 * <li>integer → {@code Long}
 * <li>decimal / exponent → {@code Double}
 * <li>true / false → {@code Boolean}
 * <li>null → {@code null}
 * </ul>
 * Writer accepts the same types plus {@code Integer} (widened to long-like
 * output) and {@code Float}.
 */
public final class Json {

    private Json() {
    }

    public static @Nullable Object parse(String s) {
        Parser p = new Parser(s);
        p.skipWs();
        Object v = p.readValue();
        p.skipWs();
        if (p.pos != s.length()) {
            throw new JsonException("trailing characters at " + p.pos);
        }
        return v;
    }

    public static String write(@Nullable Object v) {
        StringBuilder sb = new StringBuilder();
        writeValue(v, sb);
        return sb.toString();
    }

    private static void writeValue(@Nullable Object v, StringBuilder sb) {
        if (v == null) {
            sb.append("null");
            return;
        }
        switch (v) {
            case Boolean b -> sb.append(b ? "true" : "false");
            case Long l -> sb.append(l.longValue());
            case Integer i -> sb.append(i.intValue());
            case Short s -> sb.append(s.shortValue());
            case Byte b -> sb.append(b.byteValue());
            case Double d -> sb.append(d.doubleValue());
            case Float f -> sb.append(f.floatValue());
            case String s -> writeString(s, sb);
            case Map<?, ?> m -> writeMap(m, sb);
            case List<?> l -> writeList(l, sb);
            default -> throw new JsonException("unsupported type: " + v.getClass().getName());
        }
    }

    private static void writeString(String s, StringBuilder sb) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }

    private static void writeMap(Map<?, ?> m, StringBuilder sb) {
        sb.append('{');
        boolean first = true;
        for (Map.Entry<?, ?> e : m.entrySet()) {
            if (!first)
                sb.append(',');
            first = false;
            writeString(String.valueOf(e.getKey()), sb);
            sb.append(':');
            writeValue(e.getValue(), sb);
        }
        sb.append('}');
    }

    private static void writeList(List<?> l, StringBuilder sb) {
        sb.append('[');
        boolean first = true;
        for (Object v : l) {
            if (!first)
                sb.append(',');
            first = false;
            writeValue(v, sb);
        }
        sb.append(']');
    }

    private static final class Parser {
        final String s;
        int pos;

        Parser(String s) {
            this.s = s;
        }

        void skipWs() {
            while (pos < s.length()) {
                char c = s.charAt(pos);
                if (c == ' ' || c == '\t' || c == '\n' || c == '\r')
                    pos++;
                else
                    break;
            }
        }

        @Nullable
        Object readValue() {
            skipWs();
            if (pos >= s.length())
                throw new JsonException("unexpected EOF");
            char c = s.charAt(pos);
            return switch (c) {
                case '{' -> readObject();
                case '[' -> readArray();
                case '"' -> readString();
                case 't', 'f' -> readBool();
                case 'n' -> readNull();
                default -> readNumber();
            };
        }

        Map<String, Object> readObject() {
            expect('{');
            Map<String, Object> m = new LinkedHashMap<>();
            skipWs();
            if (pos < s.length() && s.charAt(pos) == '}') {
                pos++;
                return m;
            }
            while (true) {
                skipWs();
                String key = readString();
                skipWs();
                expect(':');
                Object val = readValue();
                m.put(key, val);
                skipWs();
                if (pos >= s.length())
                    throw new JsonException("unexpected EOF in object");
                char c = s.charAt(pos);
                if (c == ',') {
                    pos++;
                    continue;
                }
                if (c == '}') {
                    pos++;
                    return m;
                }
                throw new JsonException("expected , or } at " + pos);
            }
        }

        List<Object> readArray() {
            expect('[');
            List<Object> list = new ArrayList<>();
            skipWs();
            if (pos < s.length() && s.charAt(pos) == ']') {
                pos++;
                return list;
            }
            while (true) {
                Object v = readValue();
                list.add(v);
                skipWs();
                if (pos >= s.length())
                    throw new JsonException("unexpected EOF in array");
                char c = s.charAt(pos);
                if (c == ',') {
                    pos++;
                    continue;
                }
                if (c == ']') {
                    pos++;
                    return list;
                }
                throw new JsonException("expected , or ] at " + pos);
            }
        }

        String readString() {
            expect('"');
            StringBuilder sb = new StringBuilder();
            while (pos < s.length()) {
                char c = s.charAt(pos++);
                if (c == '"')
                    return sb.toString();
                if (c == '\\') {
                    if (pos >= s.length())
                        throw new JsonException("dangling escape");
                    char e = s.charAt(pos++);
                    switch (e) {
                        case '"', '\\', '/' -> sb.append(e);
                        case 'n' -> sb.append('\n');
                        case 'r' -> sb.append('\r');
                        case 't' -> sb.append('\t');
                        case 'b' -> sb.append('\b');
                        case 'f' -> sb.append('\f');
                        case 'u' -> {
                            if (pos + 4 > s.length())
                                throw new JsonException("bad \\u escape");
                            int code = Integer.parseInt(s.substring(pos, pos + 4), 16);
                            sb.append((char) code);
                            pos += 4;
                        }
                        default -> throw new JsonException("unknown escape \\" + e);
                    }
                } else {
                    sb.append(c);
                }
            }
            throw new JsonException("unterminated string");
        }

        Boolean readBool() {
            if (s.startsWith("true", pos)) {
                pos += 4;
                return Boolean.TRUE;
            }
            if (s.startsWith("false", pos)) {
                pos += 5;
                return Boolean.FALSE;
            }
            throw new JsonException("expected true/false at " + pos);
        }

        @Nullable
        Object readNull() {
            if (s.startsWith("null", pos)) {
                pos += 4;
                return null;
            }
            throw new JsonException("expected null at " + pos);
        }

        Object readNumber() {
            int start = pos;
            if (pos < s.length() && (s.charAt(pos) == '-' || s.charAt(pos) == '+'))
                pos++;
            boolean isDouble = false;
            while (pos < s.length()) {
                char c = s.charAt(pos);
                if (c >= '0' && c <= '9') {
                    pos++;
                    continue;
                }
                if (c == '.') {
                    isDouble = true;
                    pos++;
                    continue;
                }
                if (c == 'e' || c == 'E') {
                    isDouble = true;
                    pos++;
                    if (pos < s.length() && (s.charAt(pos) == '+' || s.charAt(pos) == '-'))
                        pos++;
                    continue;
                }
                break;
            }
            String num = s.substring(start, pos);
            if (num.isEmpty() || num.equals("-") || num.equals("+")) {
                throw new JsonException("invalid number at " + start);
            }
            if (isDouble)
                return Double.parseDouble(num);
            return Long.parseLong(num);
        }

        void expect(char c) {
            if (pos >= s.length() || s.charAt(pos) != c) {
                throw new JsonException("expected '" + c + "' at " + pos);
            }
            pos++;
        }
    }

    public static final class JsonException extends RuntimeException {
        public JsonException(String msg) {
            super(msg);
        }
    }
}
