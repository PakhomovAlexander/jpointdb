package io.jpointdb.cli;

import io.jpointdb.core.json.Json;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultParser;
import org.jline.reader.impl.completer.StringsCompleter;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.InfoCmp;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jspecify.annotations.Nullable;

/**
 * REPL / one-shot CLI for JPointDB.
 *
 * <pre>
 *   ./cli.sh                         # REPL with history / completion / highlighting
 *   ./cli.sh -c "SELECT COUNT(*) FROM hits;"
 *   ./cli.sh --url http://host:port
 * </pre>
 *
 * <p>
 * History is persisted at {@code ~/.jpointdb_history} (via JLine). Completion
 * candidates combine SQL keywords with column names fetched from the server's
 * {@code /schema} endpoint at startup.
 */
public final class Main {

    private static final List<String> SQL_KEYWORDS =
            List.of("SELECT", "FROM", "WHERE", "GROUP BY", "HAVING", "ORDER BY", "LIMIT", "OFFSET", "AS", "AND", "OR",
                    "NOT", "IN", "LIKE", "IS NULL", "IS NOT NULL", "DISTINCT", "ASC", "DESC", "CASE", "WHEN", "THEN",
                    "ELSE", "END", "BETWEEN", "COUNT(*)", "COUNT(", "SUM(", "AVG(", "MIN(", "MAX(");

    public static void main(String[] args) throws Exception {
        String url = "http://localhost:8080";
        String oneShot = null;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--url" -> url = args[++i];
                case "-c" -> oneShot = args[++i];
                case "--help", "-h" -> {
                    usage();
                    return;
                }
                default -> {
                    System.err.println("unexpected: " + args[i]);
                    usage();
                    System.exit(2);
                }
            }
        }

        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

        if (oneShot != null) {
            System.exit(runQuery(client, url, oneShot));
        }
        repl(client, url);
    }

    private static void usage() {
        System.err.println("Usage: cli [--url http://host:port] [-c \"SQL\"]");
    }

    private static void repl(HttpClient client, String url) throws Exception {
        SchemaInfo schema = fetchSchema(client, url);

        try (Terminal terminal = TerminalBuilder.builder().system(true).build()) {
            List<String> candidates = new ArrayList<>();
            candidates.addAll(SQL_KEYWORDS);
            if (schema.tableName() != null)
                candidates.add(schema.tableName());
            candidates.addAll(schema.columnNames());
            candidates.addAll(List.of("\\q", "\\c", "\\h", "\\s", "clear", "help", "schema"));

            Path historyFile = Path.of(System.getProperty("user.home"), ".jpointdb_history");

            // Default parser treats '\' as escape char → '\s' would become 's'.
            // We want backslash to be a literal (it introduces our meta-commands like \s, \c).
            DefaultParser parser = new DefaultParser();
            parser.setEscapeChars(null);

            LineReader reader = LineReaderBuilder.builder().terminal(terminal).parser(parser)
                    .completer(new StringsCompleter(candidates)).highlighter(new SqlHighlighter())
                    .variable(LineReader.HISTORY_FILE, historyFile).variable(LineReader.HISTORY_SIZE, 5000)
                    .variable(LineReader.HISTORY_FILE_SIZE, 10000).option(LineReader.Option.HISTORY_BEEP, false)
                    .option(LineReader.Option.CASE_INSENSITIVE, true)
                    .option(LineReader.Option.DISABLE_EVENT_EXPANSION, true).build();

            printBanner(terminal, url, schema);

            StringBuilder buf = new StringBuilder();
            while (true) {
                String prompt = buf.length() == 0 ? color("jp> ", 2) : dim(" .. ");
                String line;
                try {
                    line = reader.readLine(prompt);
                } catch (UserInterruptException e) {
                    buf.setLength(0);
                    continue;
                } catch (EndOfFileException e) {
                    break;
                }
                if (line == null)
                    break;
                String trimmed = line.trim();

                // Meta-commands (only valid at start of a fresh buffer).
                if (buf.length() == 0) {
                    MetaCommand meta = parseMeta(trimmed);
                    switch (meta) {
                        case EXIT -> {
                            return;
                        }
                        case CLEAR -> {
                            if (!terminal.puts(InfoCmp.Capability.clear_screen)) {
                                // dumb terminal (e.g. piped stdin) — write ANSI directly.
                                terminal.writer().print("\u001B[2J\u001B[H");
                            } else {
                                terminal.puts(InfoCmp.Capability.cursor_home);
                            }
                            terminal.flush();
                            continue;
                        }
                        case HELP -> {
                            printHelp(terminal);
                            continue;
                        }
                        case SCHEMA -> {
                            printSchemaSummary(terminal, schema);
                            continue;
                        }
                        case EMPTY -> {
                            continue;
                        }
                        case NONE -> { /* fall through — treat as SQL */ }
                    }
                }

                buf.append(line).append('\n');
                if (trimmed.endsWith(";")) {
                    String sql = buf.toString();
                    buf.setLength(0);
                    runQuery(client, url, sql);
                }
            }
        }
    }

    private static void printBanner(Terminal terminal, String url, SchemaInfo schema) {
        terminal.writer()
                .println(
                        color("JPointDB CLI", 2)
                                + " — " + url + (schema.tableName() != null
                                                                            ? dim(" · table ") + schema.tableName()
                                                                                    + dim(" · " + schema.columnNames()
                                                                                            .size() + " cols")
                                                                            : ""));
        terminal.writer().println(dim("  ↑/↓ history · TAB complete · ^L clear · ^C cancel · ^D exit · \\h help"));
        terminal.writer().flush();
    }

    private static void printHelp(Terminal terminal) {
        String[] lines = {color("SQL:", 2) + " end every statement with ';' (can span lines)", color("Meta:", 2),
                "  \\q, exit, quit     exit", "  \\c, clear, cls     clear screen (or Ctrl+L)",
                "  \\s, schema         show table / columns summary", "  \\h, help, ?        this help",
                color("Keys:", 2) + " ↑/↓ history · TAB complete · ^R reverse search · ^C cancel line"};
        for (String l : lines)
            terminal.writer().println("  " + l);
        terminal.writer().flush();
    }

    private static void printSchemaSummary(Terminal terminal, SchemaInfo schema) {
        if (schema.tableName() == null) {
            terminal.writer().println(dim("  (schema unavailable — server not reachable at startup)"));
            terminal.flush();
            return;
        }
        terminal.writer().println(color("table", 4) + " " + schema.tableName() + dim(" · ")
                + schema.columnNames().size() + dim(" columns"));
        int per = 4;
        for (int i = 0; i < schema.columnNames().size(); i += per) {
            StringBuilder row = new StringBuilder("  ");
            for (int j = i; j < Math.min(i + per, schema.columnNames().size()); j++) {
                String name = schema.columnNames().get(j);
                if (j > i)
                    row.append("  ");
                row.append(padRight(name, 22));
            }
            terminal.writer().println(row);
        }
        terminal.writer().flush();
    }

    private static String color(String s, int colorCode) {
        return "\u001B[1;3" + colorCode + "m" + s + "\u001B[0m";
    }

    private static String dim(String s) {
        return "\u001B[2m" + s + "\u001B[0m";
    }

    private record SchemaInfo(@Nullable String tableName, List<String> columnNames) {
        static SchemaInfo unavailable() {
            return new SchemaInfo(null, List.of());
        }
    }

    enum MetaCommand {
        EXIT, CLEAR, HELP, SCHEMA, EMPTY, NONE
    }

    static MetaCommand parseMeta(String trimmed) {
        if (trimmed.isEmpty())
            return MetaCommand.EMPTY;
        if (trimmed.equals("\\q") || trimmed.equalsIgnoreCase("exit") || trimmed.equalsIgnoreCase("quit")) {
            return MetaCommand.EXIT;
        }
        if (trimmed.equals("\\c") || trimmed.equalsIgnoreCase("clear") || trimmed.equalsIgnoreCase("cls")) {
            return MetaCommand.CLEAR;
        }
        if (trimmed.equals("\\h") || trimmed.equalsIgnoreCase("help") || trimmed.equals("?")) {
            return MetaCommand.HELP;
        }
        if (trimmed.equals("\\s") || trimmed.equalsIgnoreCase("schema")) {
            return MetaCommand.SCHEMA;
        }
        return MetaCommand.NONE;
    }

    @SuppressWarnings("unchecked")
    private static SchemaInfo fetchSchema(HttpClient client, String url) {
        try {
            HttpRequest req =
                    HttpRequest.newBuilder(URI.create(url + "/schema")).timeout(Duration.ofSeconds(3)).GET().build();
            HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200)
                return SchemaInfo.unavailable();
            Object parsed = Json.parse(resp.body());
            if (!(parsed instanceof Map<?, ?> root))
                return SchemaInfo.unavailable();
            Map<String, Object> rootMap = (Map<String, Object>) root;
            String tableName = rootMap.get("name") != null ? String.valueOf(rootMap.get("name")) : null;
            Object cols = rootMap.get("columns");
            if (!(cols instanceof List<?> list))
                return new SchemaInfo(tableName, List.of());
            List<String> names = new ArrayList<>(list.size());
            for (Object c : list) {
                if (c instanceof Map<?, ?> cm) {
                    Object name = cm.get("name");
                    if (name != null)
                        names.add(String.valueOf(name));
                }
            }
            return new SchemaInfo(tableName, names);
        } catch (Exception e) {
            return SchemaInfo.unavailable();
        }
    }

    static int runQuery(HttpClient client, String url, String sql) {
        try {
            HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url + "/query"))
                    .header("Content-Type", "text/plain; charset=utf-8").timeout(Duration.ofMinutes(5))
                    .POST(HttpRequest.BodyPublishers.ofString(sql, StandardCharsets.UTF_8)).build();
            HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            Object parsed;
            try {
                parsed = Json.parse(resp.body());
            } catch (RuntimeException e) {
                System.err.println("Invalid JSON from server (HTTP " + resp.statusCode() + "): " + resp.body());
                return 1;
            }
            if (resp.statusCode() != 200) {
                Object err = (parsed instanceof Map<?, ?> m) ? m.get("error") : resp.body();
                System.err.println("\u001B[31mError " + resp.statusCode() + ":\u001B[0m " + err);
                return 1;
            }
            if (parsed != null)
                printTable(parsed);
            return 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    @SuppressWarnings("unchecked")
    static void printTable(@Nullable Object parsed) {
        if (!(parsed instanceof Map<?, ?> root)) {
            System.err.println("unexpected response: " + parsed);
            return;
        }
        Map<String, Object> m = (Map<String, Object>) root;
        List<Map<String, Object>> columns = (List<Map<String, Object>>) m.get("columns");
        if (columns == null) {
            System.err.println("no columns in response");
            return;
        }
        List<List<Object>> rows = (List<List<Object>>) m.get("rows");
        if (rows == null)
            rows = List.of();
        Object rowCount = m.get("rowCount");
        Object elapsed = m.get("elapsedMs");

        List<String> headers = new ArrayList<>();
        for (Map<String, Object> c : columns)
            headers.add(String.valueOf(c.get("name")));

        int cols = headers.size();
        int[] widths = new int[cols];
        for (int i = 0; i < cols; i++)
            widths[i] = headers.get(i).length();

        List<String[]> stringRows = new ArrayList<>(rows.size());
        for (List<Object> row : rows) {
            String[] s = new String[cols];
            for (int i = 0; i < cols; i++) {
                Object v = i < row.size() ? row.get(i) : null;
                s[i] = formatCell(v);
                if (s[i].length() > widths[i])
                    widths[i] = Math.min(s[i].length(), 80);
            }
            stringRows.add(s);
        }

        printHeader(headers, widths);
        printDivider(widths);
        for (String[] row : stringRows)
            printDataRow(row, widths);
        long rc = rowCount instanceof Number n ? n.longValue() : 0L;
        System.out.println(dim("(" + rc + " row" + (rc == 1 ? "" : "s") + " in " + elapsed + " ms)"));
    }

    private static String formatCell(@Nullable Object v) {
        if (v == null)
            return "∅";
        return String.valueOf(v);
    }

    private static void printHeader(List<String> row, int[] widths) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < row.size(); i++) {
            if (i > 0)
                sb.append("  ");
            sb.append("\u001B[1;34m").append(padRight(row.get(i), widths[i])).append("\u001B[0m");
        }
        System.out.println(sb);
    }

    private static void printDataRow(String[] row, int[] widths) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < row.length; i++) {
            String cell = row[i];
            if (cell.length() > widths[i])
                cell = cell.substring(0, Math.max(1, widths[i] - 1)) + "…";
            if (i > 0)
                sb.append("  ");
            if ("∅".equals(cell)) {
                sb.append("\u001B[2m").append(padRight(cell, widths[i])).append("\u001B[0m");
            } else {
                sb.append(padRight(cell, widths[i]));
            }
        }
        System.out.println(sb);
    }

    private static void printDivider(int[] widths) {
        StringBuilder sb = new StringBuilder("\u001B[2m");
        for (int i = 0; i < widths.length; i++) {
            if (i > 0)
                sb.append("  ");
            sb.append("─".repeat(widths[i]));
        }
        sb.append("\u001B[0m");
        System.out.println(sb);
    }

    static String padRight(String s, int width) {
        if (s.length() >= width)
            return s;
        return s + " ".repeat(width - s.length());
    }
}
