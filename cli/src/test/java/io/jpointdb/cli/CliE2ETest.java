package io.jpointdb.cli;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end tests that spawn the CLI as a subprocess and talk to it through
 * stdin/stdout, backed by a stub HTTP server emulating JPointDB's REST surface.
 *
 * <p>
 * Guards against regressions in meta-command parsing under the actual JLine
 * reader (piped stdin falls back to a dumb terminal, which previously stripped
 * {@code '\'} via {@code DefaultParser.escapeChars}).
 */
class CliE2ETest {

    private static HttpServer stub;
    private static int port;

    @BeforeAll
    @SuppressWarnings("AddressSelection") // localhost-only stub server for tests.
    static void startStub() throws Exception {
        stub = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        stub.createContext("/schema", ex -> sendJson(ex, "{\"name\":\"t\",\"rowCount\":3,\"columns\":["
                + "{\"name\":\"id\",\"type\":\"I64\",\"nullable\":false,\"encoding\":\"PLAIN\"},"
                + "{\"name\":\"channel\",\"type\":\"STRING\",\"nullable\":false,\"encoding\":\"DICT\"}" + "]}"));
        stub.createContext("/query", ex -> sendJson(ex, "{\"columns\":[{\"name\":\"count_star()\",\"type\":\"I64\"}],"
                + "\"rows\":[[3]],\"rowCount\":1,\"elapsedMs\":1}"));
        stub.setExecutor(null);
        stub.start();
        port = stub.getAddress().getPort();
    }

    @AfterAll
    static void stopStub() {
        if (stub != null)
            stub.stop(0);
    }

    private static void sendJson(HttpExchange ex, String body) throws IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(200, b.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(b);
        }
    }

    /**
     * Run the CLI as a subprocess with the given stdin content; return combined
     * stdout+stderr.
     */
    private static ProcessResult runCli(String stdin) throws Exception {
        String javaHome = System.getProperty("java.home");
        List<String> cmd = new ArrayList<>(List.of(javaHome + "/bin/java", "-cp", System.getProperty("java.class.path"),
                "--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED", "io.jpointdb.cli.Main",
                "--url", "http://127.0.0.1:" + port));
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        Process p = pb.start();
        p.getOutputStream().write(stdin.getBytes(StandardCharsets.UTF_8));
        p.getOutputStream().close();
        boolean done = p.waitFor(15, TimeUnit.SECONDS);
        byte[] out = p.getInputStream().readAllBytes();
        if (!done) {
            p.destroyForcibly();
            fail("CLI didn't exit within 15s. Captured:\n" + new String(out, StandardCharsets.UTF_8));
        }
        return new ProcessResult(p.exitValue(), new String(out, StandardCharsets.UTF_8));
    }

    private static final Pattern ANSI = Pattern.compile("\u001B\\[[0-9;]*[a-zA-Z]");

    private static String stripAnsi(String s) {
        return ANSI.matcher(s).replaceAll("");
    }

    // ------- tests -------

    @Test
    void schemaMetaCommandPrintsAllColumns() throws Exception {
        ProcessResult r = runCli("\\s\n\\q\n");
        String plain = stripAnsi(r.output);
        assertEquals(0, r.exit, "CLI should exit cleanly:\n" + plain);
        assertTrue(plain.contains("table t"), "schema header missing:\n" + plain);
        assertTrue(plain.contains("id"), "column 'id' missing:\n" + plain);
        assertTrue(plain.contains("channel"), "column 'channel' missing:\n" + plain);
    }

    @Test
    void clearMetaCommandEmitsClearScreenEscape() throws Exception {
        ProcessResult r = runCli("\\c\n\\q\n");
        assertEquals(0, r.exit);
        assertTrue(r.output.contains("\u001B[2J") || r.output.contains("\u001B[H") || r.output.contains("\u001B[3J"),
                "expected a clear-screen ANSI escape in output; got (ESC replaced):\n"
                        + r.output.replace("\u001B", "^["));
    }

    @Test
    void quitMetaCommandExitsCleanly() throws Exception {
        ProcessResult r = runCli("\\q\n");
        assertEquals(0, r.exit);
        // Banner should appear, but no prompt loops after it.
        assertTrue(stripAnsi(r.output).contains("JPointDB CLI"));
    }

    @Test
    void exitKeywordAlsoQuits() throws Exception {
        ProcessResult r = runCli("exit\n");
        assertEquals(0, r.exit);
    }

    @Test
    void helpMetaCommandPrintsKeybindings() throws Exception {
        ProcessResult r = runCli("\\h\n\\q\n");
        String plain = stripAnsi(r.output);
        assertEquals(0, r.exit);
        assertTrue(plain.contains("history"), plain);
        assertTrue(plain.contains("complete"), plain);
        assertTrue(plain.contains("clear"), plain);
    }

    @Test
    void pipedSqlQueryRunsAgainstServer() throws Exception {
        ProcessResult r = runCli("SELECT COUNT(*) FROM t;\n\\q\n");
        String plain = stripAnsi(r.output);
        assertEquals(0, r.exit, plain);
        assertTrue(plain.contains("count_star()"), plain);
        assertTrue(plain.contains("3"), plain);
    }

    @Test
    void multiLineSqlIsAccumulatedUntilSemicolon() throws Exception {
        ProcessResult r = runCli("SELECT\n  COUNT(*)\nFROM t\n;\n\\q\n");
        String plain = stripAnsi(r.output);
        assertEquals(0, r.exit, plain);
        assertTrue(plain.contains("count_star()"), plain);
    }

    @Test
    void metaCommandIsIgnoredIfMidStatement() throws Exception {
        // A line starting with '\s' after an unfinished statement is treated as
        // SQL continuation. We verify by sending a genuine multi-line statement
        // that happens to contain a backslash-s fragment; the SQL will be wrong
        // and the server (stub) always returns success, so our only check is
        // that \q still works afterwards.
        ProcessResult r = runCli("SELECT\n\\s\n;\n\\q\n");
        assertEquals(0, r.exit);
    }

    @Test
    void oneShotQueryViaCFlag() throws Exception {
        String javaHome = System.getProperty("java.home");
        ProcessBuilder pb = new ProcessBuilder(javaHome + "/bin/java", "-cp", System.getProperty("java.class.path"),
                "--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED", "io.jpointdb.cli.Main",
                "--url", "http://127.0.0.1:" + port, "-c", "SELECT COUNT(*) FROM t;");
        pb.redirectErrorStream(true);
        Process p = pb.start();
        assertTrue(p.waitFor(15, TimeUnit.SECONDS), "one-shot didn't exit");
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, p.exitValue(), out);
        String plain = stripAnsi(out);
        assertTrue(plain.contains("count_star()"), plain);
        assertTrue(plain.contains("3"), plain);
    }

    private record ProcessResult(int exit, String output) {
    }
}
