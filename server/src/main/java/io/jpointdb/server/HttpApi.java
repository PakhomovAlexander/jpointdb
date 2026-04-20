package io.jpointdb.server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.jpointdb.core.query.QueryEngine;
import io.jpointdb.core.query.QueryResult;
import io.jpointdb.core.sql.SqlException;
import io.jpointdb.core.table.Table;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import org.jspecify.annotations.Nullable;

final class HttpApi {

    private HttpApi() {
    }

    static void register(HttpServer server, Table table) {
        server.createContext("/health", ex -> respond(ex, 200, "application/json",
                "{\"status\":\"ok\",\"rowCount\":" + table.rowCount() + "}"));

        server.createContext("/schema", ex -> {
            if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
                respond(ex, 405, "application/json", JsonSerializer.error("GET only"));
                return;
            }
            respond(ex, 200, "application/json", JsonSerializer.schema(table));
        });

        server.createContext("/query", ex -> {
            if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
                respond(ex, 405, "application/json", JsonSerializer.error("POST only"));
                return;
            }
            String sql = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            long start = System.nanoTime();
            try {
                QueryResult r = QueryEngine.run(table, sql);
                long elapsedNanos = System.nanoTime() - start;
                long elapsedMs = elapsedNanos / 1_000_000;
                respond(ex, 200, "application/json",
                        JsonSerializer.queryResult(r, elapsedMs, elapsedNanos));
            } catch (SqlException e) {
                respond(ex, 400, "application/json",
                        JsonSerializer.error(e.getMessage() == null ? "parse error" : e.getMessage()));
            } catch (RuntimeException e) {
                respond(ex, 500, "application/json",
                        JsonSerializer.error(e.getClass().getSimpleName() + ": " + e.getMessage()));
            }
        });

        server.createContext("/", ex -> {
            String path = ex.getRequestURI().getPath();
            if (path.equals("/"))
                path = "/index.html";
            byte[] data = readResource("/web" + path);
            if (data == null) {
                respond(ex, 404, "application/json", JsonSerializer.error("not found: " + path));
                return;
            }
            String ct = contentTypeFor(path);
            respondBytes(ex, 200, ct, data);
        });
    }

    private static byte @Nullable [] readResource(String resource) {
        try (var in = HttpApi.class.getResourceAsStream(resource)) {
            return in == null ? null : in.readAllBytes();
        } catch (IOException e) {
            return null;
        }
    }

    private static String contentTypeFor(String path) {
        if (path.endsWith(".html") || path.endsWith(".htm"))
            return "text/html; charset=utf-8";
        if (path.endsWith(".js"))
            return "application/javascript; charset=utf-8";
        if (path.endsWith(".css"))
            return "text/css; charset=utf-8";
        return "application/octet-stream";
    }

    private static void respond(HttpExchange ex, int code, String contentType, String body) throws IOException {
        respondBytes(ex, code, contentType, body.getBytes(StandardCharsets.UTF_8));
    }

    private static void respondBytes(HttpExchange ex, int code, String contentType, byte[] body) throws IOException {
        ex.getResponseHeaders().set("Content-Type", contentType);
        ex.sendResponseHeaders(code, body.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(body);
        }
    }
}
