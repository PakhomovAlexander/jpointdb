package io.jpointdb.server;

import com.sun.net.httpserver.HttpServer;
import io.jpointdb.core.convert.TsvConverter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class HttpApiTest {

    private static Path buildTable(Path dir) throws IOException {
        Path src = dir.resolve("in.tsv");
        Files.writeString(src, "1\tus\n2\tde\n3\tus\n4\tfr\n5\tus\n");
        Path out = dir.resolve("t.jpdb");
        new TsvConverter(src, out, TsvConverter.Options.defaults().withColumnNames(List.of("id", "country"))).convert();
        return out;
    }

    @Test
    void healthSchemaAndQueryRoundTrip(@TempDir Path dir) throws Exception {
        Path table = buildTable(dir);
        HttpServer server = Main.startForTesting(table, 0);
        int port = server.getAddress().getPort();
        try {
            HttpClient client = HttpClient.newHttpClient();

            HttpResponse<String> health = client.send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/health")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, health.statusCode());
            assertTrue(health.body().contains("\"status\":\"ok\""));
            assertTrue(health.body().contains("\"rowCount\":5"));

            HttpResponse<String> schema = client.send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/schema")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, schema.statusCode());
            assertTrue(schema.body().contains("\"name\":\"id\""));
            assertTrue(schema.body().contains("\"name\":\"country\""));

            HttpResponse<String> q = client.send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/query"))
                            .header("Content-Type", "text/plain")
                            .POST(HttpRequest.BodyPublishers.ofString("SELECT COUNT(*) FROM t")).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, q.statusCode());
            assertTrue(q.body().contains("\"rowCount\":1"));
            assertTrue(q.body().contains("[5]"));

            HttpResponse<String> group =
                    client.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/query"))
                            .POST(HttpRequest.BodyPublishers.ofString(
                                    "SELECT country, COUNT(*) AS c FROM t GROUP BY country ORDER BY c DESC LIMIT 10"))
                            .build(), HttpResponse.BodyHandlers.ofString());
            assertEquals(200, group.statusCode());
            assertTrue(group.body().contains("\"us\""));
            assertTrue(group.body().contains("3"));

            HttpResponse<String> bad = client.send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/query"))
                            .POST(HttpRequest.BodyPublishers.ofString("SELECT nope FROM t")).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(400, bad.statusCode());
            assertTrue(bad.body().contains("error"));

            HttpResponse<String> index =
                    client.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/")).GET().build(),
                            HttpResponse.BodyHandlers.ofString());
            assertEquals(200, index.statusCode());
            assertTrue(index.headers().firstValue("Content-Type").orElse("").startsWith("text/html"));
            assertTrue(index.body().toLowerCase(java.util.Locale.ROOT).contains("<title>jpoint"));
        } finally {
            server.stop(0);
        }
    }
}
