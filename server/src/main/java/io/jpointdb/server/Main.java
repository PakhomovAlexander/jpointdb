package io.jpointdb.server;

import com.sun.net.httpserver.HttpServer;
import io.jpointdb.core.convert.TsvConverter;
import io.jpointdb.core.table.Table;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.jspecify.annotations.Nullable;

/**
 * JPointDB server entry point.
 *
 * <pre>
 *   Usage:
 *     --data-dir &lt;path&gt;           open an existing .jpdb directory
 *     --tsv &lt;path&gt;                convert a TSV at startup (outputs next to it
 *                                  by default, override via --data-dir)
 *     --port &lt;n&gt;                  default 8080
 *     --header                     the TSV's first row is a header
 *     --column-names a,b,c         names to assign when no header
 * </pre>
 */
public final class Main {

    public static void main(String[] args) throws Exception {
        @Nullable
        Path dataDir = null;
        @Nullable
        Path tsv = null;
        int port = 8080;
        boolean header = false;
        @Nullable
        List<String> names = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--data-dir" -> dataDir = Path.of(args[++i]);
                case "--tsv" -> tsv = Path.of(args[++i]);
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--header" -> header = true;
                case "--column-names" -> names = List.of(args[++i].split(","));
                case "--help", "-h" -> {
                    usage();
                    return;
                }
                default -> {
                    System.err.println("unknown argument: " + args[i]);
                    usage();
                    System.exit(2);
                }
            }
        }

        if (dataDir == null && tsv == null) {
            System.err.println("must provide --data-dir or --tsv");
            usage();
            System.exit(2);
        }

        if (tsv != null) {
            if (dataDir == null) {
                String fname = tsv.getFileName().toString();
                int dot = fname.lastIndexOf('.');
                String base = dot > 0 ? fname.substring(0, dot) : fname;
                Path parent = tsv.toAbsolutePath().getParent();
                if (parent == null)
                    throw new IllegalArgumentException("--tsv must not be at filesystem root");
                dataDir = parent.resolve(base + ".jpdb");
            }
            Path dd = dataDir;
            if (dd == null)
                throw new IllegalStateException("dataDir unset");
            if (!Files.exists(dd.resolve("meta.json"))) {
                System.out.println("Converting " + tsv + " → " + dataDir + " ...");
                long t0 = System.nanoTime();
                TsvConverter.Options opts = TsvConverter.Options.defaults();
                if (header)
                    opts = opts.withHeader(true);
                if (names != null)
                    opts = opts.withColumnNames(names);
                new TsvConverter(tsv, dd, opts).convert();
                long ms = (System.nanoTime() - t0) / 1_000_000;
                System.out.println("Converted in " + ms + " ms.");
            } else {
                System.out.println("Using existing " + dd);
            }
        }

        if (dataDir == null)
            throw new IllegalStateException("dataDir unset");
        Table table = Table.open(dataDir);
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        HttpApi.register(server, table);
        server.setExecutor(null);
        server.start();
        System.out.println("JPointDB listening on http://localhost:" + port + " (table " + dataDir + ", "
                + table.rowCount() + " rows, " + table.columnCount() + " columns)");
    }

    private static void usage() {
        System.err.println("Usage: server --data-dir <path> [--port 8080]");
        System.err.println(
                "   or  server --tsv <path> [--data-dir <out>] [--header] [--column-names a,b,c] [--port 8080]");
    }

    // Keep a handle so we can stop from tests.
    public static HttpServer startForTesting(Path dataDir, int port) throws Exception {
        if (dataDir == null)
            throw new IllegalStateException("dataDir unset");
        Table table = Table.open(dataDir);
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        HttpApi.register(server, table);
        server.setExecutor(null);
        server.start();
        return server;
    }
}
