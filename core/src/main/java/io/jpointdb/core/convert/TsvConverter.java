package io.jpointdb.core.convert;

import io.jpointdb.core.column.*;
import io.jpointdb.core.schema.ColumnType;
import io.jpointdb.core.schema.Encoding;
import io.jpointdb.core.schema.Schema;
import io.jpointdb.core.schema.SchemaSniffer;
import io.jpointdb.core.table.ColumnMeta;
import io.jpointdb.core.table.TableMeta;
import io.jpointdb.core.tsv.TsvScanner;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * Converts a TSV file into a JPointDB column directory. Three passes:
 * <ol>
 * <li>Schema sniff — infer types (I32/I64/F64/STRING).
 * <li>Dictionary build — for STRING columns, build a dict (may overflow).
 * <li>Column write — emit each column file plus meta.json.
 * </ol>
 *
 * <p>
 * Output directory layout:
 *
 * <pre>
 *   outDir/
 *     meta.json
 *     col_0.bin, col_1.bin, ...
 *     dict_1.bin, dict_3.bin, ...   (only for DICT-encoded STRING columns)
 * </pre>
 */
public final class TsvConverter {

    public record Options(boolean hasHeader, @Nullable List<String> columnNames, int dictCardinalityThreshold) {
        public static Options defaults() {
            return new Options(false, null, 1_000_000);
        }

        public Options withHeader(boolean headerPresent) {
            return new Options(headerPresent, columnNames, dictCardinalityThreshold);
        }

        public Options withColumnNames(List<String> names) {
            return new Options(hasHeader, names, dictCardinalityThreshold);
        }

        public Options withDictThreshold(int threshold) {
            return new Options(hasHeader, columnNames, threshold);
        }
    }

    private final Path tsvFile;
    private final Path outDir;
    private final Options options;

    public TsvConverter(Path tsvFile, Path outDir, Options options) {
        this.tsvFile = tsvFile;
        this.outDir = outDir;
        this.options = options;
    }

    public void convert() throws IOException {
        try (Arena arena = Arena.ofShared()) {
            MemorySegment tsv = mmapTsv(arena);

            List<String> columnNames = extractColumnNames(tsv);

            Schema schema = sniffTypes(tsv, columnNames);

            long[] rowCountHolder = new long[1];
            List<DictionaryBuilder> dicts = buildDictionaries(tsv, schema, rowCountHolder);
            long rowCount = rowCountHolder[0];

            Files.createDirectories(outDir);

            List<ColumnMeta> metas = buildColumnMetas(schema, dicts);

            for (int i = 0; i < metas.size(); i++) {
                ColumnMeta meta = metas.get(i);
                if (meta.encoding() == Encoding.DICT) {
                    dicts.get(i).writeToFile(outDir.resolve(meta.dictFile()));
                }
            }

            writeColumnFiles(tsv, metas, dicts);

            new TableMeta(TableMeta.CURRENT_VERSION, rowCount, metas).save(outDir.resolve("meta.json"));
        }
    }

    private MemorySegment mmapTsv(Arena arena) throws IOException {
        try (FileChannel ch = FileChannel.open(tsvFile, StandardOpenOption.READ)) {
            return ch.map(FileChannel.MapMode.READ_ONLY, 0, ch.size(), arena);
        }
    }

    private @Nullable List<String> extractColumnNames(MemorySegment tsv) {
        if (options.hasHeader) {
            TsvScanner sc = new TsvScanner(tsv);
            if (sc.nextRecord()) {
                List<String> names = new ArrayList<>(sc.fieldCount());
                for (int i = 0; i < sc.fieldCount(); i++)
                    names.add(sc.fieldAsString(i));
                return names;
            }
            return List.of();
        }
        return options.columnNames;
    }

    private Schema sniffTypes(MemorySegment tsv, @Nullable List<String> columnNames) {
        TsvScanner sc = new TsvScanner(tsv);
        if (options.hasHeader)
            sc.nextRecord();
        return SchemaSniffer.sniff(sc, columnNames);
    }

    private List<DictionaryBuilder> buildDictionaries(MemorySegment tsv, Schema schema, long[] rowCountOut) {
        int n = schema.columnCount();
        List<DictionaryBuilder> dicts = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            if (schema.column(i).type() == ColumnType.STRING) {
                dicts.add(new DictionaryBuilder(options.dictCardinalityThreshold));
            } else {
                dicts.add(null);
            }
        }
        TsvScanner sc = new TsvScanner(tsv);
        if (options.hasHeader)
            sc.nextRecord();
        long rowCount = 0;
        while (sc.nextRecord()) {
            int fc = sc.fieldCount();
            for (int i = 0; i < fc && i < n; i++) {
                DictionaryBuilder d = dicts.get(i);
                if (d == null)
                    continue;
                if (sc.fieldIsNull(i))
                    continue;
                d.putOrGet(sc.data(), sc.fieldOffset(i), sc.fieldLength(i));
            }
            rowCount++;
        }
        rowCountOut[0] = rowCount;
        return dicts;
    }

    private List<ColumnMeta> buildColumnMetas(Schema schema, List<DictionaryBuilder> dicts) {
        int n = schema.columnCount();
        List<ColumnMeta> metas = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            Schema.Column sc = schema.column(i);
            String dataFile = "col_" + i + ".bin";
            String dictFile = null;
            Encoding encoding;
            if (sc.type() == ColumnType.STRING) {
                DictionaryBuilder d = dicts.get(i);
                if (d != null && !d.overflowed()) {
                    encoding = Encoding.DICT;
                    dictFile = "dict_" + i + ".bin";
                } else {
                    encoding = Encoding.RAW;
                }
            } else {
                encoding = Encoding.PLAIN;
            }
            metas.add(new ColumnMeta(sc.name(), sc.type(), sc.nullable(), encoding, dataFile, dictFile));
        }
        return metas;
    }

    private void writeColumnFiles(MemorySegment tsv, List<ColumnMeta> metas, List<DictionaryBuilder> dicts)
            throws IOException {
        int n = metas.size();
        List<FieldSink> sinks = new ArrayList<>(n);
        IOException primary = null;
        try {
            for (int i = 0; i < n; i++) {
                sinks.add(FieldSink.open(outDir.resolve(metas.get(i).dataFile()), metas.get(i), dicts.get(i)));
            }
            TsvScanner sc = new TsvScanner(tsv);
            if (options.hasHeader)
                sc.nextRecord();
            while (sc.nextRecord()) {
                int fc = sc.fieldCount();
                for (int i = 0; i < n; i++) {
                    FieldSink sink = sinks.get(i);
                    if (i >= fc) {
                        sink.acceptMissing();
                    } else {
                        sink.accept(sc, i);
                    }
                }
            }
        } catch (IOException e) {
            primary = e;
        }
        IOException closeErr = closeAll(sinks);
        if (primary != null) {
            if (closeErr != null)
                primary.addSuppressed(closeErr);
            throw primary;
        }
        if (closeErr != null)
            throw closeErr;
    }

    private static @Nullable IOException closeAll(List<FieldSink> sinks) {
        IOException deferred = null;
        for (FieldSink s : sinks) {
            try {
                s.close();
            } catch (IOException e) {
                if (deferred == null)
                    deferred = e;
                else
                    deferred.addSuppressed(e);
            }
        }
        return deferred;
    }
}
