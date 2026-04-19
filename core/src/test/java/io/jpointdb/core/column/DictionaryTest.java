package io.jpointdb.core.column;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class DictionaryTest {

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void emptyBuilderHasZeroSize() {
        DictionaryBuilder b = new DictionaryBuilder();
        assertEquals(0, b.size());
        assertFalse(b.overflowed());
    }

    @Test
    void firstInsertReturnsZeroId() {
        DictionaryBuilder b = new DictionaryBuilder();
        assertEquals(0, b.putOrGet(utf8("Chrome")));
        assertEquals(1, b.size());
    }

    @Test
    void repeatedKeyReturnsSameId() {
        DictionaryBuilder b = new DictionaryBuilder();
        int id1 = b.putOrGet(utf8("Chrome"));
        int id2 = b.putOrGet(utf8("Chrome"));
        assertEquals(id1, id2);
        assertEquals(1, b.size());
    }

    @Test
    void distinctKeysReturnDistinctIds() {
        DictionaryBuilder b = new DictionaryBuilder();
        assertEquals(0, b.putOrGet(utf8("Chrome")));
        assertEquals(1, b.putOrGet(utf8("Firefox")));
        assertEquals(2, b.putOrGet(utf8("Safari")));
        assertEquals(0, b.putOrGet(utf8("Chrome")));
        assertEquals(3, b.size());
    }

    @Test
    void emptyKeyIsSupported() {
        DictionaryBuilder b = new DictionaryBuilder();
        int id = b.putOrGet(utf8(""));
        assertEquals(0, id);
        assertEquals(0, b.keyLength(id));
        assertArrayEquals(new byte[0], b.keyBytes(id));
    }

    @Test
    void keyBytesReturnsOriginalBytes() {
        DictionaryBuilder b = new DictionaryBuilder();
        b.putOrGet(utf8("привет"));
        b.putOrGet(utf8("world"));
        assertArrayEquals(utf8("привет"), b.keyBytes(0));
        assertArrayEquals(utf8("world"), b.keyBytes(1));
    }

    @Test
    void overflowReturnsMinusOneAndLatches() {
        DictionaryBuilder b = new DictionaryBuilder(2);
        assertEquals(0, b.putOrGet(utf8("a")));
        assertEquals(1, b.putOrGet(utf8("b")));
        assertEquals(-1, b.putOrGet(utf8("c")));
        assertTrue(b.overflowed());
        assertEquals(-1, b.putOrGet(utf8("d")));
        assertEquals(2, b.size());
    }

    @Test
    void overflowStillAllowsExistingLookups() {
        DictionaryBuilder b = new DictionaryBuilder(1);
        assertEquals(0, b.putOrGet(utf8("a")));
        assertEquals(-1, b.putOrGet(utf8("b")));
        assertTrue(b.overflowed());
        // After overflow putOrGet returns -1 even for existing keys — caller switches mode.
        assertEquals(-1, b.putOrGet(utf8("a")));
    }

    @Test
    void resizeAcrossHashTableBoundary() {
        DictionaryBuilder b = new DictionaryBuilder();
        int n = 2000;
        for (int i = 0; i < n; i++) {
            assertEquals(i, b.putOrGet(utf8("k" + i)));
        }
        for (int i = 0; i < n; i++) {
            assertEquals(i, b.putOrGet(utf8("k" + i)));
        }
        assertEquals(n, b.size());
    }

    @Test
    void writeAndReadBack(@TempDir Path dir) throws IOException {
        DictionaryBuilder b = new DictionaryBuilder();
        b.putOrGet(utf8("Chrome"));
        b.putOrGet(utf8("Firefox"));
        b.putOrGet(utf8("Safari"));
        b.putOrGet(utf8(""));
        b.putOrGet(utf8("привет"));

        Path file = dir.resolve("dict.bin");
        b.writeToFile(file);

        try (Dictionary d = Dictionary.open(file)) {
            assertEquals(5, d.size());
            assertEquals("Chrome", d.keyAsString(0));
            assertEquals("Firefox", d.keyAsString(1));
            assertEquals("Safari", d.keyAsString(2));
            assertEquals("", d.keyAsString(3));
            assertEquals("привет", d.keyAsString(4));
        }
    }

    @Test
    void emptyDictionaryRoundTrip(@TempDir Path dir) throws IOException {
        DictionaryBuilder b = new DictionaryBuilder();
        Path file = dir.resolve("dict.bin");
        b.writeToFile(file);
        try (Dictionary d = Dictionary.open(file)) {
            assertEquals(0, d.size());
        }
    }

    @Test
    void openRejectsBadMagic(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("dict.bin");
        java.nio.file.Files.write(file,
                new byte[]{'X', 'X', 'X', 'X', 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
        assertThrows(IOException.class, () -> Dictionary.open(file));
    }
}
