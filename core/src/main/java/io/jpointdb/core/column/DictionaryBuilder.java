package io.jpointdb.core.column;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

/**
 * Hash-based builder that maps byte sequences to dense integer IDs.
 *
 * <p>
 * Open addressing with linear probing, 0.75 load factor. Keys are copied into a
 * single growable byte array and referenced by (offset, length).
 *
 * <p>
 * If the number of unique keys would exceed {@code maxCardinality},
 * {@link #putOrGet} returns {@code -1} and subsequent calls continue to return
 * {@code -1} — the caller should fall back to raw-string encoding.
 */
public final class DictionaryBuilder {

    private static final byte[] MAGIC = {'J', 'P', 'D', 'I'};
    private static final byte VERSION = 1;
    static final int HEADER_SIZE = 24;

    private final int maxCardinality;

    private int[] hashTable;
    private int mask;
    private int[] keyOffsets;
    private int[] keyLengths;
    private byte[] storage;
    private int storageUsed;
    private int count;
    private boolean overflowed;

    public DictionaryBuilder() {
        this(Integer.MAX_VALUE);
    }

    public DictionaryBuilder(int maxCardinality) {
        if (maxCardinality <= 0)
            throw new IllegalArgumentException("maxCardinality must be > 0");
        this.maxCardinality = maxCardinality;
        this.hashTable = new int[256];
        this.mask = 255;
        this.keyOffsets = new int[64];
        this.keyLengths = new int[64];
        this.storage = new byte[1024];
    }

    public int putOrGet(MemorySegment data, long offset, int length) {
        if (overflowed)
            return -1;
        int hash = hashBytes(data, offset, length);
        int slot = hash & mask;
        while (true) {
            int idPlus = hashTable[slot];
            if (idPlus == 0) {
                if (count >= maxCardinality) {
                    overflowed = true;
                    return -1;
                }
                int id = count;
                ensureIdCapacity(id + 1);
                ensureStorageCapacity(storageUsed + length);
                keyOffsets[id] = storageUsed;
                keyLengths[id] = length;
                if (length > 0) {
                    MemorySegment.copy(data, ValueLayout.JAVA_BYTE, offset, storage, storageUsed, length);
                }
                storageUsed += length;
                count++;
                hashTable[slot] = id + 1;
                if (count * 4 > hashTable.length * 3) {
                    resizeHashTable();
                }
                return id;
            }
            int id = idPlus - 1;
            if (keyEquals(id, data, offset, length)) {
                return id;
            }
            slot = (slot + 1) & mask;
        }
    }

    public int putOrGet(byte[] bytes) {
        return putOrGet(MemorySegment.ofArray(bytes), 0, bytes.length);
    }

    public int size() {
        return count;
    }

    public boolean overflowed() {
        return overflowed;
    }

    public int keyLength(int id) {
        return keyLengths[id];
    }

    public byte[] keyBytes(int id) {
        return Arrays.copyOfRange(storage, keyOffsets[id], keyOffsets[id] + keyLengths[id]);
    }

    private static int hashBytes(MemorySegment data, long offset, int length) {
        int h = 0;
        for (int i = 0; i < length; i++) {
            h = 31 * h + (data.get(ValueLayout.JAVA_BYTE, offset + i) & 0xff);
        }
        return h;
    }

    private int hashStoredKey(int id) {
        int h = 0;
        int off = keyOffsets[id];
        int len = keyLengths[id];
        for (int i = 0; i < len; i++) {
            h = 31 * h + (storage[off + i] & 0xff);
        }
        return h;
    }

    private boolean keyEquals(int id, MemorySegment data, long offset, int length) {
        if (keyLengths[id] != length)
            return false;
        int sOff = keyOffsets[id];
        for (int i = 0; i < length; i++) {
            if (storage[sOff + i] != data.get(ValueLayout.JAVA_BYTE, offset + i))
                return false;
        }
        return true;
    }

    private void ensureIdCapacity(int needed) {
        if (needed > keyOffsets.length) {
            int newLen = Math.max(keyOffsets.length * 2, needed);
            keyOffsets = Arrays.copyOf(keyOffsets, newLen);
            keyLengths = Arrays.copyOf(keyLengths, newLen);
        }
    }

    private void ensureStorageCapacity(int needed) {
        if (needed < 0)
            throw new IllegalStateException("dictionary storage overflow");
        if (needed > storage.length) {
            int newLen = Math.max(storage.length * 2, needed);
            storage = Arrays.copyOf(storage, newLen);
        }
    }

    private void resizeHashTable() {
        int[] newTable = new int[hashTable.length * 2];
        int newMask = newTable.length - 1;
        for (int id = 0; id < count; id++) {
            int h = hashStoredKey(id);
            int slot = h & newMask;
            while (newTable[slot] != 0)
                slot = (slot + 1) & newMask;
            newTable[slot] = id + 1;
        }
        hashTable = newTable;
        mask = newMask;
    }

    public void writeToFile(Path file) throws IOException {
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

            ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE).order(ByteOrder.nativeOrder());
            header.put(MAGIC);
            header.put(VERSION);
            header.put((byte) 0).put((byte) 0).put((byte) 0);
            header.putLong(count);
            header.putLong(storageUsed);
            header.flip();
            while (header.hasRemaining())
                ch.write(header);

            int offsetsBytes = (count + 1) * 8;
            ByteBuffer offsetsBuf = ByteBuffer.allocate(offsetsBytes).order(ByteOrder.nativeOrder());
            for (int i = 0; i < count; i++) {
                offsetsBuf.putLong(keyOffsets[i] & 0xFFFFFFFFL);
            }
            offsetsBuf.putLong(storageUsed & 0xFFFFFFFFL);
            offsetsBuf.flip();
            while (offsetsBuf.hasRemaining())
                ch.write(offsetsBuf);

            if (storageUsed > 0) {
                ByteBuffer storageBuf = ByteBuffer.wrap(storage, 0, storageUsed);
                while (storageBuf.hasRemaining())
                    ch.write(storageBuf);
            }
        }
    }
}
