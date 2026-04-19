package io.jpointdb.core.column;

import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;

import static org.junit.jupiter.api.Assertions.*;

class NullBitmapTest {

    @Test
    void emptyBitmapHasZeroBytes() {
        NullBitmap nb = new NullBitmap();
        assertEquals(0, nb.byteLength());
        assertArrayEquals(new byte[0], nb.toByteArray());
    }

    @Test
    void singleValidBitSet() {
        NullBitmap nb = new NullBitmap();
        nb.appendValid();
        assertEquals(1, nb.byteLength());
        byte[] bytes = nb.toByteArray();
        assertEquals(1, bytes[0] & 0xff);
    }

    @Test
    void mixedValidAndNullBits() {
        NullBitmap nb = new NullBitmap();
        nb.appendValid();   // bit 0 set
        nb.appendNull();    // bit 1 clear
        nb.appendValid();   // bit 2 set
        nb.appendNull();    // bit 3 clear
        byte[] bytes = nb.toByteArray();
        assertEquals(0b00000101, bytes[0] & 0xff);
    }

    @Test
    void growsAcrossByteBoundary() {
        NullBitmap nb = new NullBitmap();
        for (int i = 0; i < 17; i++)
            nb.appendValid();
        assertEquals(3, nb.byteLength());
        byte[] bytes = nb.toByteArray();
        assertEquals(0xff, bytes[0] & 0xff);
        assertEquals(0xff, bytes[1] & 0xff);
        assertEquals(0x01, bytes[2] & 0xff);
    }

    @Test
    void isValidReadsFromMemorySegment() {
        NullBitmap nb = new NullBitmap();
        nb.appendValid();
        nb.appendNull();
        nb.appendValid();
        MemorySegment seg = MemorySegment.ofArray(nb.toByteArray());
        assertTrue(NullBitmap.isValid(seg, 0));
        assertFalse(NullBitmap.isValid(seg, 1));
        assertTrue(NullBitmap.isValid(seg, 2));
    }
}
