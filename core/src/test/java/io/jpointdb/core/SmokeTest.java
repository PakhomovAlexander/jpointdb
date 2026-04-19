package io.jpointdb.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SmokeTest {

    @Test
    void javaVersionIs25OrAbove() {
        int major = Runtime.version().feature();
        assertTrue(major >= 25, "Expected Java 25+, got " + major);
    }

    @Test
    void vectorApiIsAvailable() throws ClassNotFoundException {
        Class<?> species = Class.forName("jdk.incubator.vector.IntVector");
        assertEquals("jdk.incubator.vector.IntVector", species.getName());
    }
}
