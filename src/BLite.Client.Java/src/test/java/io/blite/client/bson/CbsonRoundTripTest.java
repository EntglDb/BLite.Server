// BLite.Client.Java — CbsonRoundTripTest
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client.bson;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CbsonRoundTripTest {

    /** Build a stable key → id map for testing. */
    private static Map<String, Integer> keyMap() {
        var m = new LinkedHashMap<String, Integer>();
        m.put("name",    1);
        m.put("price",   2);
        m.put("active",  3);
        m.put("count",   4);
        m.put("ratio",   5);
        m.put("created", 6);
        m.put("data",    7);
        m.put("tags",    8);
        return m;
    }

    private static Map<Integer, String> reverse(Map<String, Integer> fwd) {
        var r = new LinkedHashMap<Integer, String>();
        fwd.forEach((k, v) -> r.put(v, k));
        return r;
    }

    @Test
    void string_roundtrip() {
        var doc    = Map.of("name", (Object) "Widget");
        var bytes  = CbsonWriter.encode(doc, keyMap());
        var result = CbsonReader.decode(bytes, reverse(keyMap()));
        assertEquals("Widget", result.get("name"));
    }

    @Test
    void int32_roundtrip() {
        var doc    = Map.of("count", (Object) 42);
        var bytes  = CbsonWriter.encode(doc, keyMap());
        var result = CbsonReader.decode(bytes, reverse(keyMap()));
        assertEquals(42, ((Number) result.get("count")).intValue());
    }

    @Test
    void int64_roundtrip() {
        var doc    = Map.of("count", (Object) 9_000_000_000L);
        var bytes  = CbsonWriter.encode(doc, keyMap());
        var result = CbsonReader.decode(bytes, reverse(keyMap()));
        assertEquals(9_000_000_000L, ((Number) result.get("count")).longValue());
    }

    @Test
    void double_roundtrip() {
        var doc    = Map.of("price", (Object) 9.99);
        var bytes  = CbsonWriter.encode(doc, keyMap());
        var result = CbsonReader.decode(bytes, reverse(keyMap()));
        assertEquals(9.99, ((Number) result.get("price")).doubleValue(), 0.0001);
    }

    @Test
    void boolean_roundtrip() {
        var doc    = Map.of("active", (Object) true);
        var bytes  = CbsonWriter.encode(doc, keyMap());
        var result = CbsonReader.decode(bytes, reverse(keyMap()));
        assertTrue((Boolean) result.get("active"));
    }

    @Test
    void bytes_roundtrip() {
        var payload = new byte[]{1, 2, 3, 4, 5};
        var doc     = Map.of("data", (Object) payload);
        var bytes   = CbsonWriter.encode(doc, keyMap());
        var result  = CbsonReader.decode(bytes, reverse(keyMap()));
        assertArrayEquals(payload, (byte[]) result.get("data"));
    }

    @Test
    void multiple_fields_roundtrip() {
        var doc = new LinkedHashMap<String, Object>();
        doc.put("name",  "Widget");
        doc.put("price", 9.99);
        doc.put("count", 3);
        var bytes  = CbsonWriter.encode(doc, keyMap());
        var result = CbsonReader.decode(bytes, reverse(keyMap()));
        assertEquals("Widget", result.get("name"));
        assertEquals(9.99,  ((Number) result.get("price")).doubleValue(), 0.0001);
        assertEquals(3,     ((Number) result.get("count")).intValue());
    }

    @Test
    void array_roundtrip() {
        var tags = List.of("a", "b", "c");
        var doc  = Map.of("tags", (Object) tags);
        var bytes  = CbsonWriter.encode(doc, keyMap());
        var result = CbsonReader.decode(bytes, reverse(keyMap()));
        // Array elements come back as a List; exact element types may vary
        var resultTags = (List<?>) result.get("tags");
        assertEquals(3, resultTags.size());
    }
}
