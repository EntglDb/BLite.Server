// BLite.Client.Java — CbsonWriter
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Encodes a Map<String, Object> to the C-BSON binary format used by BLite.
// C-BSON differs from standard BSON only in field names: instead of null-
// terminated strings, each field is identified by a 2-byte LE ushort that maps
// to a name through the server's global key dictionary.

package io.blite.client.bson;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

public final class CbsonWriter {

    private CbsonWriter() {}

    /**
     * Encodes a document as C-BSON bytes.
     *
     * @param doc    Source document (field names normalised to lower-case).
     * @param keyMap Forward map of lower-case field name → ushort ID.
     */
    @SuppressWarnings("unchecked")
    public static byte[] encode(Map<String, Object> doc, Map<String, Integer> keyMap) {
        var parts = new ArrayList<byte[]>();
        for (var entry : doc.entrySet()) {
            parts.add(writeElement(entry.getKey(), entry.getValue(), keyMap));
        }
        parts.add(new byte[]{0x00}); // END_OF_DOCUMENT
        return wrapWithSize(concat(parts));
    }

    // ── Internals ────────────────────────────────────────────────────────────

    private static byte[] writeElement(String name, Object value, Map<String, Integer> keyMap) {
        String lower = name.toLowerCase();
        Integer id   = keyMap.get(lower);
        if (id == null) {
            throw new IllegalStateException(
                "C-BSON key '" + name + "' not registered. Call ensureKeys before writing documents.");
        }
        return concat(List.of(
            new byte[]{ resolveType(value) },
            ushortLE(id),
            encodeValue(value, keyMap)
        ));
    }

    @SuppressWarnings("unchecked")
    private static byte[] writeArray(List<?> arr, Map<String, Integer> keyMap) {
        var parts = new ArrayList<byte[]>();
        for (int i = 0; i < arr.size(); i++) {
            Object elem = arr.get(i);
            parts.add(concat(List.of(
                new byte[]{ resolveType(elem) },
                ushortLE(i),
                encodeValue(elem, keyMap)
            )));
        }
        parts.add(new byte[]{0x00});
        return wrapWithSize(concat(parts));
    }

    private static byte resolveType(Object value) {
        if (value == null)                           return (byte) 0x0A; // NULL
        if (value instanceof Boolean)                return (byte) 0x08; // BOOLEAN
        if (value instanceof Long)                   return (byte) 0x12; // INT64
        if (value instanceof Integer)                return (byte) 0x10; // INT32
        if (value instanceof Double || value instanceof Float) return (byte) 0x01; // DOUBLE
        if (value instanceof String)                 return (byte) 0x02; // STRING
        if (value instanceof Instant || value instanceof Date) return (byte) 0x09; // DATETIME
        if (value instanceof byte[])                 return (byte) 0x05; // BINARY
        if (value instanceof List<?>)                return (byte) 0x04; // ARRAY
        if (value instanceof Map<?, ?>)              return (byte) 0x03; // DOCUMENT
        throw new IllegalArgumentException("Unsupported C-BSON type: " + value.getClass().getSimpleName());
    }

    @SuppressWarnings("unchecked")
    private static byte[] encodeValue(Object value, Map<String, Integer> keyMap) {
        if (value == null) return new byte[0];

        if (value instanceof Boolean b) {
            return new byte[]{ (byte) (b ? 1 : 0) };
        }
        if (value instanceof Long l) {
            return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(l).array();
        }
        if (value instanceof Integer i) {
            return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(i).array();
        }
        if (value instanceof Double d) {
            return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(d).array();
        }
        if (value instanceof Float f) {
            return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(f.doubleValue()).array();
        }
        if (value instanceof String s) {
            byte[] strBytes = s.getBytes(StandardCharsets.UTF_8);
            byte[] lenBytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(strBytes.length + 1).array();
            return concat(List.of(lenBytes, strBytes, new byte[]{0x00}));
        }
        if (value instanceof Instant inst) {
            return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(inst.toEpochMilli()).array();
        }
        if (value instanceof Date date) {
            return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(date.getTime()).array();
        }
        if (value instanceof byte[] bytes) {
            // Binary: 4-byte LE length + 1-byte subtype + payload
            byte[] lenBytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(bytes.length).array();
            return concat(List.of(lenBytes, new byte[]{0x00}, bytes));
        }
        if (value instanceof List<?> list) {
            return writeArray(list, keyMap);
        }
        if (value instanceof Map<?, ?> map) {
            return encode((Map<String, Object>) map, keyMap);
        }
        throw new IllegalArgumentException("Unsupported C-BSON type: " + value.getClass().getSimpleName());
    }

    private static byte[] wrapWithSize(byte[] body) {
        int size = 4 + body.length;
        return concat(List.of(
            ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(size).array(),
            body
        ));
    }

    private static byte[] ushortLE(int v) {
        return ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN).putShort((short) (v & 0xFFFF)).array();
    }

    private static byte[] concat(List<byte[]> parts) {
        int total = parts.stream().mapToInt(b -> b.length).sum();
        var result = new byte[total];
        int pos = 0;
        for (var part : parts) {
            System.arraycopy(part, 0, result, pos, part.length);
            pos += part.length;
        }
        return result;
    }
}
