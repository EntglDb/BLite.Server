// BLite.Client.Java — CbsonReader
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Decodes C-BSON bytes into a Map<String, Object>.
// Unknown field IDs (not in the reverse map) fall back to their numeric string.

package io.blite.client.bson;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class CbsonReader {

    private CbsonReader() {}

    /**
     * Decodes C-BSON bytes into a document.
     *
     * @param bytes      Raw C-BSON bytes.
     * @param reverseMap Map of ushort ID → lower-case field name.
     */
    public static Map<String, Object> decode(byte[] bytes, Map<Integer, String> reverseMap) {
        return readDocument(new Cursor(bytes, 0), reverseMap);
    }

    // ── Document / array decoding ────────────────────────────────────────────

    private static Map<String, Object> readDocument(Cursor c, Map<Integer, String> reverseMap) {
        int docEnd = c.pos + c.readInt32() - 4;
        var doc    = new LinkedHashMap<String, Object>();

        while (c.pos < docEnd) {
            int type = c.readUInt8();
            if (type == 0x00) break; // END_OF_DOCUMENT
            int    id   = c.readUInt16();
            String name = reverseMap.getOrDefault(id, String.valueOf(id));
            doc.put(name, readValue(type, c, reverseMap));
        }
        return doc;
    }

    private static List<Object> readArray(Cursor c, Map<Integer, String> reverseMap) {
        int arrEnd = c.pos + c.readInt32() - 4;
        var arr    = new ArrayList<Object>();

        while (c.pos < arrEnd) {
            int type = c.readUInt8();
            if (type == 0x00) break;
            c.skip(2); // positional ushort index
            arr.add(readValue(type, c, reverseMap));
        }
        return arr;
    }

    private static Object readValue(int type, Cursor c, Map<Integer, String> reverseMap) {
        return switch (type) {
            case 0x0A -> null;
            case 0x08 -> c.readUInt8() != 0;
            case 0x10 -> c.readInt32();
            case 0x12, 0x11 -> c.readInt64();
            case 0x01 -> c.readDouble();
            case 0x09 -> new Date(c.readInt64());
            case 0x02 -> {
                int    len = c.readInt32();
                String s   = c.readString(len - 1);
                c.skip(1); // null terminator
                yield s;
            }
            case 0x05 -> {
                int    len     = c.readInt32();
                c.skip(1);     // subtype
                yield c.readBytes(len);
            }
            case 0x03 -> readDocument(c, reverseMap);
            case 0x04 -> readArray(c, reverseMap);
            case 0x07 -> c.readBytes(12); // ObjectId
            default   -> throw new IllegalStateException("Unknown BSON type: 0x" + Integer.toHexString(type));
        };
    }

    // ── Internal cursor ──────────────────────────────────────────────────────

    private static final class Cursor {
        final byte[] buf;
        int pos;

        Cursor(byte[] buf, int pos) {
            this.buf = buf;
            this.pos = pos;
        }

        int readInt32() {
            int v = ByteBuffer.wrap(buf, pos, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
            pos += 4;
            return v;
        }

        int readUInt16() {
            int v = ByteBuffer.wrap(buf, pos, 2).order(ByteOrder.LITTLE_ENDIAN).getShort() & 0xFFFF;
            pos += 2;
            return v;
        }

        int readUInt8() {
            return buf[pos++] & 0xFF;
        }

        long readInt64() {
            long v = ByteBuffer.wrap(buf, pos, 8).order(ByteOrder.LITTLE_ENDIAN).getLong();
            pos += 8;
            return v;
        }

        double readDouble() {
            double v = ByteBuffer.wrap(buf, pos, 8).order(ByteOrder.LITTLE_ENDIAN).getDouble();
            pos += 8;
            return v;
        }

        String readString(int len) {
            String s = new String(buf, pos, len, StandardCharsets.UTF_8);
            pos += len;
            return s;
        }

        byte[] readBytes(int len) {
            byte[] b = Arrays.copyOfRange(buf, pos, pos + len);
            pos += len;
            return b;
        }

        void skip(int n) { pos += n; }
    }
}
