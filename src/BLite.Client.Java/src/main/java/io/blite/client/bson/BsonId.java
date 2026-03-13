// BLite.Client.Java — BsonId
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client.bson;

import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Opaque document identifier. Mirrors the {@code BsonIdBytes} proto message. */
public final class BsonId {

    private final byte[]     value;
    private final BsonIdType type;

    public BsonId(byte[] value, BsonIdType type) {
        this.value = value.clone();
        this.type  = type;
    }

    public byte[]     getValue() { return value.clone(); }
    public BsonIdType getType()  { return type; }

    // ── Factory methods ──────────────────────────────────────────────────────

    public static BsonId fromObjectId(String hex) {
        return new BsonId(hexToBytes(hex), BsonIdType.OBJECT_ID);
    }

    public static BsonId fromObjectIdBytes(byte[] bytes) {
        if (bytes.length != 12) throw new IllegalArgumentException("ObjectId must be 12 bytes");
        return new BsonId(bytes, BsonIdType.OBJECT_ID);
    }

    public static BsonId fromString(String s) {
        return new BsonId(s.getBytes(StandardCharsets.UTF_8), BsonIdType.STRING);
    }

    public static BsonId fromInt32(int n) {
        return new BsonId(ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(n).array(), BsonIdType.INT32);
    }

    public static BsonId fromInt64(long n) {
        return new BsonId(ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(n).array(), BsonIdType.INT64);
    }

    public static BsonId fromGuid(String s) {
        return new BsonId(s.getBytes(StandardCharsets.UTF_8), BsonIdType.GUID);
    }

    // ── Proto conversion ─────────────────────────────────────────────────────

    public static BsonId fromProto(ByteString protoValue, int idType) {
        return new BsonId(protoValue.toByteArray(), BsonIdType.fromCode(idType));
    }

    public ByteString toProtoValue() {
        return ByteString.copyFrom(value);
    }

    // ── Equality / toString ──────────────────────────────────────────────────

    @Override
    public String toString() {
        return switch (type) {
            case OBJECT_ID          -> bytesToHex(value);
            case STRING, GUID       -> new String(value, StandardCharsets.UTF_8);
            case INT32              -> String.valueOf(ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN).getInt());
            case INT64              -> String.valueOf(ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN).getLong());
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BsonId other)) return false;
        return type == other.type && Arrays.equals(value, other.value);
    }

    @Override
    public int hashCode() {
        return 31 * type.hashCode() + Arrays.hashCode(value);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static String bytesToHex(byte[] bytes) {
        var sb = new StringBuilder(bytes.length * 2);
        for (var b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    private static byte[] hexToBytes(String hex) {
        int len  = hex.length();
        var data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                    + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }
}
