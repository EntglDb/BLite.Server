// BLite.Client.Java — BsonIdType
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client.bson;

public enum BsonIdType {
    OBJECT_ID(0),
    STRING(1),
    INT32(2),
    INT64(3),
    GUID(4);

    public final int code;

    BsonIdType(int code) { this.code = code; }

    public static BsonIdType fromCode(int code) {
        for (var v : values()) {
            if (v.code == code) return v;
        }
        throw new IllegalArgumentException("Unknown BsonIdType code: " + code);
    }
}
