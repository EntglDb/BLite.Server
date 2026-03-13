// BLite.Client.Java — BsonType
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client.bson;

public enum BsonType {
    END_OF_DOCUMENT(0x00),
    DOUBLE(0x01),
    STRING(0x02),
    DOCUMENT(0x03),
    ARRAY(0x04),
    BINARY(0x05),
    OBJECT_ID(0x07),
    BOOLEAN(0x08),
    DATE_TIME(0x09),
    NULL(0x0A),
    INT32(0x10),
    TIMESTAMP(0x11),
    INT64(0x12);

    public final int code;

    BsonType(int code) { this.code = code; }
}
