// BLite.Client.Java — ScalarKind
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client.query;

public enum ScalarKind {
    NULL(0),
    BOOL(1),
    INT32(2),
    INT64(3),
    DOUBLE(4),
    DECIMAL(5),
    STRING(6),
    DATE_TIME(7),
    GUID(8),
    OBJECT_ID(9),
    ARRAY(10);

    public final int code;

    ScalarKind(int code) { this.code = code; }
}
