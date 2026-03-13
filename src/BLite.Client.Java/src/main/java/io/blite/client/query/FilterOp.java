// BLite.Client.Java — FilterOp
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client.query;

public enum FilterOp {
    EQ(0),
    NOT_EQ(1),
    LT(2),
    LT_EQ(3),
    GT(4),
    GT_EQ(5),
    STARTS_WITH(6),
    CONTAINS(7),
    IN(8);

    public final int code;

    FilterOp(int code) { this.code = code; }
}
