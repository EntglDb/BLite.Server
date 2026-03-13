// BLite.Client.Java — LogicalOp
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client.query;

public enum LogicalOp {
    AND(0),
    OR(1);

    public final int code;

    LogicalOp(int code) { this.code = code; }
}
