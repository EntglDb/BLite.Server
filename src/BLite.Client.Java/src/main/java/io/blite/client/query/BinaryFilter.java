// BLite.Client.Java — BinaryFilter
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client.query;

public record BinaryFilter(String field, FilterOp op, ScalarValue value) implements FilterNode {}
