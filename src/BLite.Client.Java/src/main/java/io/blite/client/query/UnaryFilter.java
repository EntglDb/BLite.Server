// BLite.Client.Java — UnaryFilter
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client.query;

public record UnaryFilter(FilterNode operand) implements FilterNode {}
