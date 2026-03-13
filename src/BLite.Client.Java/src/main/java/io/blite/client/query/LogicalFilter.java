// BLite.Client.Java — LogicalFilter
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client.query;

import java.util.List;

public record LogicalFilter(LogicalOp op, List<FilterNode> children) implements FilterNode {}
