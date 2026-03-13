// BLite.Client.Java — QueryDescriptor
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client.query;

import java.util.List;

/**
 * Describes a query to execute on the server.
 * Mirrors the C# {@code QueryDescriptor} that is MessagePack-serialized and
 * sent as the {@code query_descriptor} field of {@code QueryRequest}.
 */
public record QueryDescriptor(
        String collection,
        FilterNode where,
        List<SortSpec> orderBy,
        Integer take,
        Integer skip
) {}
