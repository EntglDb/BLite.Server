// BLite.Client.Java — SortSpec
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client.query;

public record SortSpec(String field, boolean descending) {

    public static SortSpec asc(String field)  { return new SortSpec(field, false); }
    public static SortSpec desc(String field) { return new SortSpec(field, true); }
}
