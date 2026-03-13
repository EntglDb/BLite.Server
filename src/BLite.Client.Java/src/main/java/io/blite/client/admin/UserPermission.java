// BLite.Client.Java — UserPermission
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client.admin;

/**
 * Represents a permission entry for a user on a collection.
 *
 * <p>{@code ops} is a bitmask of {@code BLiteOperation} flags:
 * Query=1, Insert=2, Update=4, Delete=8, Drop=16, Admin=32.
 * Use the {@code OPS_*} constants or combine them with {@code |}.
 */
public record UserPermission(String collection, int ops) {

    public static final int OPS_QUERY  = 1;
    public static final int OPS_INSERT = 2;
    public static final int OPS_UPDATE = 4;
    public static final int OPS_DELETE = 8;
    public static final int OPS_DROP   = 16;
    public static final int OPS_ADMIN  = 32;
    public static final int OPS_WRITE  = OPS_INSERT | OPS_UPDATE | OPS_DELETE;
    public static final int OPS_ALL    = OPS_QUERY | OPS_WRITE | OPS_DROP | OPS_ADMIN;

    /** Wildcard collection entry (matches any collection). */
    public static UserPermission all(int ops) { return new UserPermission("*", ops); }
}
