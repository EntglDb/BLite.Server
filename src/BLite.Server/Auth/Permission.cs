// BLite.Server — Auth models
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

using BLite.Bson;

namespace BLite.Server.Auth;

/// <summary>
/// Bit-flags for the operations that can be granted per collection.
/// Composite values (Write, All) are provided for convenience.
/// </summary>
[Flags]
public enum BLiteOperation
{
    None   = 0,
    Query  = 1,
    Insert = 2,
    Update = 4,
    Delete = 8,
    Drop   = 16,
    Admin  = 32,
    // Composites
    Write  = Insert | Update | Delete,
    All    = Query | Write | Drop | Admin
}

/// <summary>
/// Grants a set of <see cref="BLiteOperation"/> flags on a specific collection.
/// Use <c>"*"</c> as <see cref="Collection"/> to match every collection in the user's namespace.
/// </summary>
public sealed record PermissionEntry(string Collection, BLiteOperation Ops);

/// <summary>
/// A BLite Server principal, identified by an API key (stored as SHA-256 hash).
/// </summary>
/// <param name="Username">Unique login name.</param>
/// <param name="ApiKeyHash">Hex-encoded SHA-256 hash — the plaintext key is never stored.</param>
/// <param name="Namespace">
/// Optional namespace prefix.  All collection names are transparently prefixed with
/// <c>"&lt;Namespace&gt;:"</c> before hitting the engine.  <c>null</c> = root (no prefix).
/// </param>
/// <param name="Permissions">Per-collection operation grants.</param>
/// <param name="Active">When <c>false</c> the key is revoked.</param>
/// <param name="CreatedAt">UTC creation timestamp.</param>
/// <param name="StoredId">BsonId assigned by the engine — set when the user is loaded from DB.</param>
public sealed record BLiteUser(
    string                         Username,
    string                         ApiKeyHash,
    string?                        Namespace,
    IReadOnlyList<PermissionEntry> Permissions,
    bool                           Active,
    DateTime                       CreatedAt,
    BsonId?                        StoredId = null);
