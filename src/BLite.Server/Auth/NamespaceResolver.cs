// BLite.Server — Namespace isolation
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Root users (Namespace == null) see collection names unchanged.
// Namespaced users work inside a transparent prefix:
//   client sends "orders" → engine stores "tenant1:orders"

namespace BLite.Server.Auth;

public static class NamespaceResolver
{
    private const char Sep = ':';

    /// <summary>
    /// Returns the physical (engine-level) collection name for <paramref name="user"/>.
    /// </summary>
    public static string Resolve(BLiteUser user, string collection)
        => user.Namespace is null
            ? collection
            : $"{user.Namespace}{Sep}{collection}";

    /// <summary>
    /// Strips the namespace prefix to produce the logical (client-facing) name.
    /// If the name does not carry the user's prefix it is returned unchanged.
    /// </summary>
    public static string Strip(BLiteUser user, string physicalName)
    {
        if (user.Namespace is null) return physicalName;

        var prefix = $"{user.Namespace}{Sep}";
        return physicalName.StartsWith(prefix, StringComparison.Ordinal)
            ? physicalName[prefix.Length..]
            : physicalName;
    }

    /// <summary>
    /// Returns <c>true</c> if <paramref name="physicalName"/> belongs to <paramref name="user"/>'s namespace.
    /// Root users own every collection.
    /// </summary>
    public static bool BelongsTo(BLiteUser user, string physicalName)
        => user.Namespace is null
            || physicalName.StartsWith($"{user.Namespace}{Sep}", StringComparison.Ordinal);
}
