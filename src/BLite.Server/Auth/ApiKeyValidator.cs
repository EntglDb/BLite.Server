// BLite.Server — API Key authentication
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Delegates lookup to UserRepository.  A valid key must resolve to an Active user.
// Open/dev mode is still supported: if no users are stored yet all requests are
// accepted and treated as root.

namespace BLite.Server.Auth;

/// <summary>
/// Resolves an API key to its <see cref="BLiteUser"/>, or returns <c>null</c>
/// when the key is unknown or the user is inactive.
/// When the repository has no users (first-boot before root is initialized) the server
/// operates in open/dev mode — every request resolves to a synthetic root user.
/// </summary>
public sealed class ApiKeyValidator(UserRepository users)
{
    private static readonly BLiteUser DevRoot = new(
        Username:    "root",
        ApiKeyHash:  string.Empty,
        Namespace:   null,
        Permissions: [new PermissionEntry("*", BLiteOperation.All)],
        Active:      true,
        CreatedAt:   DateTime.UtcNow);

    public BLiteUser? Resolve(string? key)
    {
        // Dev mode: no users stored → treat every call as root.
        if (users.ListAll().Count == 0) return DevRoot;
        if (key is null) return null;
        return users.FindByKey(key);
    }
}
