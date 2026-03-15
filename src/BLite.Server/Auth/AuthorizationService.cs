// BLite.Server — Authorization check
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Pure in-memory logic — no I/O.  Inject as Singleton.

using Grpc.Core;

namespace BLite.Server.Auth;

public sealed class AuthorizationService
{
    /// <summary>
    /// Throws <see cref="RpcException"/> (<c>PermissionDenied</c>) if
    /// <paramref name="user"/> may not perform <paramref name="op"/> on <paramref name="collection"/>.
    /// </summary>
    public void RequirePermission(BLiteUser user, string collection, BLiteOperation op)
    {
        if (!CheckPermission(user, collection, op))
            throw new RpcException(new Status(
                StatusCode.PermissionDenied,
                $"User '{user.Username}' lacks {op} on '{collection}'."));
    }

    /// <summary>
    /// Returns <c>true</c> if <paramref name="user"/> may perform <paramref name="op"/>
    /// on <paramref name="collection"/>.
    /// </summary>
    public bool CheckPermission(BLiteUser user, string collection, BLiteOperation op)
    {
        if (!user.Active) return false;

        // Collections starting with '_' are reserved.
        // The caller must hold Admin permission on '*' or the specific collection.
        if (collection.StartsWith('_'))
        {
            bool hasAdmin = user.Permissions.Any(p =>
                (p.Collection == "*" || p.Collection.Equals(collection, StringComparison.Ordinal)) &&
                (p.Ops & BLiteOperation.Admin) != 0);
            if (!hasAdmin) return false;
        }

        foreach (var entry in user.Permissions)
        {
            bool matches = entry.Collection == "*"
                || entry.Collection.Equals(collection, StringComparison.Ordinal);

            if (matches && (entry.Ops & op) == op)
                return true;
        }

        return false;
    }
}
