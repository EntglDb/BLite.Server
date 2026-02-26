// BLite.Client — RemoteAdminClient
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

using BLite.Proto.V1;
using Grpc.Core;

namespace BLite.Client.Admin;

/// <summary>
/// Wraps the <c>AdminService</c> gRPC stub for user and permission management.
/// Requires the caller's API key to carry the <c>Admin</c> permission.
/// Obtain via <see cref="BLiteClient.Admin"/>.
/// </summary>
public sealed class RemoteAdminClient
{
    private readonly AdminService.AdminServiceClient _stub;
    private readonly Metadata _headers;

    internal RemoteAdminClient(
        AdminService.AdminServiceClient stub, Metadata headers)
    {
        _stub    = stub;
        _headers = headers;
    }

    // ── User management ───────────────────────────────────────────────────────

    /// <summary>
    /// Creates a new BLite user and returns the one-time plaintext API key.
    /// </summary>
    /// <param name="databaseId">
    /// Optional physical database this user belongs to (tenant isolation).
    /// <c>null</c> / empty = default (system) database.
    /// The database must already exist — provision it first with
    /// <see cref="ProvisionTenantAsync"/> if needed.
    /// </param>
    public async Task<string> CreateUserAsync(
        string username,
        string? @namespace,
        IEnumerable<UserPermission>? permissions = null,
        string? databaseId = null,
        CancellationToken ct = default)
    {
        var request = new CreateUserRequest
        {
            Username   = username,
            Namespace  = @namespace ?? string.Empty,
            DatabaseId = databaseId ?? string.Empty
        };
        if (permissions is not null)
            request.Permissions.AddRange(permissions);

        var response = await _stub.CreateUserAsync(request, _headers, cancellationToken: ct);

        if (!string.IsNullOrEmpty(response.Error))
            throw new InvalidOperationException($"CreateUser failed: {response.Error}");

        return response.ApiKey;
    }

    /// <summary>Revokes the specified user's API key (disables the account).</summary>
    public async Task RevokeUserAsync(string username, CancellationToken ct = default)
    {
        var response = await _stub.RevokeUserAsync(
            new UsernameRequest { Username = username }, _headers, cancellationToken: ct);

        if (!response.Success)
            throw new InvalidOperationException($"RevokeUser failed: {response.Error}");
    }

    /// <summary>Rotates the API key for the specified user and returns the new plaintext key.</summary>
    public async Task<string> RotateKeyAsync(string username, CancellationToken ct = default)
    {
        var response = await _stub.RotateKeyAsync(
            new UsernameRequest { Username = username }, _headers, cancellationToken: ct);

        if (!string.IsNullOrEmpty(response.Error))
            throw new InvalidOperationException($"RotateKey failed: {response.Error}");

        return response.ApiKey;
    }

    /// <summary>Returns all users registered on the server.</summary>
    public async Task<IReadOnlyList<UserInfo>> ListUsersAsync(CancellationToken ct = default)
    {
        var response = await _stub.ListUsersAsync(
            new Empty(), _headers, cancellationToken: ct);

        if (!string.IsNullOrEmpty(response.Error))
            throw new InvalidOperationException($"ListUsers failed: {response.Error}");

        return [.. response.Users];
    }

    /// <summary>Replaces the permission set for the specified user.</summary>
    public async Task UpdatePermissionsAsync(
        string username,
        IEnumerable<UserPermission> permissions,
        CancellationToken ct = default)
    {
        var request = new UpdatePermsRequest { Username = username };
        request.Permissions.AddRange(permissions);

        var response = await _stub.UpdatePermsAsync(request, _headers, cancellationToken: ct);

        if (!response.Success)
            throw new InvalidOperationException($"UpdatePerms failed: {response.Error}");
    }

    // ── Tenant provisioning ───────────────────────────────────────────────────

    /// <summary>
    /// Creates a new physical database for the given tenant identifier
    /// (no server restart required).
    /// The <paramref name="databaseId"/> is lowercased and trimmed by the server.
    /// </summary>
    public async Task ProvisionTenantAsync(string databaseId, CancellationToken ct = default)
    {
        var response = await _stub.ProvisionTenantAsync(
            new ProvisionTenantRequest { DatabaseId = databaseId },
            _headers, cancellationToken: ct);

        if (!response.Success)
            throw new InvalidOperationException($"ProvisionTenant failed: {response.Error}");
    }

    /// <summary>
    /// Removes a tenant database from the server.
    /// </summary>
    /// <param name="deleteFiles">
    /// When <c>true</c> the underlying <c>.db</c> file is permanently deleted from disk
    /// (GDPR right-to-erasure). When <c>false</c> only the in-memory engine is closed;
    /// the file can be re-opened later with another <see cref="ProvisionTenantAsync"/> call.
    /// </param>
    public async Task DeprovisionTenantAsync(
        string databaseId, bool deleteFiles = false, CancellationToken ct = default)
    {
        var response = await _stub.DeprovisionTenantAsync(
            new DeprovisionTenantRequest
            {
                DatabaseId  = databaseId,
                DeleteFiles = deleteFiles
            },
            _headers, cancellationToken: ct);

        if (!response.Success)
            throw new InvalidOperationException($"DeprovisionTenant failed: {response.Error}");
    }

    /// <summary>
    /// Returns all tenant databases known to the server (provisioned or file-discovered).
    /// </summary>
    public async Task<IReadOnlyList<TenantInfo>> ListTenantsAsync(CancellationToken ct = default)
    {
        var response = await _stub.ListTenantsAsync(
            new Empty(), _headers, cancellationToken: ct);

        if (!string.IsNullOrEmpty(response.Error))
            throw new InvalidOperationException($"ListTenants failed: {response.Error}");

        return [.. response.Tenants];
    }
}
