// BLite.Client.IntegrationTests — AdminTests
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// End-to-end tests for AdminService: user CRUD, key rotation,
// permission updates, and tenant provisioning lifecycle.

using BLite.Client.IntegrationTests.Infrastructure;
using BLite.Proto.V1;
using BLite.Server.Auth;

namespace BLite.Client.IntegrationTests;

[Collection("Integration")]
public class AdminTests : IntegrationTestBase
{
    public AdminTests(BLiteServerFixture fixture) : base(fixture) { }

    // ── User management ───────────────────────────────────────────────────────

    [Fact]
    public async Task CreateUser_ThenListUsers_ContainsUser()
    {
        await using var admin = CreateClient();
        var username = $"u_{Guid.NewGuid():N}";

        await admin.Admin.CreateUserAsync(username, null);

        var users = await admin.Admin.ListUsersAsync();
        Assert.Contains(users, u => u.Username == username);
    }

    [Fact]
    public async Task CreateUser_ReturnsNonEmptyApiKey()
    {
        await using var admin = CreateClient();
        var username = $"u_{Guid.NewGuid():N}";

        var key = await admin.Admin.CreateUserAsync(username, null);

        Assert.NotEmpty(key);
    }

    [Fact]
    public async Task CreateUser_NewUserIsActive()
    {
        await using var admin = CreateClient();
        var username = $"u_{Guid.NewGuid():N}";
        await admin.Admin.CreateUserAsync(username, null);

        var users = await admin.Admin.ListUsersAsync();
        var user = Assert.Single(users, u => u.Username == username);
        Assert.True(user.Active);
    }

    [Fact]
    public async Task RevokeUser_UserIsSetInactive()
    {
        await using var admin = CreateClient();
        var username = $"u_{Guid.NewGuid():N}";
        await admin.Admin.CreateUserAsync(username, null);

        await admin.Admin.RevokeUserAsync(username);

        var users = await admin.Admin.ListUsersAsync();
        var user = Assert.Single(users, u => u.Username == username);
        Assert.False(user.Active);
    }

    [Fact]
    public async Task RotateKey_ReturnsNewKey_DifferentFromOriginal()
    {
        await using var admin = CreateClient();
        var username = $"u_{Guid.NewGuid():N}";
        var original = await admin.Admin.CreateUserAsync(username, null);

        var rotated = await admin.Admin.RotateKeyAsync(username);

        Assert.NotEmpty(rotated);
        Assert.NotEqual(original, rotated);
    }

    [Fact]
    public async Task RotateKey_NewKeyGrantsAccess()
    {
        await using var admin = CreateClient();
        var username = $"u_{Guid.NewGuid():N}";
        var colName  = UniqueCollection();
        await admin.Admin.CreateUserAsync(
            username, null,
            [new UserPermission { Collection = "*", Ops = (int)BLiteOperation.All }]);

        var newKey = await admin.Admin.RotateKeyAsync(username);

        await using var userClient = CreateClient(newKey);
        var col = userClient.GetDynamicCollection(colName);
        var doc = await col.NewDocumentAsync(["x"], b => b.AddInt32("x", 1));
        var id = await col.InsertAsync(doc);
        Assert.False(id.IsEmpty);
    }

    [Fact]
    public async Task UpdatePermissions_RestrictsOperations()
    {
        await using var admin = CreateClient();
        var username = $"u_{Guid.NewGuid():N}";
        var colName  = UniqueCollection();

        // Start with full write access
        var key = await admin.Admin.CreateUserAsync(
            username, null,
            [new UserPermission { Collection = "*", Ops = (int)BLiteOperation.All }]);

        // Downgrade to read-only
        await admin.Admin.UpdatePermissionsAsync(
            username,
            [new UserPermission { Collection = "*", Ops = (int)BLiteOperation.Query }]);

        await using var userClient = CreateClient(key);
        var col = userClient.GetDynamicCollection(colName);
        var doc = await col.NewDocumentAsync(["x"], b => b.AddInt32("x", 1));

        await Assert.ThrowsAsync<Grpc.Core.RpcException>(() => col.InsertAsync(doc));
    }

    [Fact]
    public async Task UpdatePermissions_ExpandsOperations()
    {
        await using var admin = CreateClient();
        var username = $"u_{Guid.NewGuid():N}";
        var colName  = UniqueCollection();

        // Start with read-only
        var key = await admin.Admin.CreateUserAsync(
            username, null,
            [new UserPermission { Collection = "*", Ops = (int)BLiteOperation.Query }]);

        // Upgrade to full write
        await admin.Admin.UpdatePermissionsAsync(
            username,
            [new UserPermission { Collection = "*", Ops = (int)BLiteOperation.All }]);

        await using var userClient = CreateClient(key);
        var col = userClient.GetDynamicCollection(colName);
        var doc = await col.NewDocumentAsync(["x"], b => b.AddInt32("x", 1));
        var id = await col.InsertAsync(doc);
        Assert.False(id.IsEmpty);
    }

    [Fact]
    public async Task ListUsers_AlwaysReturnsNonNullList()
    {
        await using var admin = CreateClient();

        var users = await admin.Admin.ListUsersAsync();

        Assert.NotNull(users);
    }

    // ── Tenant management ─────────────────────────────────────────────────────

    [Fact]
    public async Task ProvisionTenant_ThenListTenants_ContainsTenant()
    {
        await using var admin = CreateClient();
        var tenantId = $"t{Guid.NewGuid():N}";

        await admin.Admin.ProvisionTenantAsync(tenantId);

        var tenants = await admin.Admin.ListTenantsAsync();
        Assert.Contains(tenants, t => t.DatabaseId == tenantId);

        // Cleanup
        await admin.Admin.DeprovisionTenantAsync(tenantId, deleteFiles: true);
    }

    [Fact]
    public async Task DeprovisionTenant_TenantNoLongerActive()
    {
        await using var admin = CreateClient();
        var tenantId = $"t{Guid.NewGuid():N}";

        await admin.Admin.ProvisionTenantAsync(tenantId);
        await admin.Admin.DeprovisionTenantAsync(tenantId, deleteFiles: true);

        var tenants = await admin.Admin.ListTenantsAsync();
        Assert.DoesNotContain(tenants, t => t.DatabaseId == tenantId && t.IsActive);
    }

    [Fact]
    public async Task ListTenants_ReturnsNonNullList()
    {
        await using var admin = CreateClient();

        var tenants = await admin.Admin.ListTenantsAsync();

        Assert.NotNull(tenants);
    }

    [Fact]
    public async Task ProvisionTenant_UserWithTenantDb_CanWrite()
    {
        await using var admin = CreateClient();
        var tenantId = $"t{Guid.NewGuid():N}";
        await admin.Admin.ProvisionTenantAsync(tenantId);

        var tenantKey = await admin.Admin.CreateUserAsync(
            $"u_{Guid.NewGuid():N}", null,
            [new UserPermission { Collection = "*", Ops = (int)BLiteOperation.All }],
            databaseId: tenantId);

        await using var tenantClient = CreateClient(tenantKey);
        var col = tenantClient.GetDynamicCollection(UniqueCollection());
        var doc = await col.NewDocumentAsync(["x"], b => b.AddInt32("x", 42));
        var id = await col.InsertAsync(doc);

        Assert.False(id.IsEmpty);

        // Cleanup
        await admin.Admin.DeprovisionTenantAsync(tenantId, deleteFiles: true);
    }
}
