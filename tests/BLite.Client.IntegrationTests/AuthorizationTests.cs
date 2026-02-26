// BLite.Client.IntegrationTests — AuthorizationTests
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// End-to-end tests for permission enforcement:
// - Invalid / revoked API keys are rejected (Unauthenticated)
// - Users without the required permission are rejected (PermissionDenied)
// - Read-only users can still query
// - Non-admin users are blocked from AdminService RPCs

using BLite.Client.IntegrationTests.Infrastructure;
using BLite.Proto.V1;
using BLite.Server.Auth;
using Grpc.Core;

namespace BLite.Client.IntegrationTests;

[Collection("Integration")]
public class AuthorizationTests : IntegrationTestBase
{
    public AuthorizationTests(BLiteServerFixture fixture) : base(fixture) { }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private async Task<string> CreateUserKeyAsync(BLiteOperation ops, string collection = "*")
    {
        await using var admin = CreateClient();
        var username = $"u_{Guid.NewGuid():N}";
        return await admin.Admin.CreateUserAsync(
            username, null,
            [new UserPermission { Collection = collection, Ops = (int)ops }]);
    }

    private async Task<(string Username, string Key)> CreateUserAsync(BLiteOperation ops)
    {
        await using var admin = CreateClient();
        var username = $"u_{Guid.NewGuid():N}";
        var key = await admin.Admin.CreateUserAsync(
            username, null,
            [new UserPermission { Collection = "*", Ops = (int)ops }]);
        return (username, key);
    }

    // ── Invalid / revoked keys ────────────────────────────────────────────────

    [Fact]
    public async Task InvalidApiKey_Insert_ThrowsUnauthenticated()
    {
        var colName = UniqueCollection();

        // Create doc with valid root client
        await using var rootClient = CreateClient();
        var rootCol = rootClient.GetDynamicCollection(colName);
        var doc = await rootCol.NewDocumentAsync(["x"], b => b.AddInt32("x", 1));

        // Try to insert with invalid key
        await using var invalidClient = CreateClient(apiKey: "blt_completely_invalid_key_99999");
        var invalidCol = invalidClient.GetDynamicCollection(colName);

        var ex = await Assert.ThrowsAsync<RpcException>(() => invalidCol.InsertAsync(doc));
        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
    }

    [Fact]
    public async Task RevokedUser_Insert_ThrowsPermissionDeniedOrUnauthenticated()
    {
        var colName = UniqueCollection();
        var (username, key) = await CreateUserAsync(BLiteOperation.All);

        // Create doc with valid key before revoking
        await using var userClient = CreateClient(key);
        var col = userClient.GetDynamicCollection(colName);
        var doc = await col.NewDocumentAsync(["x"], b => b.AddInt32("x", 1));

        // Revoke the user
        await using var admin = CreateClient();
        await admin.Admin.RevokeUserAsync(username);

        // Try to insert with revoked key
        var ex = await Assert.ThrowsAsync<RpcException>(() => col.InsertAsync(doc));
        Assert.True(
            ex.StatusCode is StatusCode.PermissionDenied or StatusCode.Unauthenticated,
            $"Expected PermissionDenied or Unauthenticated, got {ex.StatusCode}");
    }

    // ── Read-only user ────────────────────────────────────────────────────────

    [Fact]
    public async Task ReadOnlyUser_Insert_ThrowsPermissionDenied()
    {
        var colName = UniqueCollection();

        // Create doc with root
        await using var rootClient = CreateClient();
        var rootCol = rootClient.GetDynamicCollection(colName);
        var doc = await rootCol.NewDocumentAsync(["x"], b => b.AddInt32("x", 1));

        // Try to insert with read-only client
        var roKey = await CreateUserKeyAsync(BLiteOperation.Query, colName);
        await using var roClient = CreateClient(roKey);
        var roCol = roClient.GetDynamicCollection(colName);

        var ex = await Assert.ThrowsAsync<RpcException>(() => roCol.InsertAsync(doc));
        Assert.Equal(StatusCode.PermissionDenied, ex.StatusCode);
    }

    [Fact]
    public async Task ReadOnlyUser_Update_ThrowsPermissionDenied()
    {
        var colName = UniqueCollection();

        // Root inserts a document
        await using var admin = CreateClient();
        var rootCol = admin.GetDynamicCollection(colName);
        var d = await rootCol.NewDocumentAsync(["x"], b => b.AddInt32("x", 1));
        var id = await rootCol.InsertAsync(d);

        var roKey = await CreateUserKeyAsync(BLiteOperation.Query, colName);
        await using var roClient = CreateClient(roKey);
        var roCol = roClient.GetDynamicCollection(colName);

        // Create updated doc with root (so no auth error during doc creation)
        var updated = await rootCol.NewDocumentAsync(["x"], b => b.AddInt32("x", 99));

        // Try to update with read-only client
        var ex = await Assert.ThrowsAsync<RpcException>(() => roCol.UpdateAsync(id, updated));
        Assert.Equal(StatusCode.PermissionDenied, ex.StatusCode);
    }

    [Fact]
    public async Task ReadOnlyUser_Delete_ThrowsPermissionDenied()
    {
        var colName = UniqueCollection();

        await using var admin = CreateClient();
        var rootCol = admin.GetDynamicCollection(colName);
        var d = await rootCol.NewDocumentAsync(["x"], b => b.AddInt32("x", 1));
        var id = await rootCol.InsertAsync(d);

        var roKey = await CreateUserKeyAsync(BLiteOperation.Query, colName);
        await using var roClient = CreateClient(roKey);
        var roCol = roClient.GetDynamicCollection(colName);

        var ex = await Assert.ThrowsAsync<RpcException>(() => roCol.DeleteAsync(id));
        Assert.Equal(StatusCode.PermissionDenied, ex.StatusCode);
    }

    [Fact]
    public async Task ReadOnlyUser_Query_Succeeds()
    {
        var colName = UniqueCollection();

        await using var admin = CreateClient();
        var rootCol = admin.GetDynamicCollection(colName);
        var d = await rootCol.NewDocumentAsync(["x"], b => b.AddInt32("x", 42));
        await rootCol.InsertAsync(d);

        var roKey = await CreateUserKeyAsync(BLiteOperation.Query, "*");
        await using var roClient = CreateClient(roKey);
        var roCol = roClient.GetDynamicCollection(colName);

        var results = new List<Bson.BsonDocument>();
        await foreach (var doc in roCol.FindAllAsync())
            results.Add(doc);

        Assert.NotEmpty(results);
    }

    [Fact]
    public async Task ReadOnlyUser_FindById_Succeeds()
    {
        var colName = UniqueCollection();

        await using var admin = CreateClient();
        var rootCol = admin.GetDynamicCollection(colName);
        var d = await rootCol.NewDocumentAsync(["x"], b => b.AddInt32("x", 5));
        var id = await rootCol.InsertAsync(d);

        var roKey = await CreateUserKeyAsync(BLiteOperation.Query, colName);
        await using var roClient = CreateClient(roKey);
        var roCol = roClient.GetDynamicCollection(colName);

        var found = await roCol.FindByIdAsync(id);
        Assert.NotNull(found);
    }

    // ── Admin-only operations ──────────────────────────────────────────────────

    [Fact]
    public async Task ReadOnlyUser_CreateIndex_ThrowsPermissionDenied()
    {
        var colName = UniqueCollection();
        var roKey   = await CreateUserKeyAsync(BLiteOperation.Query);

        await using var roClient = CreateClient(roKey);
        var roCol = roClient.GetDynamicCollection(colName);

        var ex = await Assert.ThrowsAsync<RpcException>(
            () => roCol.CreateIndexAsync("field"));
        Assert.Equal(StatusCode.PermissionDenied, ex.StatusCode);
    }

    [Fact]
    public async Task NonAdminUser_ListUsers_ThrowsPermissionDenied()
    {
        var (_, key) = await CreateUserAsync(BLiteOperation.Write);

        await using var userClient = CreateClient(key);

        var ex = await Assert.ThrowsAsync<RpcException>(
            () => userClient.Admin.ListUsersAsync());
        Assert.Equal(StatusCode.PermissionDenied, ex.StatusCode);
    }

    [Fact]
    public async Task NonAdminUser_CreateUser_ThrowsPermissionDenied()
    {
        var (_, key) = await CreateUserAsync(BLiteOperation.Write);

        await using var userClient = CreateClient(key);

        var ex = await Assert.ThrowsAsync<RpcException>(
            () => userClient.Admin.CreateUserAsync($"u_{Guid.NewGuid():N}", null));
        Assert.Equal(StatusCode.PermissionDenied, ex.StatusCode);
    }

    // ── Write-only vs. read ───────────────────────────────────────────────────

    [Fact]
    public async Task WriteOnlyUser_Insert_Succeeds()
    {
        var colName = UniqueCollection();
        var woKey = await CreateUserKeyAsync(BLiteOperation.Insert, colName);

        await using var woClient = CreateClient(woKey);
        var col = woClient.GetDynamicCollection(colName);
        var doc = await col.NewDocumentAsync(["x"], b => b.AddInt32("x", 7));

        var id = await col.InsertAsync(doc);
        Assert.False(id.IsEmpty);
    }

    [Fact]
    public async Task WriteOnlyUser_FindById_ThrowsPermissionDenied()
    {
        var colName = UniqueCollection();

        await using var admin = CreateClient();
        var rootCol = admin.GetDynamicCollection(colName);
        var d = await rootCol.NewDocumentAsync(["x"], b => b.AddInt32("x", 1));
        var id = await rootCol.InsertAsync(d);

        var woKey = await CreateUserKeyAsync(BLiteOperation.Write, colName);
        await using var woClient = CreateClient(woKey);
        var woCol = woClient.GetDynamicCollection(colName);

        var ex = await Assert.ThrowsAsync<RpcException>(() => woCol.FindByIdAsync(id));
        Assert.Equal(StatusCode.PermissionDenied, ex.StatusCode);
    }
}
