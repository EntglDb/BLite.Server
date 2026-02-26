// BLite.Client.IntegrationTests — SmokeTests
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Basic connectivity and server availability tests.

using BLite.Client.IntegrationTests.Infrastructure;

namespace BLite.Client.IntegrationTests;

[Collection("Integration")]
public class SmokeTests : IntegrationTestBase
{
    public SmokeTests(BLiteServerFixture fixture) : base(fixture) { }

    // ── Server availability ───────────────────────────────────────────────────

    [Fact]
    public async Task ListCollectionsAsync_Succeeds()
    {
        await using var client = CreateClient();
        var collections = await client.ListCollectionsAsync();

        // Don't assert on count — the "Integration" collection fixture may have
        // created collections in earlier tests.  We only verify the call works.
        Assert.NotNull(collections);
    }

    [Fact]
    public async Task GetDynamicCollection_Insert_FindById_BasicRoundTrip()
    {
        await using var client = CreateClient();
        var col = client.GetDynamicCollection(UniqueCollection());

        var doc = await col.NewDocumentAsync(
            ["name"],
            b => b.AddString("name", "smoke-test"));

        var id    = await col.InsertAsync(doc);
        var found = await col.FindByIdAsync(id);

        Assert.NotNull(found);
        Assert.True(found.TryGetString("name", out var name));
        Assert.Equal("smoke-test", name);
    }

    [Fact]
    public async Task DropCollectionAsync_AfterInsert_CollectionDisappears()
    {
        await using var client = CreateClient();
        var colName = UniqueCollection();
        var col     = client.GetDynamicCollection(colName);

        var doc = await col.NewDocumentAsync(["x"], b => b.AddInt32("x", 1));
        await col.InsertAsync(doc);

        var before = await client.ListCollectionsAsync();
        Assert.Contains(colName, before);

        await client.DropCollectionAsync(colName);

        var after = await client.ListCollectionsAsync();
        Assert.DoesNotContain(colName, after);
    }
}
