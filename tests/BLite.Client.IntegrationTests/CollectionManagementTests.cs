// BLite.Client.IntegrationTests — CollectionManagementTests
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// End-to-end tests for ListCollections and DropCollection.

using BLite.Client.IntegrationTests.Infrastructure;

namespace BLite.Client.IntegrationTests;

[Collection("Integration")]
public class CollectionManagementTests : IntegrationTestBase
{
    public CollectionManagementTests(BLiteServerFixture fixture) : base(fixture) { }

    [Fact]
    public async Task ListCollections_AfterInsert_ContainsCollection()
    {
        await using var client = CreateClient();
        var colName = UniqueCollection();
        var col = client.GetDynamicCollection(colName);
        var doc = await col.NewDocumentAsync(["x"], b => b.AddInt32("x", 1));
        await col.InsertAsync(doc);

        var names = await client.ListCollectionsAsync();

        Assert.Contains(colName, names);
    }

    [Fact]
    public async Task ListCollections_ReturnsNonNullList()
    {
        await using var client = CreateClient();

        var names = await client.ListCollectionsAsync();

        Assert.NotNull(names);
    }

    [Fact]
    public async Task ListCollections_MultipleCollections_AllListed()
    {
        await using var client = CreateClient();
        var names = new[] { UniqueCollection(), UniqueCollection(), UniqueCollection() };

        foreach (var name in names)
        {
            var col = client.GetDynamicCollection(name);
            var doc = await col.NewDocumentAsync(["x"], b => b.AddInt32("x", 1));
            await col.InsertAsync(doc);
        }

        var listed = await client.ListCollectionsAsync();
        foreach (var name in names)
            Assert.Contains(name, listed);
    }

    [Fact]
    public async Task DropCollection_ExistingCollection_ReturnsTrueAndRemoves()
    {
        await using var client = CreateClient();
        var colName = UniqueCollection();
        var col = client.GetDynamicCollection(colName);
        var doc = await col.NewDocumentAsync(["x"], b => b.AddInt32("x", 1));
        await col.InsertAsync(doc);

        var dropped = await client.DropCollectionAsync(colName);

        Assert.True(dropped);
        var names = await client.ListCollectionsAsync();
        Assert.DoesNotContain(colName, names);
    }

    [Fact]
    public async Task DropCollection_ExistingCollection_DocumentsNoLongerAccessible()
    {
        await using var client = CreateClient();
        var colName = UniqueCollection();
        var col = client.GetDynamicCollection(colName);
        var doc = await col.NewDocumentAsync(["x"], b => b.AddInt32("x", 1));
        var id = await col.InsertAsync(doc);

        await client.DropCollectionAsync(colName);

        // After drop, FindById on the old ID returns null
        var found = await col.FindByIdAsync(id);
        Assert.Null(found);
    }

    [Fact]
    public async Task DropCollection_NonExistent_ReturnsFalse()
    {
        await using var client = CreateClient();

        var result = await client.DropCollectionAsync($"nonexistent_{Guid.NewGuid():N}");

        Assert.False(result);
    }
}
