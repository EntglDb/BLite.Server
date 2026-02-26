// BLite.Client.IntegrationTests — DynamicCollectionTests
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// End-to-end tests for RemoteDynamicCollection CRUD operations.

using BLite.Bson;
using BLite.Client.Collections;
using BLite.Client.IntegrationTests.Infrastructure;

namespace BLite.Client.IntegrationTests;

[Collection("Integration")]
public class DynamicCollectionTests : IntegrationTestBase
{
    public DynamicCollectionTests(BLiteServerFixture fixture) : base(fixture) { }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private async Task<(BLiteClient Client, RemoteDynamicCollection Col)> SetupAsync()
    {
        var client = CreateClient();
        var col    = client.GetDynamicCollection(UniqueCollection());
        // Prime the key map with common fields used throughout these tests.
        _ = await col.NewDocumentAsync(["name", "value", "tag"], _ => { });
        return (client, col);
    }

    private static Task<BsonDocument> MakeDocAsync(
        RemoteDynamicCollection col, string name, int value, string tag = "default")
        => col.NewDocumentAsync(
            ["name", "value", "tag"],
            b => b.AddString("name", name)
                  .AddInt32("value", value)
                  .AddString("tag", tag));

    // ── Insert / FindById ─────────────────────────────────────────────────────

    [Fact]
    public async Task InsertAsync_ReturnsNonEmptyId()
    {
        await using var client = CreateClient();
        var col = client.GetDynamicCollection(UniqueCollection());
        var doc = await col.NewDocumentAsync(["x"], b => b.AddInt32("x", 42));

        var id = await col.InsertAsync(doc);

        Assert.False(id.IsEmpty);
    }

    [Fact]
    public async Task FindByIdAsync_ExistingDocument_ReturnsDocument()
    {
        var (client, col) = await SetupAsync();
        await using var _ = client;

        var doc   = await MakeDocAsync(col, "alice", 10);
        var id    = await col.InsertAsync(doc);
        var found = await col.FindByIdAsync(id);

        Assert.NotNull(found);
        Assert.True(found.TryGetString("name", out var name));
        Assert.Equal("alice", name);
        Assert.True(found.TryGetInt32("value", out var value));
        Assert.Equal(10, value);
    }

    [Fact]
    public async Task FindByIdAsync_NonExistentId_ReturnsNull()
    {
        await using var client = CreateClient();
        var col    = client.GetDynamicCollection(UniqueCollection());
        var fakeId = BsonId.NewId(BsonIdType.ObjectId);

        var found = await col.FindByIdAsync(fakeId);

        Assert.Null(found);
    }

    // ── FindAll ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task FindAllAsync_AfterInsertMany_ReturnsAllDocuments()
    {
        var (client, col) = await SetupAsync();
        await using var _ = client;

        for (int i = 1; i <= 5; i++)
        {
            var doc = await MakeDocAsync(col, $"user{i}", i);
            await col.InsertAsync(doc);
        }

        var all = new List<BsonDocument>();
        await foreach (var d in col.FindAllAsync())
            all.Add(d);

        Assert.Equal(5, all.Count);
    }

    // ── Find (client-side predicate) ──────────────────────────────────────────

    [Fact]
    public async Task FindAsync_WithPredicate_ReturnsOnlyMatchingDocuments()
    {
        var (client, col) = await SetupAsync();
        await using var _ = client;

        for (int i = 1; i <= 6; i++)
        {
            var tag = i % 2 == 0 ? "even" : "odd";
            var doc = await MakeDocAsync(col, $"item{i}", i, tag);
            await col.InsertAsync(doc);
        }

        var evens = new List<BsonDocument>();
        await foreach (var d in col.FindAsync(
            d => d.TryGetString("tag", out var t) && t == "even"))
        {
            evens.Add(d);
        }

        Assert.Equal(3, evens.Count);
    }

    // ── InsertBulk ────────────────────────────────────────────────────────────

    [Fact]
    public async Task InsertBulkAsync_ReturnsCorrectNumberOfIds()
    {
        var (client, col) = await SetupAsync();
        await using var _ = client;

        var docs = new List<BsonDocument>();
        for (int i = 1; i <= 4; i++)
            docs.Add(await MakeDocAsync(col, $"bulk{i}", i));

        var ids = await col.InsertBulkAsync(docs);

        Assert.Equal(4, ids.Count);
        Assert.All(ids, id => Assert.False(id.IsEmpty));
    }

    // ── Update ────────────────────────────────────────────────────────────────

    [Fact]
    public async Task UpdateAsync_ChangesDocument()
    {
        var (client, col) = await SetupAsync();
        await using var _ = client;

        var original = await MakeDocAsync(col, "before", 1);
        var id       = await col.InsertAsync(original);

        var updated = await MakeDocAsync(col, "after", 99);
        var success = await col.UpdateAsync(id, updated);

        Assert.True(success);

        var found = await col.FindByIdAsync(id);
        Assert.NotNull(found);
        Assert.True(found.TryGetString("name", out var name));
        Assert.Equal("after", name);
        Assert.True(found.TryGetInt32("value", out var value));
        Assert.Equal(99, value);
    }

    // ── Delete ────────────────────────────────────────────────────────────────

    [Fact]
    public async Task DeleteAsync_RemovesDocument()
    {
        var (client, col) = await SetupAsync();
        await using var _ = client;

        var doc     = await MakeDocAsync(col, "to-delete", 0);
        var id      = await col.InsertAsync(doc);
        var deleted = await col.DeleteAsync(id);

        Assert.True(deleted);

        var found = await col.FindByIdAsync(id);
        Assert.Null(found);
    }

    [Fact]
    public async Task DeleteAsync_NonExistentId_ReturnsFalse()
    {
        await using var client = CreateClient();
        var col    = client.GetDynamicCollection(UniqueCollection());
        var fakeId = BsonId.NewId(BsonIdType.ObjectId);

        var result = await col.DeleteAsync(fakeId);

        Assert.False(result);
    }

    // ── BulkDelete ────────────────────────────────────────────────────────────

    [Fact]
    public async Task DeleteBulkAsync_DeletesAllSpecifiedIds()
    {
        var (client, col) = await SetupAsync();
        await using var _ = client;

        var ids = new List<BsonId>();
        for (int i = 1; i <= 3; i++)
        {
            var doc = await MakeDocAsync(col, $"bulk-del{i}", i);
            ids.Add(await col.InsertAsync(doc));
        }

        var count = await col.DeleteBulkAsync(ids);

        Assert.Equal(3, count);

        foreach (var id in ids)
            Assert.Null(await col.FindByIdAsync(id));
    }

    // ── UpdateBulk ────────────────────────────────────────────────────────────

    [Fact]
    public async Task UpdateBulkAsync_UpdatesAllSpecifiedDocuments()
    {
        var (client, col) = await SetupAsync();
        await using var _ = client;

        var inserted = new List<BsonId>();
        for (int i = 1; i <= 3; i++)
        {
            var doc = await MakeDocAsync(col, $"old{i}", i);
            inserted.Add(await col.InsertAsync(doc));
        }

        var updates = new List<(BsonId, BsonDocument)>();
        for (int i = 0; i < 3; i++)
        {
            var newDoc = await MakeDocAsync(col, $"new{i + 1}", (i + 1) * 100);
            updates.Add((inserted[i], newDoc));
        }

        var count = await col.UpdateBulkAsync(updates);

        Assert.Equal(3, count);

        for (int i = 0; i < 3; i++)
        {
            var found = await col.FindByIdAsync(inserted[i]);
            Assert.NotNull(found);
            Assert.True(found.TryGetInt32("value", out var v));
            Assert.Equal((i + 1) * 100, v);
        }
    }
}
