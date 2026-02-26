// BLite.Client.IntegrationTests — TransactionTests
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// End-to-end tests for RemoteTransaction (BeginTransactionAsync / Commit / Rollback).

using BLite.Bson;
using BLite.Client.Collections;
using BLite.Client.IntegrationTests.Infrastructure;

namespace BLite.Client.IntegrationTests;

[Collection("Integration")]
public class TransactionTests : IntegrationTestBase
{
    public TransactionTests(BLiteServerFixture fixture) : base(fixture) { }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private async Task<(BLiteClient Client, RemoteDynamicCollection Col)> SetupAsync()
    {
        var client = CreateClient();
        var col = client.GetDynamicCollection(UniqueCollection());
        // Prime the key map
        _ = await col.NewDocumentAsync(["key"], _ => { });
        return (client, col);
    }

    private static Task<BsonDocument> MakeDocAsync(RemoteDynamicCollection col, string key)
        => col.NewDocumentAsync(["key"], b => b.AddString("key", key));

    // ── Commit ────────────────────────────────────────────────────────────────

    [Fact]
    public async Task CommitAsync_InsertInTransaction_DocumentVisible()
    {
        var (client, col) = await SetupAsync();
        await using var _ = client;

        var tx = await client.BeginTransactionAsync();
        var doc = await MakeDocAsync(col, "committed");
        var id = await col.InsertAsync(doc, tx);
        await tx.CommitAsync();

        var found = await col.FindByIdAsync(id);

        Assert.NotNull(found);
        Assert.True(found.TryGetString("key", out var val));
        Assert.Equal("committed", val);
    }

    // ── Rollback ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task RollbackAsync_InsertInTransaction_DocumentNotVisible()
    {
        var (client, col) = await SetupAsync();
        await using var _ = client;

        var tx = await client.BeginTransactionAsync();
        var doc = await MakeDocAsync(col, "rolled-back");
        var id = await col.InsertAsync(doc, tx);
        await tx.RollbackAsync();

        var found = await col.FindByIdAsync(id);

        Assert.Null(found);
    }

    // ── Auto-rollback on dispose ──────────────────────────────────────────────

    [Fact]
    public async Task DisposeWithoutCommit_AutomaticRollback()
    {
        var (client, col) = await SetupAsync();
        await using var _ = client;

        BsonId id;
        await using (var tx = await client.BeginTransactionAsync())
        {
            var doc = await MakeDocAsync(col, "auto-rollback");
            id = await col.InsertAsync(doc, tx);
            // Dispose without CommitAsync — should trigger auto-rollback
        }

        var found = await col.FindByIdAsync(id);

        Assert.Null(found);
    }

    // ── Multi-collection atomic commit ────────────────────────────────────────

    [Fact]
    public async Task MultipleCollections_SameTransaction_AtomicCommit()
    {
        await using var client = CreateClient();

        var col1 = client.GetDynamicCollection(UniqueCollection());
        var col2 = client.GetDynamicCollection(UniqueCollection());

        // Prime key maps
        _ = await col1.NewDocumentAsync(["name"], _ => { });
        _ = await col2.NewDocumentAsync(["name"], _ => { });

        var tx = await client.BeginTransactionAsync();

        var doc1 = await col1.NewDocumentAsync(["name"], b => b.AddString("name", "alpha"));
        var doc2 = await col2.NewDocumentAsync(["name"], b => b.AddString("name", "beta"));

        var id1 = await col1.InsertAsync(doc1, tx);
        var id2 = await col2.InsertAsync(doc2, tx);

        await tx.CommitAsync();

        var found1 = await col1.FindByIdAsync(id1);
        var found2 = await col2.FindByIdAsync(id2);

        Assert.NotNull(found1);
        Assert.NotNull(found2);
        Assert.True(found1.TryGetString("name", out var v1));
        Assert.True(found2.TryGetString("name", out var v2));
        Assert.Equal("alpha", v1);
        Assert.Equal("beta",  v2);
    }
}

