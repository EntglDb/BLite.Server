// BLite.Client.IntegrationTests — TypedCollectionTests
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// End-to-end tests for RemoteCollection<TId, T> (source-generated mappers).

using BLite.Client.Collections;
using BLite.Client.IntegrationTests.Infrastructure;
using BLite.Client.IntegrationTests.Infrastructure.Mappers;

namespace BLite.Client.IntegrationTests;

[Collection("Integration")]
public class TypedCollectionTests : IntegrationTestBase
{
    public TypedCollectionTests(BLiteServerFixture fixture) : base(fixture) { }

    // ── Mapper wrapper with overridable CollectionName ────────────────────────

    /// <summary>
    /// Wraps the generated mapper so each test can target its own isolated
    /// collection, preventing ID collisions across tests that share the fixture.
    /// </summary>
    private sealed class NamedProductMapper(string collectionName)
        : BLite_Client_IntegrationTests_Infrastructure_TestProductMapper
    {
        public override string CollectionName => collectionName;
    }

    // ── Helper ────────────────────────────────────────────────────────────────

    private (BLiteClient Client, RemoteCollection<int, TestProduct> Col) Setup()
    {
        var client = CreateClient();
        var col = client.GetCollection<int, TestProduct>(new NamedProductMapper(UniqueCollection()));
        return (client, col);
    }

    private static TestProduct NewProduct(int id, string name, decimal price, int stock = 10)
        => new() { Id = id, Name = name, Price = price, Stock = stock };

    // ── InsertAsync / FindByIdAsync ───────────────────────────────────────────

    [Fact]
    public async Task InsertAsync_TypedEntity_RoundTrips()
    {
        var (client, col) = Setup();
        await using var _ = client;

        var product = NewProduct(1, "Widget", 9.99m);
        var id = await col.InsertAsync(product);

        var found = await col.FindByIdAsync(id);

        Assert.NotNull(found);
        Assert.Equal(1,        found.Id);
        Assert.Equal("Widget", found.Name);
        Assert.Equal(9.99m,    found.Price);
        Assert.Equal(10,       found.Stock);
    }

    [Fact]
    public async Task FindByIdAsync_TypedEntity_ReturnsCorrectType()
    {
        var (client, col) = Setup();
        await using var _ = client;

        var product = NewProduct(42, "Gadget", 24.99m, stock: 5);
        await col.InsertAsync(product);

        var found = await col.FindByIdAsync(42);

        Assert.NotNull(found);
        Assert.IsType<TestProduct>(found);
        Assert.Equal(42,      found.Id);
        Assert.Equal("Gadget", found.Name);
        Assert.Equal(24.99m,   found.Price);
        Assert.Equal(5,        found.Stock);
    }

    [Fact]
    public async Task FindByIdAsync_NonExistent_ReturnsNull()
    {
        var (client, col) = Setup();
        await using var _ = client;

        var found = await col.FindByIdAsync(9999);

        Assert.Null(found);
    }

    // ── FindAllAsync ──────────────────────────────────────────────────────────

    [Fact]
    public async Task FindAllAsync_TypedEntities_StreamsAll()
    {
        var (client, col) = Setup();
        await using var _ = client;

        for (int i = 1; i <= 4; i++)
            await col.InsertAsync(NewProduct(i, $"P{i}", i * 1.50m));

        var all = new List<TestProduct>();
        await foreach (var p in col.FindAllAsync())
            all.Add(p);

        Assert.Equal(4, all.Count);
        Assert.All(all, p => Assert.NotNull(p.Name));
    }

    // ── UpdateAsync ───────────────────────────────────────────────────────────

    [Fact]
    public async Task UpdateAsync_TypedEntity_PersistsChanges()
    {
        var (client, col) = Setup();
        await using var _ = client;

        await col.InsertAsync(NewProduct(1, "OldName", 5.00m));

        var updated = NewProduct(1, "NewName", 7.99m, stock: 20);
        var success = await col.UpdateAsync(updated);

        Assert.True(success);
        var found = await col.FindByIdAsync(1);
        Assert.NotNull(found);
        Assert.Equal("NewName", found.Name);
        Assert.Equal(7.99m,     found.Price);
        Assert.Equal(20,        found.Stock);
    }

    // ── DeleteAsync ───────────────────────────────────────────────────────────

    [Fact]
    public async Task DeleteAsync_ByTypedId_RemovesDocument()
    {
        var (client, col) = Setup();
        await using var _ = client;

        await col.InsertAsync(NewProduct(7, "ToDelete", 1.00m));

        var deleted = await col.DeleteAsync(7);

        Assert.True(deleted);
        var found = await col.FindByIdAsync(7);
        Assert.Null(found);
    }

    // ── InsertBulkAsync ───────────────────────────────────────────────────────

    [Fact]
    public async Task InsertBulkAsync_TypedEntities_ReturnsAllIds()
    {
        var (client, col) = Setup();
        await using var _ = client;

        var products = Enumerable.Range(1, 5)
            .Select(i => NewProduct(i, $"Bulk{i}", i * 2.00m))
            .ToList();

        var ids = await col.InsertBulkAsync(products);

        Assert.Equal(5, ids.Count);
        Assert.All(ids, id => Assert.True(id > 0));
    }
}