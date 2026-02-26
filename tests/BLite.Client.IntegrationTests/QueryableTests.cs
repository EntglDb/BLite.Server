// BLite.Client.IntegrationTests — QueryableTests
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// End-to-end tests for LINQ push-down via RemoteQueryProvider.

using BLite.Client.Collections;
using BLite.Client.IntegrationTests.Infrastructure;
using BLite.Client.IntegrationTests.Infrastructure.Mappers;
using BLite.Core.Query;

namespace BLite.Client.IntegrationTests;

[Collection("Integration")]
public class QueryableTests : IntegrationTestBase
{
    public QueryableTests(BLiteServerFixture fixture) : base(fixture) { }

    // ── Mapper wrapper with overridable CollectionName ────────────────────────

    private sealed class NamedProductMapper(string collectionName)
        : BLite_Client_IntegrationTests_Infrastructure_TestProductMapper
    {
        public override string CollectionName => collectionName;
    }

    // ── Setup ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Creates a fresh collection pre-populated with 5 products and returns
    /// the client + collection so the test can continue querying.
    /// </summary>
    private async Task<(BLiteClient Client, RemoteCollection<int, TestProduct> Col)>
        SetupWithProductsAsync()
    {
        var client = CreateClient();
        var col = client.GetCollection<int, TestProduct>(
            new NamedProductMapper(UniqueCollection()));

        for (int i = 1; i <= 5; i++)
            await col.InsertAsync(new TestProduct
            {
                Id    = i,
                Name  = $"Product{i:D2}",
                Price = i * 10.0m,
                Stock = i * 2
            });

        return (client, col);
    }

    // ── Tests ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task AsQueryable_Where_PushesFilterToServer()
    {
        var (client, col) = await SetupWithProductsAsync();
        await using var _ = client;

        var results = await col.AsQueryable()
            .Where(p => p.Stock > 4)
            .ToListAsync();

        // Products with Stock > 4: P3(6), P4(8), P5(10) → 3 items
        Assert.Equal(3, results.Count);
        Assert.All(results, p => Assert.True(p.Stock > 4));
    }

    [Fact]
    public async Task AsQueryable_OrderBy_ReturnsOrderedResults()
    {
        var (client, col) = await SetupWithProductsAsync();
        await using var _ = client;

        var results = await col.AsQueryable()
            .OrderByDescending(p => p.Price)
            .ToListAsync();

        Assert.Equal(5, results.Count);
        // Prices should be descending: 50, 40, 30, 20, 10
        for (int i = 0; i < results.Count - 1; i++)
            Assert.True(results[i].Price >= results[i + 1].Price);
    }

    [Fact]
    public async Task AsQueryable_Take_LimitsResults()
    {
        var (client, col) = await SetupWithProductsAsync();
        await using var _ = client;

        var results = await col.AsQueryable()
            .Take(2)
            .ToListAsync();

        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task AsQueryable_Skip_OffsetResults()
    {
        var (client, col) = await SetupWithProductsAsync();
        await using var _ = client;

        var all     = await col.AsQueryable().ToListAsync();
        var skipped = await col.AsQueryable().Skip(3).ToListAsync();

        Assert.Equal(all.Count - 3, skipped.Count);
    }

    [Fact]
    public async Task ToListAsync_MaterialisesAll()
    {
        var (client, col) = await SetupWithProductsAsync();
        await using var _ = client;

        var results = await col.AsQueryable().ToListAsync();

        Assert.Equal(5, results.Count);
    }

    [Fact]
    public async Task FirstOrDefaultAsync_ReturnsFirstMatch()
    {
        var (client, col) = await SetupWithProductsAsync();
        await using var _ = client;

        var product = await col.AsQueryable()
            .Where(p => p.Name == "Product03")
            .FirstOrDefaultAsync();

        Assert.NotNull(product);
        Assert.Equal(3,           product.Id);
        Assert.Equal("Product03", product.Name);
    }

    [Fact]
    public async Task CountAsync_ReturnsCorrectCount()
    {
        var (client, col) = await SetupWithProductsAsync();
        await using var _ = client;

        var count = await col.AsQueryable()
            .Where(p => p.Price >= 30m)
            .CountAsync();

        // Prices 30, 40, 50 → 3
        Assert.Equal(3, count);
    }
}

