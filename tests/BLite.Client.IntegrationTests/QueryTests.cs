// BLite.Client.IntegrationTests — QueryTests
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// End-to-end tests for server-side query execution via QueryDescriptor
// (filter, sort, skip/take, compound predicates).

using BLite.Bson;
using BLite.Client.Collections;
using BLite.Client.IntegrationTests.Infrastructure;
using BLite.Proto;

namespace BLite.Client.IntegrationTests;

[Collection("Integration")]
public class QueryTests : IntegrationTestBase
{
    public QueryTests(BLiteServerFixture fixture) : base(fixture) { }

    // ── Setup ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Inserts <paramref name="count"/> documents with fields name/score/active.
    /// score = i*10  (10, 20, 30, …),  active = (i % 2 == 0).
    /// </summary>
    private async Task<(BLiteClient, RemoteDynamicCollection)> SeedAsync(int count = 5)
    {
        var client = CreateClient();
        var col = client.GetDynamicCollection(UniqueCollection());
        _ = await col.NewDocumentAsync(["name", "score", "active"], _ => { });

        for (int i = 1; i <= count; i++)
        {
            var doc = await col.NewDocumentAsync(
                ["name", "score", "active"],
                b => b.AddString("name",   $"user{i}")
                      .AddInt32("score",   i * 10)
                      .AddBoolean("active", i % 2 == 0));
            await col.InsertAsync(doc);
        }

        return (client, col);
    }

    // ── Equality filter ───────────────────────────────────────────────────────

    [Fact]
    public async Task QueryAsync_EqualityFilter_ReturnsOnlyMatchingDocument()
    {
        var (client, col) = await SeedAsync(5);
        await using var _ = client;

        var descriptor = new QueryDescriptor
        {
            Collection = col.Name,
            Where = new BinaryFilter
            {
                Field = "name",
                Op    = FilterOp.Eq,
                Value = ScalarValue.From("user3")
            }
        };

        var results = new List<BsonDocument>();
        await foreach (var doc in col.QueryAsync(descriptor))
            results.Add(doc);

        Assert.Single(results);
        Assert.True(results[0].TryGetString("name", out var name));
        Assert.Equal("user3", name);
    }

    [Fact]
    public async Task QueryAsync_EqualityFilter_NoMatch_ReturnsEmpty()
    {
        var (client, col) = await SeedAsync(3);
        await using var _ = client;

        var descriptor = new QueryDescriptor
        {
            Collection = col.Name,
            Where = new BinaryFilter
            {
                Field = "name",
                Op    = FilterOp.Eq,
                Value = ScalarValue.From("does_not_exist")
            }
        };

        var results = new List<BsonDocument>();
        await foreach (var doc in col.QueryAsync(descriptor))
            results.Add(doc);

        Assert.Empty(results);
    }

    // ── Range filters ─────────────────────────────────────────────────────────

    [Fact]
    public async Task QueryAsync_GreaterThan_ReturnsCorrectDocuments()
    {
        var (client, col) = await SeedAsync(5);
        await using var _ = client;

        var descriptor = new QueryDescriptor
        {
            Collection = col.Name,
            Where = new BinaryFilter
            {
                Field = "score",
                Op    = FilterOp.Gt,
                Value = ScalarValue.From(30)
            }
        };

        var results = new List<BsonDocument>();
        await foreach (var doc in col.QueryAsync(descriptor))
            results.Add(doc);

        Assert.Equal(2, results.Count);
        Assert.All(results, d =>
        {
            Assert.True(d.TryGetInt32("score", out var s));
            Assert.True(s > 30);
        });
    }

    [Fact]
    public async Task QueryAsync_LessThanOrEqual_ReturnsCorrectDocuments()
    {
        var (client, col) = await SeedAsync(5);
        await using var _ = client;

        var descriptor = new QueryDescriptor
        {
            Collection = col.Name,
            Where = new BinaryFilter
            {
                Field = "score",
                Op    = FilterOp.LtEq,
                Value = ScalarValue.From(20)
            }
        };

        var results = new List<BsonDocument>();
        await foreach (var doc in col.QueryAsync(descriptor))
            results.Add(doc);

        Assert.Equal(2, results.Count);
        Assert.All(results, d =>
        {
            Assert.True(d.TryGetInt32("score", out var s));
            Assert.True(s <= 20);
        });
    }

    // ── Sort ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task QueryAsync_SortAscending_ReturnsInAscendingOrder()
    {
        var (client, col) = await SeedAsync(5);
        await using var _ = client;

        var descriptor = new QueryDescriptor
        {
            Collection = col.Name,
            OrderBy = [new SortSpec { Field = "score", Descending = false }]
        };

        var scores = new List<int>();
        await foreach (var doc in col.QueryAsync(descriptor))
        {
            doc.TryGetInt32("score", out var s);
            scores.Add(s);
        }

        Assert.Equal(scores.OrderBy(x => x).ToList(), scores);
    }

    [Fact]
    public async Task QueryAsync_SortDescending_ReturnsInDescendingOrder()
    {
        var (client, col) = await SeedAsync(5);
        await using var _ = client;

        var descriptor = new QueryDescriptor
        {
            Collection = col.Name,
            OrderBy = [new SortSpec { Field = "score", Descending = true }]
        };

        var scores = new List<int>();
        await foreach (var doc in col.QueryAsync(descriptor))
        {
            doc.TryGetInt32("score", out var s);
            scores.Add(s);
        }

        Assert.Equal(scores.OrderByDescending(x => x).ToList(), scores);
    }

    // ── Pagination ────────────────────────────────────────────────────────────

    [Fact]
    public async Task QueryAsync_Take_LimitsResultCount()
    {
        var (client, col) = await SeedAsync(10);
        await using var _ = client;

        var descriptor = new QueryDescriptor
        {
            Collection = col.Name,
            Take = 3
        };

        var results = new List<BsonDocument>();
        await foreach (var doc in col.QueryAsync(descriptor))
            results.Add(doc);

        Assert.Equal(3, results.Count);
    }

    [Fact]
    public async Task QueryAsync_SkipAndTake_ReturnsPaginatedWindow()
    {
        var (client, col) = await SeedAsync(10);
        await using var _ = client;

        var descriptor = new QueryDescriptor
        {
            Collection = col.Name,
            OrderBy = [new SortSpec { Field = "score", Descending = false }],
            Skip = 3,
            Take = 4
        };

        var results = new List<BsonDocument>();
        await foreach (var doc in col.QueryAsync(descriptor))
            results.Add(doc);

        Assert.Equal(4, results.Count);
        // Scores should start at (3+1)*10 = 40
        Assert.True(results[0].TryGetInt32("score", out var first));
        Assert.Equal(40, first);
    }

    // ── Compound predicates ───────────────────────────────────────────────────

    [Fact]
    public async Task QueryAsync_AndFilter_AppliesAllConditions()
    {
        var (client, col) = await SeedAsync(6);
        await using var _ = client;

        var descriptor = new QueryDescriptor
        {
            Collection = col.Name,
            Where = new LogicalFilter
            {
                Op = LogicalOp.And,
                Children =
                [
                    new BinaryFilter { Field = "score",  Op = FilterOp.GtEq, Value = ScalarValue.From(20) },
                    new BinaryFilter { Field = "active", Op = FilterOp.Eq,   Value = ScalarValue.From(true) }
                ]
            }
        };

        var results = new List<BsonDocument>();
        await foreach (var doc in col.QueryAsync(descriptor))
            results.Add(doc);

        Assert.All(results, d =>
        {
            Assert.True(d.TryGetInt32("score", out var s));
            Assert.True(s >= 20);
        });
        Assert.All(results, d =>
        {
            Assert.True(d.TryGetValue("active", out var a) && a.IsBoolean && a.AsBoolean);
        });
    }

    [Fact]
    public async Task QueryAsync_OrFilter_ReturnsUnionOfMatches()
    {
        var (client, col) = await SeedAsync(5);
        await using var _ = client;

        var descriptor = new QueryDescriptor
        {
            Collection = col.Name,
            Where = new LogicalFilter
            {
                Op = LogicalOp.Or,
                Children =
                [
                    new BinaryFilter { Field = "name", Op = FilterOp.Eq, Value = ScalarValue.From("user1") },
                    new BinaryFilter { Field = "name", Op = FilterOp.Eq, Value = ScalarValue.From("user5") }
                ]
            }
        };

        var results = new List<BsonDocument>();
        await foreach (var doc in col.QueryAsync(descriptor))
            results.Add(doc);

        Assert.Equal(2, results.Count);
    }

    // ── Edge cases ────────────────────────────────────────────────────────────

    [Fact]
    public async Task QueryAsync_EmptyCollection_ReturnsNoDocuments()
    {
        await using var client = CreateClient();
        var col = client.GetDynamicCollection(UniqueCollection());

        var descriptor = new QueryDescriptor { Collection = col.Name };

        var results = new List<BsonDocument>();
        await foreach (var doc in col.QueryAsync(descriptor))
            results.Add(doc);

        Assert.Empty(results);
    }

    [Fact]
    public async Task QueryAsync_NoFilter_ReturnsAllDocuments()
    {
        var (client, col) = await SeedAsync(4);
        await using var _ = client;

        var descriptor = new QueryDescriptor { Collection = col.Name };

        var results = new List<BsonDocument>();
        await foreach (var doc in col.QueryAsync(descriptor))
            results.Add(doc);

        Assert.Equal(4, results.Count);
    }
}
