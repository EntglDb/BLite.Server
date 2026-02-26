// BLite.Client.IntegrationTests — BlqlRestTests
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// End-to-end tests for the BLQL REST endpoints:
//   POST /api/v1/{dbId}/{collection}/query
//   GET  /api/v1/{dbId}/{collection}/query
//   POST /api/v1/{dbId}/{collection}/query/count

using System.Net;
using System.Text;
using System.Text.Json;
using BLite.Client.IntegrationTests.Infrastructure;

namespace BLite.Client.IntegrationTests;

[Collection("Integration")]
public class BlqlRestTests : IntegrationTestBase
{
    // System database → dbId "default" maps to null inside the server.
    private const string DbId = "default";

    public BlqlRestTests(BLiteServerFixture fixture) : base(fixture) { }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Inserts three test documents (Alice/active/30, Bob/active/17, Charlie/inactive/25)
    /// via gRPC and returns the unique collection name.
    /// </summary>
    private async Task<string> SeedCollectionAsync()
    {
        var colName = UniqueCollection();
        await using var client = CreateClient();
        var col = client.GetDynamicCollection(colName);

        foreach (var (name, age, status) in new[]
        {
            ("Alice",   30, "active"),
            ("Bob",     17, "active"),
            ("Charlie", 25, "inactive"),
        })
        {
            var doc = await col.NewDocumentAsync(
                ["name", "age", "status"],
                b => b.AddString("name",   name)
                      .AddInt32 ("age",    age)
                      .AddString("status", status));
            await col.InsertAsync(doc);
        }
        return colName;
    }

    /// <summary>Posts a raw JSON string to the given path and returns the deserialized response.</summary>
    private async Task<JsonElement> PostQueryAsync(string path, string body)
    {
        using var http = Fixture.CreateHttpClient();
        var content  = new StringContent(body, Encoding.UTF8, "application/json");
        var response = await http.PostAsync(path, content);
        response.EnsureSuccessStatusCode();
        var json = await response.Content.ReadAsStringAsync();
        return JsonSerializer.Deserialize<JsonElement>(json);
    }

    private string QueryUrl(string col)  => $"/api/v1/{DbId}/{col}/query";
    private string CountUrl(string col)  => $"/api/v1/{DbId}/{col}/query/count";

    // ── POST /query — full query ──────────────────────────────────────────────

    [Fact]
    public async Task PostQuery_NoBody_ReturnsAllDocuments()
    {
        var col  = await SeedCollectionAsync();
        var result = await PostQueryAsync(QueryUrl(col), "{}");

        Assert.Equal(3, result.GetProperty("count").GetInt32());
    }

    [Fact]
    public async Task PostQuery_WithEqualityFilter_ReturnsMatchingDocuments()
    {
        var col = await SeedCollectionAsync();
        var result = await PostQueryAsync(QueryUrl(col), """
            { "filter": { "status": "active" } }
            """);

        Assert.Equal(2, result.GetProperty("count").GetInt32());
        var docs = result.GetProperty("documents").EnumerateArray().ToList();
        Assert.All(docs, d => Assert.Equal("active", d.GetProperty("status").GetString()));
    }

    [Fact]
    public async Task PostQuery_WithCompoundFilter_ReturnsOnlyMatchingDocuments()
    {
        // active AND age >= 18  →  Alice only (Bob is 17)
        var col = await SeedCollectionAsync();
        var result = await PostQueryAsync(QueryUrl(col), """
            { "filter": { "status": "active", "age": { "$gte": 18 } } }
            """);

        Assert.Equal(1, result.GetProperty("count").GetInt32());
        var doc = result.GetProperty("documents").EnumerateArray().First();
        Assert.Equal("Alice", doc.GetProperty("name").GetString());
    }

    [Fact]
    public async Task PostQuery_WithSort_ReturnsDocumentsInOrder()
    {
        var col = await SeedCollectionAsync();
        var result = await PostQueryAsync(QueryUrl(col), """
            { "filter": { "status": "active" }, "sort": { "age": -1 } }
            """);

        var docs = result.GetProperty("documents").EnumerateArray().ToList();
        Assert.Equal(2, docs.Count);
        // age DESC → Alice (30), Bob (17)
        Assert.Equal("Alice", docs[0].GetProperty("name").GetString());
        Assert.Equal("Bob",   docs[1].GetProperty("name").GetString());
    }

    [Fact]
    public async Task PostQuery_WithSkipAndLimit_AppliesPaging()
    {
        var col = await SeedCollectionAsync();
        // All 3 docs, sorted by name ASC → Alice, Bob, Charlie; skip 1, take 1 → Bob
        var result = await PostQueryAsync(QueryUrl(col), """
            { "sort": { "name": 1 }, "skip": 1, "limit": 1 }
            """);

        Assert.Equal(1, result.GetProperty("count").GetInt32());
        Assert.Equal(1, result.GetProperty("skip").GetInt32());
        Assert.Equal(1, result.GetProperty("limit").GetInt32());
        var doc = result.GetProperty("documents").EnumerateArray().First();
        Assert.Equal("Bob", doc.GetProperty("name").GetString());
    }

    [Fact]
    public async Task PostQuery_WithIncludeProjection_OmitsOtherFields()
    {
        var col = await SeedCollectionAsync();
        var result = await PostQueryAsync(QueryUrl(col), """
            { "filter": { "name": "Alice" }, "include": ["name"] }
            """);

        var doc = result.GetProperty("documents").EnumerateArray().First();
        // "name" should be present, "age" and "status" should not
        Assert.True(doc.TryGetProperty("name",   out _), "name should be included");
        Assert.False(doc.TryGetProperty("age",    out _), "age should be excluded");
        Assert.False(doc.TryGetProperty("status", out _), "status should be excluded");
    }

    [Fact]
    public async Task PostQuery_WithExcludeProjection_IncludesOtherFields()
    {
        var col = await SeedCollectionAsync();
        var result = await PostQueryAsync(QueryUrl(col), """
            { "filter": { "name": "Alice" }, "exclude": ["status"] }
            """);

        var doc = result.GetProperty("documents").EnumerateArray().First();
        Assert.True(doc.TryGetProperty("name", out _), "name should be present");
        Assert.True(doc.TryGetProperty("age",  out _), "age should be present");
        Assert.False(doc.TryGetProperty("status", out _), "status should be excluded");
    }

    [Fact]
    public async Task PostQuery_WithNewOperators_StringStartsWith()
    {
        var col = await SeedCollectionAsync();
        var result = await PostQueryAsync(QueryUrl(col), """
            { "filter": { "name": { "$startsWith": "Ch" } } }
            """);

        Assert.Equal(1, result.GetProperty("count").GetInt32());
        Assert.Equal("Charlie",
            result.GetProperty("documents").EnumerateArray().First()
                .GetProperty("name").GetString());
    }

    [Fact]
    public async Task PostQuery_WithModFilter_ReturnsEvenAges()
    {
        var col = await SeedCollectionAsync();
        // age % 2 == 0 → Alice(30), Charlie(25 → odd) → only Alice and Charlie? Actually 30%2=0 (Alice), 17%2=1 (Bob), 25%2=1 (Charlie) → only Alice
        var result = await PostQueryAsync(QueryUrl(col), """
            { "filter": { "age": { "$mod": [2, 0] } } }
            """);

        Assert.Equal(1, result.GetProperty("count").GetInt32());
        Assert.Equal("Alice",
            result.GetProperty("documents").EnumerateArray().First()
                .GetProperty("name").GetString());
    }

    // ── POST /query — error handling ──────────────────────────────────────────

    [Fact]
    public async Task PostQuery_UnknownOperator_Returns400WithMessage()
    {
        var col = await SeedCollectionAsync();
        using var http = Fixture.CreateHttpClient();
        var content  = new StringContent(
            """{ "filter": { "age": { "$where": "malicious" } } }""",
            Encoding.UTF8, "application/json");
        var response = await http.PostAsync(QueryUrl(col), content);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("filter", body, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task PostQuery_ModDivisorZero_Returns400()
    {
        var col = await SeedCollectionAsync();
        using var http = Fixture.CreateHttpClient();
        var content  = new StringContent(
            """{ "filter": { "age": { "$mod": [0, 0] } } }""",
            Encoding.UTF8, "application/json");
        var response = await http.PostAsync(QueryUrl(col), content);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    // ── GET /query ────────────────────────────────────────────────────────────

    [Fact]
    public async Task GetQuery_WithFilterQueryParam_ReturnsMatchingDocuments()
    {
        var col = await SeedCollectionAsync();
        using var http = Fixture.CreateHttpClient();

        var filter   = Uri.EscapeDataString("""{"status":"active"}""");
        var response = await http.GetAsync($"/api/v1/{DbId}/{col}/query?filter={filter}&limit=10");

        response.EnsureSuccessStatusCode();
        var result = JsonSerializer.Deserialize<JsonElement>(
            await response.Content.ReadAsStringAsync());

        Assert.Equal(2, result.GetProperty("count").GetInt32());
    }

    [Fact]
    public async Task GetQuery_NoFilter_ReturnsAllDocuments()
    {
        var col = await SeedCollectionAsync();
        using var http = Fixture.CreateHttpClient();
        var response = await http.GetAsync($"/api/v1/{DbId}/{col}/query?limit=100");

        response.EnsureSuccessStatusCode();
        var result = JsonSerializer.Deserialize<JsonElement>(
            await response.Content.ReadAsStringAsync());

        Assert.Equal(3, result.GetProperty("count").GetInt32());
    }

    // ── POST /query/count ─────────────────────────────────────────────────────

    [Fact]
    public async Task PostQueryCount_WithFilter_ReturnsCorrectCount()
    {
        var col = await SeedCollectionAsync();
        var result = await PostQueryAsync(CountUrl(col), """
            { "filter": { "status": "active" } }
            """);

        Assert.Equal(2, result.GetProperty("count").GetInt32());
    }

    [Fact]
    public async Task PostQueryCount_NoFilter_CountsAllDocuments()
    {
        var col = await SeedCollectionAsync();
        var result = await PostQueryAsync(CountUrl(col), "{}");

        Assert.Equal(3, result.GetProperty("count").GetInt32());
    }

    [Fact]
    public async Task PostQueryCount_WithCompoundFilter_CountsCorrectly()
    {
        var col = await SeedCollectionAsync();
        var result = await PostQueryAsync(CountUrl(col), """
            { "filter": { "$or": [ { "age": { "$lt": 18 } }, { "status": "inactive" } ] } }
            """);

        // Bob (17, active) + Charlie (25, inactive) = 2
        Assert.Equal(2, result.GetProperty("count").GetInt32());
    }
}
