// BLite.Client.IntegrationTests — VectorSearchTests
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// End-to-end tests for vector similarity search via gRPC and REST.
// Tests cover both the RemoteDynamicCollection.VectorSearchAsync client API
// and the POST /api/v1/{dbId}/{collection}/vector-search REST endpoint.

using System.Net;
using System.Text;
using System.Text.Json;
using BLite.Bson;
using BLite.Client.Collections;
using BLite.Client.IntegrationTests.Infrastructure;

namespace BLite.Client.IntegrationTests;

[Collection("Integration")]
public class VectorSearchTests : IntegrationTestBase
{
    private const string DbId = "default";
    private const int Dims = 4;

    public VectorSearchTests(BLiteServerFixture fixture) : base(fixture) { }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Seeds a collection with three documents each carrying a known 4-dim float vector,
    /// creates a Vector HNSW index on "vec" via the SDK, and returns the collection.
    /// </summary>
    private async Task<(BLiteClient Client, RemoteDynamicCollection Col)> SeedAsync()
    {
        var client = CreateClient();
        var col = client.GetDynamicCollection(UniqueCollection());

        await col.NewDocumentAsync(["label", "vec"], _ => { });

        var docs = new[]
        {
            ("A", new float[] { 1f, 0f, 0f, 0f }),
            ("B", new float[] { 0f, 1f, 0f, 0f }),
            ("C", new float[] { 0f, 0f, 0f, 1f }),
        };

        foreach (var (label, vec) in docs)
        {
            var doc = await col.NewDocumentAsync(
                ["label", "vec"],
                b => b.AddString("label", label)
                      .AddFloatArray("vec", vec));
            await col.InsertAsync(doc);
        }

        await col.CreateVectorIndexAsync("vec", Dims, "Cosine");

        return (client, col);
    }

    // ── Index management (SDK) ────────────────────────────────────────────────

    [Fact]
    public async Task CreateVectorIndexAsync_ThenListIndexes_ContainsVectorIndex()
    {
        var (client, col) = await SeedAsync();
        await using var _ = client;

        var indexes = await col.ListIndexesAsync();

        Assert.Contains(indexes, i => i.Type == "Vector" && i.FieldPath == "vec");
    }

    [Fact]
    public async Task DropIndexAsync_ExistingIndex_ReturnsTrueAndIndexGone()
    {
        var (client, col) = await SeedAsync();
        await using var _ = client;

        var indexes = await col.ListIndexesAsync();
        var vecIdx = indexes.First(i => i.Type == "Vector");

        var ok = await col.DropIndexAsync(vecIdx.Name);

        Assert.True(ok);
        var after = await col.ListIndexesAsync();
        Assert.DoesNotContain(after, i => i.Name == vecIdx.Name);
    }

    // ── VectorSource (SDK) ────────────────────────────────────────────────────

    [Fact]
    public async Task SetVectorSourceAsync_ThenGet_ReturnsConfiguration()
    {
        await using var client = CreateClient();
        var col = client.GetDynamicCollection(UniqueCollection());

        await col.SetVectorSourceAsync([("label", "Label:", null)], separator: " | ");

        var info = await col.GetVectorSourceAsync();

        Assert.NotNull(info);
        Assert.Equal(" | ", info.Separator);
        Assert.Single(info.Fields);
        Assert.Equal("label", info.Fields[0].Path);
        Assert.Equal("Label:", info.Fields[0].Prefix);
    }

    [Fact]
    public async Task GetVectorSourceAsync_WhenNotConfigured_ReturnsNull()
    {
        await using var client = CreateClient();
        var col = client.GetDynamicCollection(UniqueCollection());

        var info = await col.GetVectorSourceAsync();

        Assert.Null(info);
    }

    [Fact]
    public async Task SetVectorSourceAsync_EmptyFields_ClearsConfiguration()
    {
        await using var client = CreateClient();
        var col = client.GetDynamicCollection(UniqueCollection());

        await col.SetVectorSourceAsync([("title", null, null)]);
        await col.SetVectorSourceAsync([]);          // clear

        var info = await col.GetVectorSourceAsync();
        Assert.Null(info);
    }

    // ── gRPC: RemoteDynamicCollection.VectorSearchAsync ───────────────────────

    [Fact]
    public async Task VectorSearchAsync_ReturnsNearestNeighbours()
    {
        var (client, col) = await SeedAsync();
        await using var _ = client;

        var results = new List<BsonDocument>();
        await foreach (var doc in col.VectorSearchAsync([1f, 0f, 0f, 0f], k: 2))
            results.Add(doc);

        Assert.NotEmpty(results);
        Assert.True(results[0].TryGetString("label", out var label));
        Assert.Equal("A", label);
    }

    [Fact]
    public async Task VectorSearchAsync_WithK1_ReturnsExactlyOneDocument()
    {
        var (client, col) = await SeedAsync();
        await using var _ = client;

        var results = new List<BsonDocument>();
        await foreach (var doc in col.VectorSearchAsync([0f, 0f, 0f, 1f], k: 1))
            results.Add(doc);

        Assert.Single(results);
        Assert.True(results[0].TryGetString("label", out var label));
        Assert.Equal("C", label);
    }

    // ── REST: POST /api/v1/{dbId}/{collection}/vector-search ─────────────────

    [Fact]
    public async Task RestVectorSearch_ReturnsNearestNeighbours()
    {
        var (client, col) = await SeedAsync();
        await using var _ = client;

        using var http = Fixture.CreateHttpClient();
        var body = JsonSerializer.Serialize(new { vector = new float[] { 1f, 0f, 0f, 0f }, k = 2 });
        var response = await http.PostAsync(
            $"/api/v1/{DbId}/{col.Name}/vector-search",
            new StringContent(body, Encoding.UTF8, "application/json"));

        response.EnsureSuccessStatusCode();
        var result = JsonSerializer.Deserialize<JsonElement>(
            await response.Content.ReadAsStringAsync());

        Assert.Equal(2, result.GetProperty("count").GetInt32());
        var first = JsonSerializer.Deserialize<JsonElement>(
            result.GetProperty("documents").EnumerateArray().First().GetString()!);
        Assert.Equal("A", first.GetProperty("label").GetString());
    }

    [Fact]
    public async Task RestVectorSearch_MissingVector_Returns400()
    {
        var (client, col) = await SeedAsync();
        await using var _ = client;

        using var http = Fixture.CreateHttpClient();
        var response = await http.PostAsync(
            $"/api/v1/{DbId}/{col.Name}/vector-search",
            new StringContent("{}", Encoding.UTF8, "application/json"));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task RestVectorSearch_NoVectorIndex_Returns422()
    {
        await using var client = CreateClient();
        var col = client.GetDynamicCollection(UniqueCollection());
        var doc = await col.NewDocumentAsync(["x"], b => b.AddInt32("x", 1));
        await col.InsertAsync(doc);

        using var http = Fixture.CreateHttpClient();
        var body = JsonSerializer.Serialize(new { vector = new float[] { 1f, 0f }, k = 1 });
        var response = await http.PostAsync(
            $"/api/v1/{DbId}/{col.Name}/vector-search",
            new StringContent(body, Encoding.UTF8, "application/json"));

        Assert.Equal(HttpStatusCode.UnprocessableEntity, response.StatusCode);
    }
}

