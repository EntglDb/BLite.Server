// BLite.Client.IntegrationTests — SchemaTests
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// End-to-end tests for GetSchema and SetSchema (version management,
// field round-trips, nullable flag, title).

using BLite.Bson;
using BLite.Client.IntegrationTests.Infrastructure;

namespace BLite.Client.IntegrationTests;

[Collection("Integration")]
public class SchemaTests : IntegrationTestBase
{
    public SchemaTests(BLiteServerFixture fixture) : base(fixture) { }

    // ── GetSchema — no schema ─────────────────────────────────────────────────

    [Fact]
    public async Task GetSchema_NeverSet_ReturnsHasSchemaFalse()
    {
        await using var client = CreateClient();
        var col = client.GetDynamicCollection(UniqueCollection());

        var schema = await col.GetSchemaAsync();

        Assert.False(schema.HasSchema);
        Assert.Empty(schema.Fields);
        Assert.Equal(0, schema.VersionCount);
        Assert.Null(schema.Title);
    }

    // ── SetSchema / GetSchema round-trip ──────────────────────────────────────

    [Fact]
    public async Task SetSchema_ThenGet_ReturnsCorrectFieldTypes()
    {
        await using var client = CreateClient();
        var col = client.GetDynamicCollection(UniqueCollection());

        await col.SetSchemaAsync(
        [
            ("name",       BsonType.String,   true),
            ("price",      BsonType.Double,   false),
            ("stock",      BsonType.Int32,    false),
            ("active",     BsonType.Boolean,  true),
            ("created_at", BsonType.DateTime, false)
        ], title: "Product");

        var schema = await col.GetSchemaAsync();

        Assert.True(schema.HasSchema);
        Assert.Equal("Product", schema.Title);
        Assert.Equal(5, schema.Fields.Count);

        Assert.Contains(schema.Fields, f => f.Name == "name"       && f.TypeName == "String");
        Assert.Contains(schema.Fields, f => f.Name == "price"      && f.TypeName == "Double");
        Assert.Contains(schema.Fields, f => f.Name == "stock"      && f.TypeName == "Int32");
        Assert.Contains(schema.Fields, f => f.Name == "active"     && f.TypeName == "Boolean");
        Assert.Contains(schema.Fields, f => f.Name == "created_at" && f.TypeName == "DateTime");
    }

    [Fact]
    public async Task SetSchema_NullableFlag_RoundTrips()
    {
        await using var client = CreateClient();
        var col = client.GetDynamicCollection(UniqueCollection());

        await col.SetSchemaAsync(
        [
            ("required_field", BsonType.String, false),
            ("optional_field", BsonType.String, true)
        ]);

        var schema = await col.GetSchemaAsync();

        var req = Assert.Single(schema.Fields, f => f.Name == "required_field");
        Assert.False(req.IsNullable);

        var opt = Assert.Single(schema.Fields, f => f.Name == "optional_field");
        Assert.True(opt.IsNullable);
    }

    [Fact]
    public async Task SetSchema_WithoutTitle_TitleIsNull()
    {
        await using var client = CreateClient();
        var col = client.GetDynamicCollection(UniqueCollection());

        await col.SetSchemaAsync([("x", BsonType.Int32, true)]);

        var schema = await col.GetSchemaAsync();

        Assert.True(schema.HasSchema);
        Assert.Null(schema.Title);
    }

    [Fact]
    public async Task SetSchema_EmptyFields_PersistsEmptySchemaVersion()
    {
        await using var client = CreateClient();
        var col = client.GetDynamicCollection(UniqueCollection());

        await col.SetSchemaAsync([]);

        var schema = await col.GetSchemaAsync();

        Assert.True(schema.HasSchema);
        Assert.Empty(schema.Fields);
        Assert.Equal(1, schema.VersionCount);
    }

    // ── TypeCode ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task SetSchema_TypeCodeMatchesBsonType()
    {
        await using var client = CreateClient();
        var col = client.GetDynamicCollection(UniqueCollection());

        await col.SetSchemaAsync([("val", BsonType.Int64, false)]);

        var schema = await col.GetSchemaAsync();
        var field = Assert.Single(schema.Fields);
        Assert.Equal((int)BsonType.Int64, field.TypeCode);
        Assert.Equal("Int64", field.TypeName);
    }

    // ── Multiple versions ─────────────────────────────────────────────────────

    [Fact]
    public async Task SetSchema_CalledTwice_VersionCountIsTwo()
    {
        await using var client = CreateClient();
        var col = client.GetDynamicCollection(UniqueCollection());

        await col.SetSchemaAsync([("name", BsonType.String, true)]);
        await col.SetSchemaAsync([("name", BsonType.String, true), ("age", BsonType.Int32, false)]);

        var schema = await col.GetSchemaAsync();

        Assert.Equal(2, schema.VersionCount);
        Assert.Equal(2, schema.Fields.Count);
    }

    [Fact]
    public async Task SetSchema_GetReturnsLatestVersion()
    {
        await using var client = CreateClient();
        var col = client.GetDynamicCollection(UniqueCollection());

        await col.SetSchemaAsync([("v1_field", BsonType.String, true)]);
        await col.SetSchemaAsync([("v2_field", BsonType.Int32,  false)]);

        var schema = await col.GetSchemaAsync();

        // Latest version has only v2_field
        Assert.Single(schema.Fields, f => f.Name == "v2_field");
        Assert.DoesNotContain(schema.Fields, f => f.Name == "v1_field");
    }
}
