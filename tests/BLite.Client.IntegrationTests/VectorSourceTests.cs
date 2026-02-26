// BLite.Client.IntegrationTests — VectorSourceTests
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// End-to-end tests for SetVectorSource and GetVectorSource
// (set, round-trip, prefix/suffix, clear).

using BLite.Client.IntegrationTests.Infrastructure;

namespace BLite.Client.IntegrationTests;

[Collection("Integration")]
public class VectorSourceTests : IntegrationTestBase
{
    public VectorSourceTests(BLiteServerFixture fixture) : base(fixture) { }

    [Fact]
    public async Task GetVectorSource_NotConfigured_ReturnsNull()
    {
        await using var client = CreateClient();
        var col = client.GetDynamicCollection(UniqueCollection());

        var config = await col.GetVectorSourceAsync();

        Assert.Null(config);
    }

    [Fact]
    public async Task SetVectorSource_ThenGet_FieldsAndSeparatorRoundTrip()
    {
        await using var client = CreateClient();
        var col = client.GetDynamicCollection(UniqueCollection());

        await col.SetVectorSourceAsync(
        [
            ("title", null, null),
            ("body",  null, null)
        ], separator: " | ");

        var config = await col.GetVectorSourceAsync();

        Assert.NotNull(config);
        Assert.Equal(" | ", config.Separator);
        Assert.Equal(2, config.Fields.Count);
        Assert.Contains(config.Fields, f => f.Path == "title");
        Assert.Contains(config.Fields, f => f.Path == "body");
    }

    [Fact]
    public async Task SetVectorSource_DefaultSeparator_IsSpace()
    {
        await using var client = CreateClient();
        var col = client.GetDynamicCollection(UniqueCollection());

        await col.SetVectorSourceAsync([("title", null, null)]);

        var config = await col.GetVectorSourceAsync();

        Assert.NotNull(config);
        Assert.Equal(" ", config.Separator);
    }

    [Fact]
    public async Task SetVectorSource_WithPrefixAndSuffix_PreservesAll()
    {
        await using var client = CreateClient();
        var col = client.GetDynamicCollection(UniqueCollection());

        await col.SetVectorSourceAsync(
        [
            ("title",  "Title: ", "."),
            ("author", "By ",     null),
            ("tags",   null,      " (tags)")
        ]);

        var config = await col.GetVectorSourceAsync();

        Assert.NotNull(config);

        var title = Assert.Single(config.Fields, f => f.Path == "title");
        Assert.Equal("Title: ", title.Prefix);
        Assert.Equal(".",       title.Suffix);

        var author = Assert.Single(config.Fields, f => f.Path == "author");
        Assert.Equal("By ", author.Prefix);
        Assert.Null(author.Suffix);

        var tags = Assert.Single(config.Fields, f => f.Path == "tags");
        Assert.Null(tags.Prefix);
        Assert.Equal(" (tags)", tags.Suffix);
    }

    [Fact]
    public async Task SetVectorSource_EmptyList_ClearsConfiguration()
    {
        await using var client = CreateClient();
        var col = client.GetDynamicCollection(UniqueCollection());

        await col.SetVectorSourceAsync([("title", null, null)]);
        Assert.NotNull(await col.GetVectorSourceAsync());

        await col.SetVectorSourceAsync([]);

        var after = await col.GetVectorSourceAsync();
        Assert.Null(after);
    }

    [Fact]
    public async Task SetVectorSource_Overwrite_ReplacesOldConfiguration()
    {
        await using var client = CreateClient();
        var col = client.GetDynamicCollection(UniqueCollection());

        await col.SetVectorSourceAsync([("field_a", null, null), ("field_b", null, null)]);
        await col.SetVectorSourceAsync([("field_c", null, null)]);

        var config = await col.GetVectorSourceAsync();

        Assert.NotNull(config);
        Assert.Single(config.Fields);
        Assert.Equal("field_c", config.Fields[0].Path);
    }
}
