// BLite.Client.IntegrationTests — TimeSeriesTests
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// End-to-end tests for ConfigureTimeSeries and GetTimeSeriesInfo.

using BLite.Client.IntegrationTests.Infrastructure;

namespace BLite.Client.IntegrationTests;

[Collection("Integration")]
public class TimeSeriesTests : IntegrationTestBase
{
    public TimeSeriesTests(BLiteServerFixture fixture) : base(fixture) { }

    [Fact]
    public async Task GetTimeSeriesInfo_Default_IsTimeSeriesFalse()
    {
        await using var client = CreateClient();
        var col = client.GetDynamicCollection(UniqueCollection());

        var info = await col.GetTimeSeriesInfoAsync();

        Assert.False(info.IsTimeSeries);
        Assert.Null(info.TtlFieldName);
        Assert.Equal(0L, info.RetentionMs);
    }

    [Fact]
    public async Task ConfigureTimeSeries_ThenGetInfo_IsTimeSeriesTrue()
    {
        await using var client = CreateClient();
        var col = client.GetDynamicCollection(UniqueCollection());
        var retention = TimeSpan.FromDays(30);

        await col.ConfigureTimeSeriesAsync("timestamp", retention);

        var info = await col.GetTimeSeriesInfoAsync();

        Assert.True(info.IsTimeSeries);
        Assert.Equal("timestamp", info.TtlFieldName);
        Assert.Equal((long)retention.TotalMilliseconds, info.RetentionMs);
    }

    [Fact]
    public async Task ConfigureTimeSeries_DifferentRetentions_Stored()
    {
        await using var client = CreateClient();

        foreach (var days in new[] { 1, 7, 365 })
        {
            var col = client.GetDynamicCollection(UniqueCollection());
            var retention = TimeSpan.FromDays(days);

            await col.ConfigureTimeSeriesAsync("ts", retention);

            var info = await col.GetTimeSeriesInfoAsync();
            Assert.Equal((long)retention.TotalMilliseconds, info.RetentionMs);
        }
    }

    [Fact]
    public async Task ConfigureTimeSeries_InsertDocument_DocumentIsAccessible()
    {
        await using var client = CreateClient();
        var col = client.GetDynamicCollection(UniqueCollection());
        await col.ConfigureTimeSeriesAsync("ts", TimeSpan.FromDays(7));

        var doc = await col.NewDocumentAsync(
            ["ts", "value"],
            b => b.AddDateTime("ts", DateTime.UtcNow).AddInt32("value", 42));
        var id = await col.InsertAsync(doc);

        Assert.False(id.IsEmpty);
        var found = await col.FindByIdAsync(id);
        Assert.NotNull(found);
        Assert.True(found.TryGetInt32("value", out var v));
        Assert.Equal(42, v);
    }

    [Fact]
    public async Task ConfigureTimeSeries_CustomTtlField_TtlFieldNameRoundTrips()
    {
        await using var client = CreateClient();
        var col = client.GetDynamicCollection(UniqueCollection());

        await col.ConfigureTimeSeriesAsync("event_time", TimeSpan.FromDays(90));

        var info = await col.GetTimeSeriesInfoAsync();
        Assert.Equal("event_time", info.TtlFieldName);
    }
}
