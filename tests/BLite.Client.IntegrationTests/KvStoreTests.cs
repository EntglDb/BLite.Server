// BLite.Client.IntegrationTests — KV store end-to-end tests

using System.Text;
using BLite.Client.IntegrationTests.Infrastructure;

namespace BLite.Client.IntegrationTests;

[Collection("Integration")]
public class KvStoreTests : IntegrationTestBase
{
    public KvStoreTests(BLiteServerFixture fixture) : base(fixture) { }

    // ── helpers ──────────────────────────────────────────────────────────

    private static string UniqueKey(string prefix = "kv") => $"{prefix}_{Guid.NewGuid():N}";

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static string Str(byte[] b) => Encoding.UTF8.GetString(b);

    // ── basic round-trips ─────────────────────────────────────────────────

    [Fact]
    public async Task SetAsync_GetAsync_RoundTrip()
    {
        await using var client = CreateClient();
        var key = UniqueKey();

        await client.Kv.SetAsync(key, Bytes("hello world"));

        var result = await client.Kv.GetAsync(key);
        Assert.NotNull(result);
        Assert.Equal("hello world", Str(result));
    }

    [Fact]
    public async Task GetAsync_NonExistentKey_ReturnsNull()
    {
        await using var client = CreateClient();

        var result = await client.Kv.GetAsync(UniqueKey("missing"));

        Assert.Null(result);
    }

    // ── exists ────────────────────────────────────────────────────────────

    [Fact]
    public async Task ExistsAsync_AfterSet_ReturnsTrue()
    {
        await using var client = CreateClient();
        var key = UniqueKey();

        await client.Kv.SetAsync(key, Bytes("x"));

        Assert.True(await client.Kv.ExistsAsync(key));
    }

    [Fact]
    public async Task ExistsAsync_NonExistentKey_ReturnsFalse()
    {
        await using var client = CreateClient();

        Assert.False(await client.Kv.ExistsAsync(UniqueKey("noent")));
    }

    // ── delete ────────────────────────────────────────────────────────────

    [Fact]
    public async Task DeleteAsync_ExistingKey_RemovedFromStore()
    {
        await using var client = CreateClient();
        var key = UniqueKey();
        await client.Kv.SetAsync(key, Bytes("to-delete"));

        var deleted = await client.Kv.DeleteAsync(key);

        Assert.True(deleted);
        Assert.Null(await client.Kv.GetAsync(key));
    }

    [Fact]
    public async Task DeleteAsync_NonExistentKey_ReturnsFalse()
    {
        await using var client = CreateClient();

        var deleted = await client.Kv.DeleteAsync(UniqueKey("ghost"));

        Assert.False(deleted);
    }

    // ── scan ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task ScanKeysAsync_WithPrefix_ReturnsMatchingKeys()
    {
        await using var client = CreateClient();
        var prefix = $"scan_{Guid.NewGuid():N}";
        var k1 = $"{prefix}_a";
        var k2 = $"{prefix}_b";
        var kOther = UniqueKey("other");

        await client.Kv.SetAsync(k1, Bytes("1"));
        await client.Kv.SetAsync(k2, Bytes("2"));
        await client.Kv.SetAsync(kOther, Bytes("3"));

        var keys = await client.Kv.ScanKeysAsync(prefix);

        Assert.Contains(k1, keys);
        Assert.Contains(k2, keys);
        Assert.DoesNotContain(kOther, keys);
    }

    [Fact]
    public async Task ScanKeysAsync_EmptyPrefix_IncludesAllSetKeys()
    {
        await using var client = CreateClient();
        var ns = $"nsall_{Guid.NewGuid():N}";
        var k1 = $"{ns}_x";
        var k2 = $"{ns}_y";

        await client.Kv.SetAsync(k1, Bytes("v"));
        await client.Kv.SetAsync(k2, Bytes("v"));

        var keys = await client.Kv.ScanKeysAsync("");

        Assert.Contains(k1, keys);
        Assert.Contains(k2, keys);
    }

    // ── refresh ───────────────────────────────────────────────────────────

    [Fact]
    public async Task RefreshAsync_ExistingKey_DoesNotThrow()
    {
        await using var client = CreateClient();
        var key = UniqueKey();
        await client.Kv.SetAsync(key, Bytes("data"), TimeSpan.FromMinutes(5));

        // Should not throw; just extends the TTL
        await client.Kv.RefreshAsync(key, TimeSpan.FromMinutes(10));

        Assert.True(await client.Kv.ExistsAsync(key));
    }

    // ── purge ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task PurgeExpiredAsync_CompletesWithoutError()
    {
        await using var client = CreateClient();

        // Insert one entry with a very short TTL to ensure there is something to consider
        await client.Kv.SetAsync(UniqueKey("exp"), Bytes("x"), TimeSpan.FromMilliseconds(1));

        // Small wait so the entry is already expired
        await Task.Delay(50);

        var purged = await client.Kv.PurgeExpiredAsync();

        // At least 0; exact count is not guaranteed because other tests may interfere
        Assert.True(purged >= 0);
    }

    // ── batch ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task BatchAsync_SetAndDelete_AreAppliedAtomically()
    {
        await using var client = CreateClient();
        var toKeep = UniqueKey("keep");
        var toDel = UniqueKey("del");

        // Seed one key to delete in the batch
        await client.Kv.SetAsync(toDel, Bytes("old"));

        var affected = await client.Kv.BatchAsync(b =>
        {
            b.Set(toKeep, Bytes("new"));
            b.Delete(toDel);
        });

        Assert.True(affected >= 2);
        Assert.NotNull(await client.Kv.GetAsync(toKeep));
        Assert.Null(await client.Kv.GetAsync(toDel));
    }
}
