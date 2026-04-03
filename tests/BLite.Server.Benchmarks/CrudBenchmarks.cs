// BLite.Server.Benchmarks — CrudBenchmarks
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Compares BLite.Server (gRPC / h2c) vs MongoDB (TCP) for common CRUD patterns.
//
// Run modes:
//   1. Via run script (Docker, recommended for CI):  .\deploy\benchmark\run-benchmarks.ps1
//   2. Self-hosted (no Docker needed): dotnet run -c Release --project tests/BLite.Server.Benchmarks
//      → BLite starts in-process automatically when localhost:2626 is not reachable.
//      → MongoDB benchmarks are skipped when localhost:27017 is not reachable.
//
// Connection URLs are read from environment variables:
//   BLITE_URL      (default: http://localhost:2626)
//   BLITE_API_KEY  (default: bench-key)
//   MONGO_URL      (default: mongodb://localhost:27017)

using System.Net.Sockets;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Order;
using BLite.Client;
using BLite.Client.Collections;
using BLite.Proto;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Mvc.Testing;
using MongoDB.Driver;

// Disambiguate the two BsonDocument types that would otherwise clash.
using BLiteBsonDoc = BLite.Bson.BsonDocument;
using BLiteBsonId  = BLite.Bson.BsonId;
using MongoBsonDoc = MongoDB.Bson.BsonDocument;

namespace BLite.Server.Benchmarks;

[SimpleJob(warmupCount: 3, iterationCount: 10)]
[MemoryDiagnoser]
[RankColumn]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class CrudBenchmarks
{
    // ── Parameters ────────────────────────────────────────────────────────────

    /// <summary>Number of documents used in bulk-insert benchmarks.</summary>
    [Params(10, 100, 1000)]
    public int BulkSize { get; set; }

    // ── Connection config ─────────────────────────────────────────────────────

    private static string BLiteUrl    => Environment.GetEnvironmentVariable("BLITE_URL")     ?? "http://localhost:2626";
    private static string BLiteApiKey => Environment.GetEnvironmentVariable("BLITE_API_KEY") ?? "bench-key";
    private static string MongoUrl    => Environment.GetEnvironmentVariable("MONGO_URL")     ?? "mongodb://localhost:27017";

    // Each BulkSize param gets its own collection so InsertBulk runs don't
    // accumulate documents that inflate Query / Update / Delete measurements.
    private string CollectionName => $"benchmark_products_{BulkSize}";
    private const string MongoDbName = "blite_bench";

    private static readonly string[] Categories = ["electronics", "books", "clothing", "food", "sports"];

    // ── State ─────────────────────────────────────────────────────────────────

    // Non-null after GlobalSetup; may use in-process channel when self-hosted.
    private BLiteClient             _bliteClient  = null!;
    private RemoteDynamicCollection _bliteCol     = null!;
    private BLiteBsonId             _bliteSeedId;   // stable ID for FindById benchmark
    private BLiteBsonId             _bliteWriteId;  // refreshed per Update/Delete iteration

    // Non-null when an external BLite server was not detected at startup.
    // EngineRegistry is used as the type parameter to identify the BLite.Server
    // assembly without conflicting with this project's own implicit Program class.
    private WebApplicationFactory<EngineRegistry>? _hostedServer;

    // Null when MongoDB is not reachable — Mongo benchmarks report errors instead of crashing.
    private MongoClient?                    _mongoClient;
    private IMongoCollection<MongoBsonDoc>? _mongoCol;   // default write concern (w:1, j:false)
    private MongoDB.Bson.ObjectId           _mongoSeedId;
    private MongoDB.Bson.ObjectId           _mongoWriteId;
    private bool                            _mongoAvailable;

    // ── Global setup / teardown ───────────────────────────────────────────────

    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        // Allow HTTP/2 cleartext (h2c) — required when UseTls = false.
        AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

        // ── BLite: external server or in-process fallback ──────────────────────
        if (await IsTcpReachableAsync(BLiteUrl))
        {
            // External server is running (Docker / manual start).
            _bliteClient = new BLiteClient(new BLiteClientOptions
            {
                Address = BLiteUrl,
                ApiKey  = BLiteApiKey,
                UseTls  = false
            });
        }
        else
        {
            // No external server detected — start BLite in-process.
            Console.WriteLine("[Benchmarks] BLite server not reachable; starting in-process.");

            var dbPath = Path.Combine(Path.GetTempPath(), $"blite_bench_{Guid.NewGuid():N}.db");
            var tenantsDir = Path.Combine(Path.GetTempPath(), $"blite_bench_tenants_{Guid.NewGuid():N}");

            _hostedServer = new WebApplicationFactory<EngineRegistry>()
                .WithWebHostBuilder(b =>
                {
                    b.UseSetting("Auth:RootKey",                  BLiteApiKey);
                    b.UseSetting("BLiteServer:DatabasePath",      dbPath);
                    b.UseSetting("BLiteServer:DatabasesDirectory", tenantsDir);
                    b.UseSetting("Telemetry:Enabled",             "false");
                    b.UseSetting("Studio:Enabled",                "false");
                    // Disable the separate REST/Studio port config so TestServer
                    // does not apply RequireHost() constraints to routes.
                    b.UseSetting("Kestrel:Endpoints:Rest:Url",   "");
                    b.UseSetting("Kestrel:Endpoints:Studio:Url", "");
                });

            // Trigger server startup.
            _ = _hostedServer.CreateClient();

            // Route gRPC through the in-process handler — no TCP overhead.
            var handler = _hostedServer.Server.CreateHandler();
            var channel = GrpcChannel.ForAddress("http://localhost", new GrpcChannelOptions
            {
                HttpHandler = handler
            });
            _bliteClient = new BLiteClient(channel, BLiteApiKey);
        }

        // ── MongoDB: optional ──────────────────────────────────────────────────
        _mongoAvailable = await IsTcpReachableAsync(MongoUrl);
        if (_mongoAvailable)
        {
            _mongoClient = new MongoClient(MongoUrl);
        }
        else
        {
            Console.WriteLine("[Benchmarks] MongoDB not reachable; Mongo benchmarks will be reported as errors.");
        }

        // Drop any leftover data so re-runs always start from a known baseline.
        await _bliteClient.DropCollectionAsync(CollectionName);
        if (_mongoAvailable)
            await _mongoClient!.GetDatabase(MongoDbName).DropCollectionAsync(CollectionName);

        // ── Create collection handles ──────────────────────────────────────────
        _bliteCol = _bliteClient.GetDynamicCollection(CollectionName);

        // Prime the client key map once — subsequent NewDocumentAsync calls for
        // the same field set use the local in-memory cache.
        await _bliteCol.NewDocumentAsync(
            ["name", "category", "price", "stock", "active"],
            _ => { });

        if (_mongoAvailable)
        {
            var db = _mongoClient!.GetDatabase(MongoDbName);
            _mongoCol = db.GetCollection<MongoBsonDoc>(CollectionName);

            // Index on "category" mirrors what a DBA would add for the query benchmark.
            await _mongoCol.Indexes.CreateOneAsync(
                new CreateIndexModel<MongoBsonDoc>(
                    Builders<MongoBsonDoc>.IndexKeys.Ascending("category")));
        }

        // Matching BTree index on BLite so the query benchmark is apples-to-apples.
        await _bliteCol.CreateIndexAsync("category");

        // ── Seed: 1 000 documents (200 per category) ──────────────────────────
        // Ensures the query benchmark returns real rows from both engines.
        await SeedManyAsync(1_000);

        // One stable document per engine for the FindById benchmark.
        _bliteSeedId = await InsertOneBLiteAsync("seed-read-doc", "electronics", 99.99, 50);
        if (_mongoAvailable)
            _mongoSeedId = await InsertOneMongoAsync("seed-read-doc", "electronics", 99.99, 50);
    }

    [GlobalCleanup]
    public async Task GlobalCleanupAsync()
    {
        await _bliteClient.DropCollectionAsync(CollectionName);
        if (_mongoAvailable)
            await _mongoClient!.GetDatabase(MongoDbName).DropCollectionAsync(CollectionName);

        await _bliteClient.DisposeAsync();
        _mongoClient?.Dispose();

        if (_hostedServer is not null)
            await _hostedServer.DisposeAsync();
    }

    // IterationSetup inserts a fresh document before each Update/Delete iteration
    // so those benchmarks always operate on a valid target.
    // .GetAwaiter().GetResult() is intentional — setup time is excluded from
    // measurements by BenchmarkDotNet.

    [IterationSetup(Targets = [nameof(UpdateOne_BLite), nameof(DeleteOne_BLite)])]
    public void IterationSetup_BLite()
        => _bliteWriteId = InsertOneBLiteAsync($"write-{Guid.NewGuid():N}", "books", 1.0, 1)
                               .GetAwaiter().GetResult();

    [IterationSetup(Targets = [nameof(UpdateOne_Mongo), nameof(DeleteOne_Mongo)])]
    public void IterationSetup_Mongo()
    {
        if (!_mongoAvailable) return;
        _mongoWriteId = InsertOneMongoAsync($"write-{Guid.NewGuid():N}", "books", 1.0, 1)
                            .GetAwaiter().GetResult();
    }

    // ── Insert (single) ───────────────────────────────────────────────────────

    // InvocationCount=1: each call appends a document, so multiple invocations
    // per iteration would grow the collection and bias the measurement.
    [BenchmarkCategory("Insert-1"), Benchmark(Baseline = true, Description = "BLite gRPC"), InvocationCount(1)]
    public async Task InsertOne_BLite()
    {
        var doc = await MakeBLiteDocAsync("bench-insert", "electronics", 9.99, 100);
        await _bliteCol.InsertAsync(doc);
    }

    [BenchmarkCategory("Insert-1"), Benchmark(Description = "MongoDB"), InvocationCount(1)]
    public Task InsertOne_Mongo()
    {
        ThrowIfMongoUnavailable();
        return _mongoCol!.InsertOneAsync(MakeMongoDoc("bench-insert", "electronics", 9.99, 100));
    }

    // ── Insert (bulk) ─────────────────────────────────────────────────────────

    // InvocationCount=1 prevents BDN from running multiple calls per iteration:
    // each call permanently adds BulkSize docs to the collection, so self-
    // contamination would bias later iterations upward.
    [BenchmarkCategory("InsertBulk"), Benchmark(Baseline = true, Description = "BLite gRPC"), InvocationCount(1)]
    public async Task InsertBulk_BLite()
    {
        var docs = new BLiteBsonDoc[BulkSize];
        for (int i = 0; i < BulkSize; i++)
            docs[i] = await MakeBLiteDocAsync($"bulk-{i}", Categories[i % 5], i * 0.99, i);
        await _bliteCol.InsertBulkAsync(docs);
    }

    [BenchmarkCategory("InsertBulk"), Benchmark(Description = "MongoDB"), InvocationCount(1)]
    public async Task InsertBulk_Mongo()
    {
        ThrowIfMongoUnavailable();
        var docs = new List<MongoBsonDoc>(BulkSize);
        for (int i = 0; i < BulkSize; i++)
            docs.Add(MakeMongoDoc($"bulk-{i}", Categories[i % 5], i * 0.99, i));
        await _mongoCol!.InsertManyAsync(docs);
    }

    // ── FindById ──────────────────────────────────────────────────────────────

    [BenchmarkCategory("FindById"), Benchmark(Baseline = true, Description = "BLite gRPC")]
    public Task<BLiteBsonDoc?> FindById_BLite()
        => _bliteCol.FindByIdAsync(_bliteSeedId);

    [BenchmarkCategory("FindById"), Benchmark(Description = "MongoDB")]
    public async Task FindById_Mongo()
    {
        ThrowIfMongoUnavailable();
        var filter = Builders<MongoBsonDoc>.Filter.Eq("_id", _mongoSeedId);
        _ = await _mongoCol!.Find(filter).FirstOrDefaultAsync();
    }

    // ── Update (single) ───────────────────────────────────────────────────────

    [BenchmarkCategory("Update-1"), Benchmark(Baseline = true, Description = "BLite gRPC")]
    public async Task UpdateOne_BLite()
    {
        var doc = await MakeBLiteDocAsync("updated-name", "books", 1.99, 0);
        await _bliteCol.UpdateAsync(_bliteWriteId, doc);
    }

    [BenchmarkCategory("Update-1"), Benchmark(Description = "MongoDB")]
    public Task UpdateOne_Mongo()
    {
        ThrowIfMongoUnavailable();
        var filter = Builders<MongoBsonDoc>.Filter.Eq("_id", _mongoWriteId);
        var update = Builders<MongoBsonDoc>.Update
            .Set("name",  "updated-name")
            .Set("price", 1.99)
            .Set("stock", 0);
        return _mongoCol!.UpdateOneAsync(filter, update);
    }

    // ── Delete (single) ───────────────────────────────────────────────────────

    [BenchmarkCategory("Delete-1"), Benchmark(Baseline = true, Description = "BLite gRPC")]
    public Task<bool> DeleteOne_BLite()
        => _bliteCol.DeleteAsync(_bliteWriteId);

    [BenchmarkCategory("Delete-1"), Benchmark(Description = "MongoDB")]
    public Task DeleteOne_Mongo()
    {
        ThrowIfMongoUnavailable();
        var filter = Builders<MongoBsonDoc>.Filter.Eq("_id", _mongoWriteId);
        return _mongoCol!.DeleteOneAsync(filter);
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private Task<BLiteBsonDoc> MakeBLiteDocAsync(
        string name, string category, double price, int stock)
        => _bliteCol.NewDocumentAsync(
            ["name", "category", "price", "stock", "active"],
            b => b.AddString ("name",     name)
                  .AddString ("category", category)
                  .AddDouble ("price",    price)
                  .AddInt32  ("stock",    stock)
                  .AddBoolean("active",   true));

    private static MongoBsonDoc MakeMongoDoc(
        string name, string category, double price, int stock)
        => new()
        {
            ["name"]     = name,
            ["category"] = category,
            ["price"]    = price,
            ["stock"]    = stock,
            ["active"]   = true
        };

    private async Task<BLiteBsonId> InsertOneBLiteAsync(
        string name, string category, double price, int stock)
    {
        var doc = await MakeBLiteDocAsync(name, category, price, stock);
        return await _bliteCol.InsertAsync(doc);
    }

    private async Task<MongoDB.Bson.ObjectId> InsertOneMongoAsync(
        string name, string category, double price, int stock)
    {
        var doc = MakeMongoDoc(name, category, price, stock);
        await _mongoCol!.InsertOneAsync(doc);
        return doc["_id"].AsObjectId;
    }

    private async Task SeedManyAsync(int count)
    {
        var bliteDocs = new BLiteBsonDoc[count];
        for (int i = 0; i < count; i++)
            bliteDocs[i] = await MakeBLiteDocAsync(
                $"seed-{i}", Categories[i % 5], i * 1.5, i % 500);
        await _bliteCol.InsertBulkAsync(bliteDocs);

        if (_mongoAvailable)
        {
            var mongoDocs = new List<MongoBsonDoc>(count);
            for (int i = 0; i < count; i++)
                mongoDocs.Add(MakeMongoDoc($"seed-{i}", Categories[i % 5], i * 1.5, i % 500));
            await _mongoCol!.InsertManyAsync(mongoDocs);
        }
    }

    // ── Connectivity helpers ──────────────────────────────────────────────────

    // Tries a TCP connect to the host:port encoded in a URL or a mongodb:// URI.
    private static async Task<bool> IsTcpReachableAsync(string url, int timeoutMs = 1000)
    {
        try
        {
            string host;
            int port;
            if (url.StartsWith("mongodb://", StringComparison.OrdinalIgnoreCase))
            {
                var uri = new Uri(url);
                host = uri.Host;
                port = uri.Port > 0 ? uri.Port : 27017;
            }
            else
            {
                var uri = new Uri(url);
                host = uri.Host;
                port = uri.Port > 0 ? uri.Port : 2626;
            }

            using var client = new TcpClient();
            using var cts    = new CancellationTokenSource(timeoutMs);
            await client.ConnectAsync(host, port, cts.Token);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private void ThrowIfMongoUnavailable()
    {
        if (!_mongoAvailable)
            throw new InvalidOperationException(
                "MongoDB is not reachable. Start MongoDB (or use run-benchmarks.ps1) to include Mongo benchmarks.");
    }
}
