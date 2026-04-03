// BLite.Server.Benchmarks — QueryBenchmarks
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Server-side query benchmarks: BLite gRPC vs MongoDB on a fixed 5 000-document
// collection.  No [Params] — each benchmark runs exactly once per job iteration.
//
// Categories:
//   Query          — eq filter, indexed field (category), top 100
//   QueryRange     — AND range filter on unindexed field (price), top 50
//   QuerySortTop10 — eq filter + sort desc + take 10
//   QueryCount     — eq filter, full collection count

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

using BLiteBsonDoc = BLite.Bson.BsonDocument;
using MongoBsonDoc = MongoDB.Bson.BsonDocument;

namespace BLite.Server.Benchmarks;

[SimpleJob(warmupCount: 3, iterationCount: 10)]
[MemoryDiagnoser]
[RankColumn]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class QueryBenchmarks
{
    // ── Connection config ─────────────────────────────────────────────────────

    private static string BLiteUrl    => Environment.GetEnvironmentVariable("BLITE_URL")     ?? "http://localhost:2626";
    private static string BLiteApiKey => Environment.GetEnvironmentVariable("BLITE_API_KEY") ?? "bench-key";
    private static string MongoUrl    => Environment.GetEnvironmentVariable("MONGO_URL")     ?? "mongodb://localhost:27017";

    private const string CollectionName = "benchmark_query";
    private const string MongoDbName    = "blite_bench";
    private const int    SeedCount      = 5_000;

    private static readonly string[] Categories = ["electronics", "books", "clothing", "food", "sports"];

    // ── State ─────────────────────────────────────────────────────────────────

    private BLiteClient             _bliteClient = null!;
    private RemoteDynamicCollection _bliteCol    = null!;
    private WebApplicationFactory<EngineRegistry>? _hostedServer;

    private MongoClient?                    _mongoClient;
    private IMongoCollection<MongoBsonDoc>? _mongoCol;
    private bool                            _mongoAvailable;

    // ── Setup / teardown ──────────────────────────────────────────────────────

    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

        if (await IsTcpReachableAsync(BLiteUrl))
        {
            _bliteClient = new BLiteClient(new BLiteClientOptions
            {
                Address = BLiteUrl,
                ApiKey  = BLiteApiKey,
                UseTls  = false
            });
        }
        else
        {
            Console.WriteLine("[QueryBenchmarks] BLite server not reachable; starting in-process.");

            var dbPath     = Path.Combine(Path.GetTempPath(), $"blite_qbench_{Guid.NewGuid():N}.db");
            var tenantsDir = Path.Combine(Path.GetTempPath(), $"blite_qbench_tenants_{Guid.NewGuid():N}");

            _hostedServer = new WebApplicationFactory<EngineRegistry>()
                .WithWebHostBuilder(b =>
                {
                    b.UseSetting("Auth:RootKey",                   BLiteApiKey);
                    b.UseSetting("BLiteServer:DatabasePath",       dbPath);
                    b.UseSetting("BLiteServer:DatabasesDirectory", tenantsDir);
                    b.UseSetting("Telemetry:Enabled",              "false");
                    b.UseSetting("Studio:Enabled",                 "false");
                    b.UseSetting("Kestrel:Endpoints:Rest:Url",     "");
                    b.UseSetting("Kestrel:Endpoints:Studio:Url",   "");
                });

            _ = _hostedServer.CreateClient();

            var handler = _hostedServer.Server.CreateHandler();
            var channel = GrpcChannel.ForAddress("http://localhost", new GrpcChannelOptions
            {
                HttpHandler = handler
            });
            _bliteClient = new BLiteClient(channel, BLiteApiKey);
        }

        _mongoAvailable = await IsTcpReachableAsync(MongoUrl);
        if (_mongoAvailable)
        {
            _mongoClient = new MongoClient(MongoUrl);
        }
        else
        {
            Console.WriteLine("[QueryBenchmarks] MongoDB not reachable; Mongo benchmarks will be reported as errors.");
        }

        // Drop any leftovers from previous runs.
        await _bliteClient.DropCollectionAsync(CollectionName);
        if (_mongoAvailable)
            await _mongoClient!.GetDatabase(MongoDbName).DropCollectionAsync(CollectionName);

        _bliteCol = _bliteClient.GetDynamicCollection(CollectionName);

        await _bliteCol.NewDocumentAsync(
            ["name", "category", "price", "stock", "active"],
            _ => { });

        if (_mongoAvailable)
        {
            var db = _mongoClient!.GetDatabase(MongoDbName);
            _mongoCol = db.GetCollection<MongoBsonDoc>(CollectionName);
            await _mongoCol.Indexes.CreateOneAsync(
                new CreateIndexModel<MongoBsonDoc>(
                    Builders<MongoBsonDoc>.IndexKeys.Ascending("category")));
        }

        // Index on category so both engines use an index for the Eq filter.
        await _bliteCol.CreateIndexAsync("category");

        // Seed 5 000 documents: 1 000 per category, price 0–7 499 (step 1.5).
        await SeedAsync(SeedCount);
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

    // ── Query: eq on indexed field, top 100 ──────────────────────────────────

    [BenchmarkCategory("Query"), Benchmark(Baseline = true, Description = "BLite gRPC")]
    public async Task<int> QueryByCategory_BLite()
    {
        var descriptor = new QueryDescriptor
        {
            Collection = _bliteCol.Name,
            Where = new BinaryFilter
            {
                Field = "category",
                Op    = FilterOp.Eq,
                Value = ScalarValue.From("electronics")
            },
            Take = 100
        };
        int count = 0;
        await foreach (var _ in _bliteCol.QueryAsync(descriptor))
            count++;
        return count;
    }

    [BenchmarkCategory("Query"), Benchmark(Description = "MongoDB")]
    public async Task<int> QueryByCategory_Mongo()
    {
        ThrowIfMongoUnavailable();
        var filter = Builders<MongoBsonDoc>.Filter.Eq("category", "electronics");
        var docs   = await _mongoCol!.Find(filter).Limit(100).ToListAsync();
        return docs.Count;
    }

    // ── Query: AND range filter on unindexed field, top 50 ───────────────────

    [BenchmarkCategory("QueryRange"), Benchmark(Baseline = true, Description = "BLite gRPC")]
    public async Task<int> QueryByPriceRange_BLite()
    {
        var descriptor = new QueryDescriptor
        {
            Collection = _bliteCol.Name,
            Where = new LogicalFilter
            {
                Op = LogicalOp.And,
                Children =
                [
                    new BinaryFilter { Field = "price", Op = FilterOp.GtEq, Value = ScalarValue.From(1000.0) },
                    new BinaryFilter { Field = "price", Op = FilterOp.Lt,   Value = ScalarValue.From(3000.0) }
                ]
            },
            Take = 50
        };
        int count = 0;
        await foreach (var _ in _bliteCol.QueryAsync(descriptor))
            count++;
        return count;
    }

    [BenchmarkCategory("QueryRange"), Benchmark(Description = "MongoDB")]
    public async Task<int> QueryByPriceRange_Mongo()
    {
        ThrowIfMongoUnavailable();
        var filter = Builders<MongoBsonDoc>.Filter.And(
            Builders<MongoBsonDoc>.Filter.Gte("price", 1000.0),
            Builders<MongoBsonDoc>.Filter.Lt("price",  3000.0));
        var docs = await _mongoCol!.Find(filter).Limit(50).ToListAsync();
        return docs.Count;
    }

    // ── Query: eq + sort desc + take 10 ──────────────────────────────────────

    [BenchmarkCategory("QuerySortTop10"), Benchmark(Baseline = true, Description = "BLite gRPC")]
    public async Task<int> QuerySortedTop10_BLite()
    {
        var descriptor = new QueryDescriptor
        {
            Collection = _bliteCol.Name,
            Where   = new BinaryFilter { Field = "category", Op = FilterOp.Eq, Value = ScalarValue.From("electronics") },
            OrderBy = [new SortSpec { Field = "price", Descending = true }],
            Take    = 10
        };
        int count = 0;
        await foreach (var _ in _bliteCol.QueryAsync(descriptor))
            count++;
        return count;
    }

    [BenchmarkCategory("QuerySortTop10"), Benchmark(Description = "MongoDB")]
    public async Task<int> QuerySortedTop10_Mongo()
    {
        ThrowIfMongoUnavailable();
        var filter = Builders<MongoBsonDoc>.Filter.Eq("category", "electronics");
        var sort   = Builders<MongoBsonDoc>.Sort.Descending("price");
        var docs   = await _mongoCol!.Find(filter).Sort(sort).Limit(10).ToListAsync();
        return docs.Count;
    }

    // ── Query: eq filter, count all matching ─────────────────────────────────

    [BenchmarkCategory("QueryCount"), Benchmark(Baseline = true, Description = "BLite gRPC")]
    public async Task<int> QueryCount_BLite()
    {
        var descriptor = new QueryDescriptor
        {
            Collection = _bliteCol.Name,
            Where = new BinaryFilter { Field = "category", Op = FilterOp.Eq, Value = ScalarValue.From("books") }
        };
        int count = 0;
        await foreach (var _ in _bliteCol.QueryAsync(descriptor))
            count++;
        return count;
    }

    [BenchmarkCategory("QueryCount"), Benchmark(Description = "MongoDB")]
    public async Task<long> QueryCount_Mongo()
    {
        ThrowIfMongoUnavailable();
        var filter = Builders<MongoBsonDoc>.Filter.Eq("category", "books");
        return await _mongoCol!.CountDocumentsAsync(filter);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private async Task SeedAsync(int count)
    {
        var bliteDocs = new BLiteBsonDoc[count];
        for (int i = 0; i < count; i++)
            bliteDocs[i] = await _bliteCol.NewDocumentAsync(
                ["name", "category", "price", "stock", "active"],
                b => b.AddString ("name",     $"seed-{i}")
                      .AddString ("category", Categories[i % 5])
                      .AddDouble ("price",    i * 1.5)
                      .AddInt32  ("stock",    i % 500)
                      .AddBoolean("active",   true));
        await _bliteCol.InsertBulkAsync(bliteDocs);

        if (_mongoAvailable)
        {
            var mongoDocs = new List<MongoBsonDoc>(count);
            for (int i = 0; i < count; i++)
                mongoDocs.Add(new MongoBsonDoc
                {
                    ["name"]     = $"seed-{i}",
                    ["category"] = Categories[i % 5],
                    ["price"]    = i * 1.5,
                    ["stock"]    = i % 500,
                    ["active"]   = true
                });
            await _mongoCol!.InsertManyAsync(mongoDocs);
        }
    }

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
