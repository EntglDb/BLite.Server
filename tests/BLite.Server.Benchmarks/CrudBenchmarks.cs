// BLite.Server.Benchmarks — CrudBenchmarks
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Compares BLite.Server (gRPC / h2c) vs MongoDB (TCP) for common CRUD patterns.
// Both servers must be running before this process starts — see:
//   deploy/benchmark/docker-compose.benchmark.yml
//
// Connection URLs are read from environment variables:
//   BLITE_URL      (default: http://localhost:2626)
//   BLITE_API_KEY  (default: bench-key)
//   MONGO_URL      (default: mongodb://localhost:27017)

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Order;
using BLite.Client;
using BLite.Client.Collections;
using BLite.Proto;
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

    private BLiteClient             _bliteClient  = null!;
    private RemoteDynamicCollection _bliteCol     = null!;
    private BLiteBsonId             _bliteSeedId;   // stable ID for FindById benchmark
    private BLiteBsonId             _bliteWriteId;  // refreshed per Update/Delete iteration

    private MongoClient                    _mongoClient = null!;
    private IMongoCollection<MongoBsonDoc> _mongoCol    = null!;
    private MongoDB.Bson.ObjectId          _mongoSeedId;
    private MongoDB.Bson.ObjectId          _mongoWriteId;

    // ── Global setup / teardown ───────────────────────────────────────────────

    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        // Allow HTTP/2 cleartext (h2c) — required when UseTls = false.
        AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

        // ── BLite ──────────────────────────────────────────────────────────────
        _bliteClient = new BLiteClient(new BLiteClientOptions
        {
            Address = BLiteUrl,
            ApiKey  = BLiteApiKey,
            UseTls  = false
        });

        // ── MongoDB ────────────────────────────────────────────────────────────
        _mongoClient = new MongoClient(MongoUrl);

        // Drop any leftover data so re-runs always start from a known baseline.
        await _bliteClient.DropCollectionAsync(CollectionName);
        await _mongoClient.GetDatabase(MongoDbName).DropCollectionAsync(CollectionName);

        // ── Create collection handles ──────────────────────────────────────────
        _bliteCol = _bliteClient.GetDynamicCollection(CollectionName);

        // Prime the client key map once — subsequent NewDocumentAsync calls for
        // the same field set use the local in-memory cache.
        await _bliteCol.NewDocumentAsync(
            ["name", "category", "price", "stock", "active"],
            _ => { });

        _mongoCol = _mongoClient.GetDatabase(MongoDbName)
                                .GetCollection<MongoBsonDoc>(CollectionName);

        // Index on "category" mirrors what a DBA would add for the query benchmark.
        await _mongoCol.Indexes.CreateOneAsync(
            new CreateIndexModel<MongoBsonDoc>(
                Builders<MongoBsonDoc>.IndexKeys.Ascending("category")));

        // ── Seed: 1 000 documents (200 per category) ──────────────────────────
        // Ensures the query benchmark returns real rows from both engines.
        await SeedManyAsync(1_000);

        // One stable document per engine for the FindById benchmark.
        _bliteSeedId = await InsertOneBLiteAsync("seed-read-doc", "electronics", 99.99, 50);
        _mongoSeedId = await InsertOneMongoAsync("seed-read-doc", "electronics", 99.99, 50);
    }

    [GlobalCleanup]
    public async Task GlobalCleanupAsync()
    {
        await _bliteClient.DropCollectionAsync(CollectionName);
        await _mongoClient.GetDatabase(MongoDbName).DropCollectionAsync(CollectionName);
        await _bliteClient.DisposeAsync();
        _mongoClient.Dispose();
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
        => _mongoWriteId = InsertOneMongoAsync($"write-{Guid.NewGuid():N}", "books", 1.0, 1)
                               .GetAwaiter().GetResult();

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
        => _mongoCol.InsertOneAsync(MakeMongoDoc("bench-insert", "electronics", 9.99, 100));

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
        var docs = new List<MongoBsonDoc>(BulkSize);
        for (int i = 0; i < BulkSize; i++)
            docs.Add(MakeMongoDoc($"bulk-{i}", Categories[i % 5], i * 0.99, i));
        await _mongoCol.InsertManyAsync(docs);
    }

    // ── FindById ──────────────────────────────────────────────────────────────

    [BenchmarkCategory("FindById"), Benchmark(Baseline = true, Description = "BLite gRPC")]
    public Task<BLiteBsonDoc?> FindById_BLite()
        => _bliteCol.FindByIdAsync(_bliteSeedId);

    [BenchmarkCategory("FindById"), Benchmark(Description = "MongoDB")]
    public async Task FindById_Mongo()
    {
        var filter = Builders<MongoBsonDoc>.Filter.Eq("_id", _mongoSeedId);
        _ = await _mongoCol.Find(filter).FirstOrDefaultAsync();
    }

    // ── Query (server-side category filter, top 100) ──────────────────────────

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
        var filter = Builders<MongoBsonDoc>.Filter.Eq("category", "electronics");
        var docs   = await _mongoCol.Find(filter).Limit(100).ToListAsync();
        return docs.Count;
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
        var filter = Builders<MongoBsonDoc>.Filter.Eq("_id", _mongoWriteId);
        var update = Builders<MongoBsonDoc>.Update
            .Set("name",  "updated-name")
            .Set("price", 1.99)
            .Set("stock", 0);
        return _mongoCol.UpdateOneAsync(filter, update);
    }

    // ── Delete (single) ───────────────────────────────────────────────────────

    [BenchmarkCategory("Delete-1"), Benchmark(Baseline = true, Description = "BLite gRPC")]
    public Task<bool> DeleteOne_BLite()
        => _bliteCol.DeleteAsync(_bliteWriteId);

    [BenchmarkCategory("Delete-1"), Benchmark(Description = "MongoDB")]
    public Task DeleteOne_Mongo()
    {
        var filter = Builders<MongoBsonDoc>.Filter.Eq("_id", _mongoWriteId);
        return _mongoCol.DeleteOneAsync(filter);
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
        await _mongoCol.InsertOneAsync(doc);
        return doc["_id"].AsObjectId;
    }

    private async Task SeedManyAsync(int count)
    {
        var bliteDocs = new BLiteBsonDoc[count];
        for (int i = 0; i < count; i++)
            bliteDocs[i] = await MakeBLiteDocAsync(
                $"seed-{i}", Categories[i % 5], i * 1.5, i % 500);
        await _bliteCol.InsertBulkAsync(bliteDocs);

        var mongoDocs = new List<MongoBsonDoc>(count);
        for (int i = 0; i < count; i++)
            mongoDocs.Add(MakeMongoDoc($"seed-{i}", Categories[i % 5], i * 1.5, i % 500));
        await _mongoCol.InsertManyAsync(mongoDocs);
    }
}
