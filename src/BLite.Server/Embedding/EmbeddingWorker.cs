// BLite.Server — EmbeddingWorker
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

using BLite.Bson;
using BLite.Core;
using BLite.Core.Indexing;
using BLite.Core.Storage;
using BLite.Core.Text;
using BLite.Server.Embedding;
using Microsoft.Extensions.Options;

namespace BLite.Server.Embedding;

/// <summary>
/// Background service that periodically wakes up, takes a batch of embedding tasks,
/// computes embeddings, and persists them to the documents.
/// </summary>
public sealed class EmbeddingWorker : BackgroundService
{
    private readonly IEmbeddingQueue _queue;
    private readonly EngineRegistry _registry;
    private readonly EmbeddingService _embedding;
    private readonly IOptionsMonitor<EmbeddingWorkerOptions> _options;
    private readonly ILogger<EmbeddingWorker> _logger;

    public EmbeddingWorker(
        IEmbeddingQueue queue,
        EngineRegistry registry,
        EmbeddingService embedding,
        IOptionsMonitor<EmbeddingWorkerOptions> options,
        ILogger<EmbeddingWorker> logger)
    {
        _queue = queue;
        _registry = registry;
        _embedding = embedding;
        _options = options;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("EmbeddingWorker starting");

        // Initial delay to let the server stabilize
        await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            var opts = _options.CurrentValue;

            if (opts.Enabled && _embedding.IsLoaded)
            {
                try
                {
                    await ProcessBatchAsync(opts, stoppingToken);
                }
                catch (OperationCanceledException) { }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in embedding worker cycle");
                }
            }

            await Task.Delay(TimeSpan.FromSeconds(opts.IntervalSeconds), stoppingToken);
        }

        _logger.LogInformation("EmbeddingWorker stopped");
    }

    private async Task ProcessBatchAsync(EmbeddingWorkerOptions opts, CancellationToken ct)
    {
        var tasks = await _queue.TakeBatchAsync(opts.BatchSize, ct);

        if (tasks.Count == 0)
            return;

        _logger.LogDebug("Processing {Count} embedding tasks", tasks.Count);

        // Group by database
        var byDb = tasks.GroupBy(t => t.Database).ToList();

        var completedIds = new List<BsonId>();

        foreach (var dbGroup in byDb)
        {
            try
            {
                await ProcessDatabaseGroupAsync(dbGroup, opts, completedIds, ct);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to process database group {Db}", dbGroup.Key ?? "system");
            }
        }

        // Mark all completed tasks as done
        if (completedIds.Count > 0)
        {
            await _queue.CompleteAsync(completedIds, ct);
        }

        _logger.LogDebug("Completed processing of {Count} tasks", completedIds.Count);
    }

    private async Task ProcessDatabaseGroupAsync(
        IGrouping<string?, EmbeddingTask> dbGroup,
        EmbeddingWorkerOptions opts,
        List<BsonId> completedIds,
        CancellationToken ct)
    {
        var dbId = dbGroup.Key;
        var engine = _registry.GetEngine(dbId);

        // Phase 1: Compute embeddings (CPU-bound, outside transaction)
        var toUpdate = new List<(EmbeddingTask Task, BsonId DocId, string FieldPath, float[] Vector)>();

        foreach (var task in dbGroup)
        {
            try
            {
                var config = engine.GetVectorSource(task.Collection);
                if (config == null)
                {
                    completedIds.Add(task.Id);
                    continue;
                }

                var indexes = engine.GetIndexDescriptors(task.Collection);
                var vecIdx = indexes.FirstOrDefault(d => d.Type == IndexType.Vector);
                if (vecIdx == null)
                {
                    completedIds.Add(task.Id);
                    continue;
                }

                var col = engine.GetOrCreateCollection(task.Collection);
                var doc = col.FindById(task.DocumentId);
                if (doc == null)
                {
                    completedIds.Add(task.Id);
                    continue;
                }

                var text = TextNormalizer.BuildEmbeddingText(doc, config);
                if (string.IsNullOrWhiteSpace(text))
                {
                    completedIds.Add(task.Id);
                    continue;
                }

                var vector = _embedding.Embed(text);
                toUpdate.Add((task, task.DocumentId, vecIdx.FieldPath, vector));
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to compute embedding for {Db}:{Col}:{DocId}",
                    dbId ?? "system", task.Collection, task.DocumentId);
                // Task stays in_progress → will become stale
            }
        }

        if (toUpdate.Count == 0)
            return;

        // Phase 2: Persist (single transaction per database)
        var localCompleted = new List<BsonId>();
        engine.BeginTransaction();
        try
        {
            foreach (var (task, docId, fieldPath, vector) in toUpdate)
            {
                var col = engine.GetOrCreateCollection(task.Collection);
                var doc = col.FindById(docId);
                if (doc == null)
                    continue;

                var updated = BuildDocumentWithVector(engine, doc, fieldPath, vector);
                col.Update(docId, updated);
                localCompleted.Add(task.Id);
            }

            await engine.CommitAsync(ct);
            completedIds.AddRange(localCompleted);
            _logger.LogDebug("Committed {Count} embedding updates for {Db}", localCompleted.Count, dbId ?? "system");
        }
        catch (Exception ex)
        {
            engine.Rollback();
            // localCompleted is discarded — tasks stay in_progress and become stale
            _logger.LogError(ex, "Failed to commit embeddings for {Db}", dbId ?? "system");
            throw;
        }
    }

    private static BsonDocument BuildDocumentWithVector(
        BLiteEngine engine, BsonDocument doc, string fieldPath, float[] vector)
    {
        engine.RegisterKeys([fieldPath]);
        var keyMap = (System.Collections.Concurrent.ConcurrentDictionary<string, ushort>)engine.GetKeyMap();
        var reverseMap = (System.Collections.Concurrent.ConcurrentDictionary<ushort, string>)engine.GetKeyReverseMap();
        var builder = new BsonDocumentBuilder(keyMap, reverseMap);

        if (doc.TryGetId(out var id))
            builder.AddId(id);

        foreach (var (name, value) in doc.EnumerateFields())
        {
            if (name != fieldPath)
                builder.Add(name, value);
        }

        builder.AddFloatArray(fieldPath, vector);
        return builder.Build();
    }
}
