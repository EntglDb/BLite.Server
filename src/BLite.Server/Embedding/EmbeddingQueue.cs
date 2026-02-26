// BLite.Server — EmbeddingQueue
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

using System.Collections.Concurrent;
using BLite.Bson;
using BLite.Core;
using BLite.Core.Storage;
using Microsoft.Extensions.Options;

namespace BLite.Server.Embedding;

/// <summary>
/// Persistent embedding queue backed by the system database's _emb_queue collection.
/// </summary>
public sealed class EmbeddingQueue : IEmbeddingQueue
{
    private readonly EngineRegistry _registry;
    private readonly IOptionsMonitor<EmbeddingWorkerOptions> _options;
    private readonly ILogger<EmbeddingQueue> _logger;

    public EmbeddingQueue(
        EngineRegistry registry,
        IOptionsMonitor<EmbeddingWorkerOptions> options,
        ILogger<EmbeddingQueue> logger)
    {
        _registry = registry;
        _options = options;
        _logger = logger;
    }

    private const string QueueCollectionName = "_emb_queue";

    public async Task EnqueueAsync(string? databaseId, string collection, BsonId documentId, CancellationToken ct = default)
    {
        var engine = _registry.SystemEngine;
        var col = engine.GetOrCreateCollection(QueueCollectionName);

        var key = BuildKey(databaseId, collection, documentId);

        // Find existing task with this key that's not done
        var existing = col.FindAll()
            .FirstOrDefault(doc =>
            {
                var docKey = doc.TryGetValue("key", out var kv) ? kv.AsString : "";
                var docStatus = doc.TryGetValue("rawStatus", out var sv) ? sv.AsString : "todo";
                return docKey == key && docStatus != "done";
            });

        if (existing != null)
        {
            // Delete old task — new one takes its place
            if (existing.TryGetId(out var oldId))
            {
                await col.DeleteAsync(oldId, ct);
            }
        }

        // Insert new task — preserve the original BsonId type so FindById works correctly
        var task = col.CreateDocument(
            ["key", "db", "col", "doc", "enqueuedAt", "changedAt", "rawStatus"],
            b =>
            {
                b.AddString("key", key);
                b.AddString("db", databaseId ?? "");
                b.AddString("col", collection);
                AddBsonId(b, "doc", documentId);
                b.AddDateTime("enqueuedAt", DateTime.UtcNow);
                b.AddDateTime("changedAt", DateTime.UtcNow);
                b.AddString("rawStatus", "todo");
            });

        await col.InsertAsync(task, ct);
        await engine.CommitAsync(ct);

        _logger.LogDebug("Enqueued embedding task for {Db}:{Col}:{DocId}", databaseId ?? "system", collection, documentId);
    }

    public async Task<IReadOnlyList<EmbeddingTask>> TakeBatchAsync(int batchSize, CancellationToken ct = default)
    {
        var engine = _registry.SystemEngine;
        var col = engine.GetOrCreateCollection(QueueCollectionName);
        var opts = _options.CurrentValue;

        // Fetch all tasks and filter in-memory (simple for now)
        var allTasks = col.FindAll().ToList();
        var now = DateTime.UtcNow;

        var tasks = allTasks
            .Where(doc =>
            {
                var status = doc.TryGetValue("rawStatus", out var sv) ? sv.AsString : "todo";
                var changedAt = doc.TryGetValue("changedAt", out var cv) ? cv.AsDateTime : now;

                if (status == "todo")
                    return true;
                if (status == "in_progress" && (now - changedAt).TotalMinutes > opts.StaleTimeoutMinutes)
                    return true;
                return false;
            })
            .OrderBy(doc => doc.TryGetValue("enqueuedAt", out var ev) ? ev.AsDateTime : now)
            .Take(batchSize)
            .ToList();

        if (tasks.Count == 0)
            return [];

        // Mark as in_progress
        foreach (var doc in tasks)
        {
            if (doc.TryGetId(out var id))
            {
                var updated = UpdateTaskStatus(doc, "in_progress", engine);
                await col.UpdateAsync(id, updated, ct);
            }
        }

        await engine.CommitAsync(ct);

        return tasks.Select(EmbeddingTask.FromDocument).ToList();
    }

    public async Task CompleteAsync(IReadOnlyList<BsonId> taskIds, CancellationToken ct = default)
    {
        if (taskIds.Count == 0)
            return;

        var engine = _registry.SystemEngine;
        var col = engine.GetOrCreateCollection(QueueCollectionName);

        foreach (var taskId in taskIds)
        {
            var doc = col.FindById(taskId);
            if (doc != null)
            {
                var updated = UpdateTaskStatus(doc, "done", engine);
                await col.UpdateAsync(taskId, updated, ct);
            }
        }

        await engine.CommitAsync(ct);

        _logger.LogDebug("Completed {Count} embedding tasks", taskIds.Count);
    }

    public Task<EmbeddingQueueStats> GetStatsAsync(CancellationToken ct = default)
    {
        var engine = _registry.SystemEngine;
        var col = engine.GetOrCreateCollection(QueueCollectionName);
        var opts = _options.CurrentValue;

        var all = col.FindAll().ToList();
        int todo = 0, inProgress = 0, stale = 0, done = 0;

        foreach (var doc in all)
        {
            var status = doc.TryGetValue("rawStatus", out var sv) ? sv.AsString : "todo";
            var changedAt = doc.TryGetValue("changedAt", out var cv) ? cv.AsDateTime : DateTime.UtcNow;

            if (status == "done")
                done++;
            else if (status == "in_progress" && IsStale(changedAt, opts.StaleTimeoutMinutes))
                stale++;
            else if (status == "in_progress")
                inProgress++;
            else
                todo++;
        }

        return Task.FromResult(new EmbeddingQueueStats(todo, inProgress, stale, done, all.Count));
    }

    private static string BuildKey(string? databaseId, string collection, BsonId documentId)
        => $"{databaseId ?? ""}:{collection}:{documentId}";

    private static BsonDocument UpdateTaskStatus(BsonDocument doc, string newStatus, BLiteEngine engine)
    {
        var keyMap = (ConcurrentDictionary<string, ushort>)engine.GetKeyMap();
        var reverseMap = (ConcurrentDictionary<ushort, string>)engine.GetKeyReverseMap();
        var builder = new BsonDocumentBuilder(keyMap, reverseMap);

        if (doc.TryGetId(out var id))
            builder.AddId(id);

        foreach (var (name, value) in doc.EnumerateFields())
        {
            if (name == "rawStatus")
                builder.AddString(name, newStatus);
            else if (name == "changedAt")
                builder.AddDateTime(name, DateTime.UtcNow);
            else
                builder.Add(name, value);
        }

        return builder.Build();
    }

    private static void AddBsonId(BsonDocumentBuilder b, string name, BsonId id)
    {
        switch (id.Type)
        {
            case BsonIdType.ObjectId: b.AddObjectId(name, id.AsObjectId()); break;
            case BsonIdType.Int32:    b.AddInt32(name, id.AsInt32());       break;
            case BsonIdType.Int64:    b.AddInt64(name, id.AsInt64());       break;
            case BsonIdType.Guid:     b.AddGuid(name, id.AsGuid());         break;
            default:                  b.AddString(name, id.ToString());      break;
        }
    }

    private static bool IsStale(DateTime changedAt, int staleThresholdMinutes)
        => (DateTime.UtcNow - changedAt).TotalMinutes > staleThresholdMinutes;
}
