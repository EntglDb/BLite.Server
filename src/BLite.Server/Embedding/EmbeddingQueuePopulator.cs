// BLite.Server — EmbeddingQueuePopulator
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

using System.Threading.Channels;
using BLite.Bson;
using BLite.Core;
using BLite.Core.Indexing;
using BLite.Core.Transactions;

namespace BLite.Server.Embedding;

/// <summary>
/// Hosted service that subscribes to CDC (Change Data Capture) events and
/// enqueues embedding tasks for documents that are inserted or updated.
/// </summary>
public sealed class EmbeddingQueuePopulator : IHostedService
{
    private readonly IEmbeddingQueue _queue;
    private readonly EngineRegistry _registry;
    private readonly ILogger<EmbeddingQueuePopulator> _logger;
    private readonly CancellationTokenSource _cts = new();
    private readonly Dictionary<string, (IDisposable Subscription, Task PumpTask)> _subscriptions = new();

    public EmbeddingQueuePopulator(
        IEmbeddingQueue queue,
        EngineRegistry registry,
        ILogger<EmbeddingQueuePopulator> logger)
    {
        _queue = queue;
        _registry = registry;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("EmbeddingQueuePopulator starting");

        // Subscribe to all existing engines
        foreach (var (dbId, engine) in _registry.GetAllActiveEngines())
        {
            foreach (var colName in engine.ListCollections())
            {
                await SubscribeCollectionIfRelevantAsync(dbId, engine, colName, cancellationToken);
            }
        }

        _logger.LogInformation("EmbeddingQueuePopulator started with {Count} subscriptions", _subscriptions.Count);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("EmbeddingQueuePopulator stopping");
        _cts.Cancel();

        foreach (var (_, (sub, task)) in _subscriptions)
        {
            sub.Dispose();
            try { task.Wait(TimeSpan.FromSeconds(5)); } catch { }
        }

        _cts.Dispose();
        _subscriptions.Clear();

        _logger.LogInformation("EmbeddingQueuePopulator stopped");
        return Task.CompletedTask;
    }

    /// <summary>
    /// Refreshes the subscription for a specific collection.
    /// Call this when VectorSource or VectorIndex configuration changes.
    /// </summary>
    public async Task RefreshSubscriptionAsync(string? databaseId, string collection, CancellationToken ct = default)
    {
        var key = BuildSubscriptionKey(databaseId, collection);

        // Unsubscribe if exists
        if (_subscriptions.Remove(key, out var existing))
        {
            existing.Subscription.Dispose();
            try { await existing.PumpTask; } catch { }
        }

        // Re-subscribe if relevant
        var engine = _registry.GetEngine(databaseId);
        await SubscribeCollectionIfRelevantAsync(databaseId, engine, collection, ct);
    }

    private async Task SubscribeCollectionIfRelevantAsync(
        string? databaseId, BLiteEngine engine, string collection, CancellationToken ct)
    {
        var config = engine.GetVectorSource(collection);
        if (config == null)
            return;

        var indexes = engine.GetIndexDescriptors(collection);
        if (!indexes.Any(d => d.Type == IndexType.Vector))
            return;

        var key = BuildSubscriptionKey(databaseId, collection);
        if (_subscriptions.ContainsKey(key))
            return;

        var channel = Channel.CreateUnbounded<(OperationType Op, BsonId Id)>();
        var subscription = engine.SubscribeToChanges(collection, channel.Writer);

        var pumpTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var (op, id) in channel.Reader.ReadAllAsync(_cts.Token))
                {
                    try
                    {
                        await _queue.EnqueueAsync(databaseId, collection, id, _cts.Token);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to enqueue embedding task for {Db}:{Col}:{DocId}",
                            databaseId ?? "system", collection, id);
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                _logger.LogError(ex, "CDC pump for {Db}:{Col} terminated unexpectedly",
                    databaseId ?? "system", collection);
            }
        }, _cts.Token);

        _subscriptions[key] = (subscription, pumpTask);
        _logger.LogInformation("Subscribed to CDC for {Db}:{Col}", databaseId ?? "system", collection);
    }

    private static string BuildSubscriptionKey(string? databaseId, string collection)
        => $"{databaseId ?? ""}:{collection}";
}
