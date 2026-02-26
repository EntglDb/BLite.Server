// BLite.Server — IEmbeddingQueue
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

using BLite.Bson;

namespace BLite.Server.Embedding;

/// <summary>
/// Persistent queue for embedding tasks, backed by the system database.
/// </summary>
public interface IEmbeddingQueue
{
    /// <summary>
    /// Enqueues a task for the given document, deduplicating if the same document
    /// is already in the queue with status "todo" or "in_progress".
    /// </summary>
    Task EnqueueAsync(string? databaseId, string collection, BsonId documentId, CancellationToken ct = default);

    /// <summary>
    /// Retrieves up to <paramref name="batchSize"/> tasks with status "todo" or "stale",
    /// ordered by EnqueuedAt ascending, and marks them as "in_progress".
    /// </summary>
    Task<IReadOnlyList<EmbeddingTask>> TakeBatchAsync(int batchSize, CancellationToken ct = default);

    /// <summary>
    /// Marks the given task IDs as "done" with updated ChangedAt timestamp.
    /// </summary>
    Task CompleteAsync(IReadOnlyList<BsonId> taskIds, CancellationToken ct = default);

    /// <summary>
    /// Returns statistics about the queue.
    /// </summary>
    Task<EmbeddingQueueStats> GetStatsAsync(CancellationToken ct = default);
}

/// <summary>Statistics about the embedding queue.</summary>
public sealed record EmbeddingQueueStats(
    int TodoCount,
    int InProgressCount,
    int StaleCount,
    int DoneCount,
    int TotalCount);
