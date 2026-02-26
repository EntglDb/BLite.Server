// BLite.Server — EmbeddingTaskStatus
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

namespace BLite.Server.Embedding;

/// <summary>
/// Status of an embedding task in the queue.
/// <c>Stale</c> is not persisted — it's computed based on elapsed time since <c>ChangedAt</c>.
/// </summary>
public enum EmbeddingTaskStatus
{
    /// <summary>Waiting to be processed.</summary>
    Todo,

    /// <summary>Currently being processed (embedding in progress).</summary>
    InProgress,

    /// <summary>In progress but past the timeout threshold — will be retried.</summary>
    Stale,

    /// <summary>Successfully embedded and persisted.</summary>
    Done
}
