// BLite.Server — EmbeddingWorkerOptions
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

namespace BLite.Server.Embedding;

/// <summary>Configuration options for the embedding worker and queue.</summary>
public sealed class EmbeddingWorkerOptions
{
    public const string Section = "EmbeddingWorker";

    /// <summary>Default stale timeout used when options are not yet injected (e.g. in static contexts).</summary>
    public const int DefaultStaleTimeoutMinutes = 5;

    /// <summary>Enables or disables the embedding worker completely.</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>Seconds between worker cycles (default 60).</summary>
    public int IntervalSeconds { get; set; } = 60;

    /// <summary>
    /// Maximum number of tasks to process per cycle per database.
    /// Balances latency vs throughput (default 50).
    /// </summary>
    public int BatchSize { get; set; } = 50;

    /// <summary>
    /// Minutes before an in_progress task transitions to stale and can be retried (default 5).
    /// </summary>
    public int StaleTimeoutMinutes { get; set; } = DefaultStaleTimeoutMinutes;

    /// <summary>
    /// Hours before done tasks are deleted from the queue (default 24).
    /// </summary>
    public int RetentionHours { get; set; } = 24;
}
