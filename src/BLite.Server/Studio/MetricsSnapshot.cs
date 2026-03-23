// BLite.Server — Server metrics snapshot records
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

namespace BLite.Server.Studio;

/// <summary>
/// A point-in-time snapshot of all observable server metrics, collected every
/// <see cref="ServerMetricsCollector.SampleIntervalSeconds"/> seconds.
/// </summary>
public sealed record MetricsSnapshot(
    DateTimeOffset Timestamp,
    // ── Process ──────────────────────────────────────────────────────────────
    double CpuPercent,
    long WorkingSetBytes,
    long PrivateMemoryBytes,
    int ThreadCount,
    // ── GC / managed runtime ─────────────────────────────────────────────────
    long ManagedHeapBytes,
    long GcGen0,
    long GcGen1,
    long GcGen2,
    long GcFragmentedBytes,
    // ── Traffic ───────────────────────────────────────────────────────────────
    long RpcTotalOps,
    double RequestsPerSecond,
    // ── Query cache ───────────────────────────────────────────────────────────
    long CacheHits,
    long CacheMisses,
    double CacheHitRatio,
    // ── Transactions ──────────────────────────────────────────────────────────
    int ActiveTransactions,
    IReadOnlyList<ActiveTxInfo> ActiveTxList,
    // ── Storage ───────────────────────────────────────────────────────────────
    IReadOnlyList<DatabaseStorageInfo> Databases);

/// <summary>A single active transaction surfaced in the dashboard.</summary>
public sealed record ActiveTxInfo(
    string TxnId,
    string Username,
    string DatabaseId,
    TimeSpan Age);

/// <summary>File-level storage information for one database (system or tenant).</summary>
public sealed record DatabaseStorageInfo(
    string Label,
    long FileSizeBytes,
    long WalSizeBytes);
