// BLite.Server — Server metrics collector (background service)
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Samples process, GC, traffic, cache, transaction and storage metrics every
// SampleIntervalSeconds seconds.  Keeps an in-memory circular buffer of the
// last RingCapacity snapshots and persists each snapshot to the system KV store
// with a 2-hour TTL so the history survives a process restart.

using System.Diagnostics;
using System.Text.Json;
using BLite.Server.Caching;
using BLite.Server.Telemetry;
using BLite.Server.Transactions;

namespace BLite.Server.Studio;

/// <summary>
/// Singleton background service that continuously samples server observability data
/// and makes it available to the Studio dashboard.
/// </summary>
public sealed class ServerMetricsCollector : BackgroundService
{
    public const int SampleIntervalSeconds = 10;

    // 72 slots = 12 minutes of history at the default 10 s interval.
    private const int RingCapacity = 72;
    private const string KvKeyPrefix = "_server:metrics:";
    private static readonly TimeSpan KvTtl = TimeSpan.FromHours(2);

    private readonly EngineRegistry _registry;
    private readonly QueryCacheService _cache;
    private readonly TransactionManager _txnManager;
    private readonly ILogger<ServerMetricsCollector> _logger;

    // Circular buffer — protected by _lock.
    private readonly MetricsSnapshot?[] _ring = new MetricsSnapshot[RingCapacity];
    private int _head;
    private int _count;
    private readonly object _lock = new();

    // CPU% sampling state.
    private readonly Process _process = Process.GetCurrentProcess();
    private TimeSpan _prevCpuTime;
    private DateTimeOffset _prevSampleAt;

    // RPS sampling state.
    private long _prevRpcCount;
    private DateTimeOffset _prevRpcSampleAt;

    public ServerMetricsCollector(
        EngineRegistry registry,
        QueryCacheService cache,
        TransactionManager txnManager,
        ILogger<ServerMetricsCollector> logger)
    {
        _registry   = registry;
        _cache      = cache;
        _txnManager = txnManager;
        _logger     = logger;
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /// <summary>Returns the most recently collected snapshot, or <c>null</c> if none yet.</summary>
    public MetricsSnapshot? GetLatestSnapshot()
    {
        lock (_lock)
        {
            if (_count == 0) return null;
            var idx = (_head - 1 + RingCapacity) % RingCapacity;
            return _ring[idx];
        }
    }

    /// <summary>
    /// Returns up to <paramref name="count"/> snapshots in ascending chronological order.
    /// </summary>
    public IReadOnlyList<MetricsSnapshot> GetHistory(int count = RingCapacity)
    {
        lock (_lock)
        {
            if (_count == 0) return [];
            var n      = Math.Min(count, _count);
            var result = new MetricsSnapshot[n];
            for (int i = 0; i < n; i++)
            {
                var idx    = (_head - n + i + RingCapacity * 2) % RingCapacity;
                result[i]  = _ring[idx]!;
            }
            return result;
        }
    }

    // ── BackgroundService ─────────────────────────────────────────────────────

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        await HydrateFromKvAsync(ct);

        // Initialise baselines for derivative metrics.
        _process.Refresh();
        _prevCpuTime    = _process.TotalProcessorTime;
        _prevSampleAt   = DateTimeOffset.UtcNow;
        _prevRpcCount   = BLiteMetrics.ReadRpcTotal();
        _prevRpcSampleAt = DateTimeOffset.UtcNow;

        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(SampleIntervalSeconds));
        while (await timer.WaitForNextTickAsync(ct))
        {
            try
            {
                var snapshot = CollectSnapshot();
                Enqueue(snapshot);
                await PersistToKvAsync(snapshot);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogWarning(ex, "Metrics collection tick failed.");
            }
        }
    }

    // ── Sampling ──────────────────────────────────────────────────────────────

    private MetricsSnapshot CollectSnapshot()
    {
        _process.Refresh();
        var now = DateTimeOffset.UtcNow;

        // ── CPU % ─────────────────────────────────────────────────────────────
        var cpuNow      = _process.TotalProcessorTime;
        var wallMs      = (now - _prevSampleAt).TotalMilliseconds;
        var cpuDeltaMs  = (cpuNow - _prevCpuTime).TotalMilliseconds;
        var cpuPct      = wallMs > 0
            ? Math.Min(100.0, cpuDeltaMs / (wallMs * Environment.ProcessorCount) * 100.0)
            : 0.0;
        _prevCpuTime  = cpuNow;
        _prevSampleAt = now;

        // ── Requests/sec ──────────────────────────────────────────────────────
        var rpcNow    = BLiteMetrics.ReadRpcTotal();
        var rpcWallS  = (now - _prevRpcSampleAt).TotalSeconds;
        var rps       = rpcWallS > 0 ? (rpcNow - _prevRpcCount) / rpcWallS : 0.0;
        _prevRpcCount    = rpcNow;
        _prevRpcSampleAt = now;

        // ── GC ────────────────────────────────────────────────────────────────
        var gcInfo = GC.GetGCMemoryInfo();

        // ── Cache ─────────────────────────────────────────────────────────────
        var hits   = _cache.CacheHits;
        var misses = _cache.CacheMisses;
        var total  = hits + misses;
        var hitRatio = total > 0 ? Math.Round((double)hits / total * 100.0, 1) : 0.0;

        // ── Transactions ──────────────────────────────────────────────────────
        var activeTxList = _txnManager.GetActiveSessionsSnapshot()
            .Select(s => new ActiveTxInfo(s.TxnId, s.Username, s.DatabaseId, now - s.StartedAt))
            .ToList();

        return new MetricsSnapshot(
            Timestamp:          now,
            CpuPercent:         Math.Round(cpuPct, 1),
            WorkingSetBytes:    _process.WorkingSet64,
            PrivateMemoryBytes: _process.PrivateMemorySize64,
            ThreadCount:        _process.Threads.Count,
            ManagedHeapBytes:   GC.GetTotalMemory(false),
            GcGen0:             GC.CollectionCount(0),
            GcGen1:             GC.CollectionCount(1),
            GcGen2:             GC.CollectionCount(2),
            GcFragmentedBytes:  gcInfo.FragmentedBytes,
            RpcTotalOps:        rpcNow,
            RequestsPerSecond:  Math.Round(rps, 2),
            CacheHits:          hits,
            CacheMisses:        misses,
            CacheHitRatio:      hitRatio,
            ActiveTransactions: activeTxList.Count,
            ActiveTxList:       activeTxList,
            Databases:          BuildStorageInfo());
    }

    private IReadOnlyList<DatabaseStorageInfo> BuildStorageInfo()
    {
        var result = new List<DatabaseStorageInfo>();

        var sysPath = _registry.SystemDatabasePath;
        result.Add(new DatabaseStorageInfo("(system)", FileSize(sysPath), WalSize(sysPath)));

        foreach (var tenant in _registry.ListTenants())
        {
            var dbPath = _registry.GetDatabasePath(tenant.DatabaseId);
            result.Add(new DatabaseStorageInfo(tenant.DatabaseId, FileSize(dbPath), WalSize(dbPath)));
        }

        return result;
    }

    // ── Circular buffer ───────────────────────────────────────────────────────

    private void Enqueue(MetricsSnapshot snapshot)
    {
        lock (_lock)
        {
            _ring[_head] = snapshot;
            _head = (_head + 1) % RingCapacity;
            if (_count < RingCapacity) _count++;
        }
    }

    // ── KV persistence ────────────────────────────────────────────────────────

    private Task PersistToKvAsync(MetricsSnapshot snapshot)
    {
        try
        {
            var key  = $"{KvKeyPrefix}{snapshot.Timestamp.Ticks:D20}";
            var json = JsonSerializer.SerializeToUtf8Bytes(snapshot);
            _registry.SystemEngine.KvStore.Set(key, json, KvTtl);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Metrics KV persist failed.");
        }
        return Task.CompletedTask;
    }

    private async Task HydrateFromKvAsync(CancellationToken ct)
    {
        try
        {
            var kv   = _registry.SystemEngine.KvStore;
            var keys = kv.ScanKeys(KvKeyPrefix).OrderBy(k => k).TakeLast(RingCapacity).ToList();
            foreach (var key in keys)
            {
                ct.ThrowIfCancellationRequested();
                var bytes = kv.Get(key);    // lazy-expiry check built into Get
                if (bytes is null) continue;
                try
                {
                    var snap = JsonSerializer.Deserialize<MetricsSnapshot>(bytes);
                    if (snap is not null) Enqueue(snap);
                }
                catch { /* skip corrupt entry */ }
            }
            _logger.LogDebug("Metrics history hydrated: {Count} snapshots loaded.", _count);
        }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Metrics KV hydrate failed.");
        }
        await Task.CompletedTask;
    }

    // ── File-size helpers ─────────────────────────────────────────────────────

    private static long FileSize(string path)
        => File.Exists(path) ? new FileInfo(path).Length : 0L;

    // WAL location follows PageFileConfig.Server(): {dir}/wal/{name}.wal
    // Fall back to the simple {name}.wal sibling for non-Server configs.
    private static long WalSize(string dbPath)
    {
        var dir  = Path.GetDirectoryName(dbPath) ?? ".";
        var name = Path.GetFileNameWithoutExtension(dbPath);
        var serverWal = Path.Combine(dir, "wal", name + ".wal");
        if (File.Exists(serverWal)) return new FileInfo(serverWal).Length;
        var simpleWal = Path.ChangeExtension(dbPath, ".wal");
        return File.Exists(simpleWal) ? new FileInfo(simpleWal).Length : 0L;
    }
}
