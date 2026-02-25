using System.Collections.Concurrent;
using BLite.Core;
using BLite.Server.Auth;
using BLite.Server.Caching;
using BLite.Server.Telemetry;
using Grpc.Core;
using Microsoft.Extensions.Options;

namespace BLite.Server.Transactions;

/// <summary>
/// Options read from <c>appsettings.json → "Transactions"</c>.
/// </summary>
public sealed class TransactionOptions
{
    /// <summary>Seconds before an idle transaction is automatically rolled back. Default 60.</summary>
    public int TimeoutSeconds { get; set; } = 60;
}

/// <summary>
/// Manages explicit, token-scoped transactions across one or more
/// <see cref="BLiteEngine"/> instances retrieved from <see cref="EngineRegistry"/>.
///
/// <para>
/// Because <see cref="BLiteEngine"/> serialises transactions internally with
/// a <c>SemaphoreSlim(1,1)</c> there can only be <b>one active transaction
/// per database</b> at a time.  <see cref="TransactionManager"/> enforces that
/// invariant at the gRPC boundary with a per-database semaphore.
/// </para>
///
/// <para>
/// The per-database semaphore is acquired on <see cref="BeginAsync"/> and
/// released only by <see cref="CommitAsync"/>, <see cref="RollbackAsync"/>
/// or <see cref="CleanupExpiredAsync"/>.
/// </para>
/// </summary>
public sealed class TransactionManager : IAsyncDisposable
{
    private readonly EngineRegistry              _registry;
    private readonly int                         _timeoutSeconds;
    private readonly ILogger<TransactionManager> _logger;
    private readonly QueryCacheService          _cache;

    // Per-database lock: canonical database-id → SemaphoreSlim(1,1)
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _locks
        = new(StringComparer.OrdinalIgnoreCase);

    // Active sessions keyed by token (UUID string).
    private readonly ConcurrentDictionary<string, TransactionSession> _sessions = new();

    public TransactionManager(
        EngineRegistry                   registry,
        IOptions<TransactionOptions>     opts,
        ILogger<TransactionManager>      logger,
        QueryCacheService                cache)
    {
        _registry       = registry;
        _timeoutSeconds = opts.Value.TimeoutSeconds;
        _logger         = logger;
        _cache          = cache;
    }

    // Returns (or lazily creates) the per-database semaphore.
    private SemaphoreSlim GetLock(string databaseId)
        => _locks.GetOrAdd(databaseId, _ => new SemaphoreSlim(1, 1));

    // ── Public API ────────────────────────────────────────────────────────────

    /// <summary>
    /// Begins a new transaction on behalf of <paramref name="owner"/>.
    /// Blocks until any currently active transaction on the same database has been released.
    /// </summary>
    /// <returns>The opaque <c>txn_id</c> to pass in subsequent write RPCs.</returns>
    public async Task<string> BeginAsync(BLiteUser owner, CancellationToken ct)
    {
        // Resolve engine + canonical DB key for the semaphore
        var dbId   = CanonicalDbId(owner.DatabaseId);
        var engine = _registry.GetEngine(owner.DatabaseId);
        var dbLock = GetLock(dbId);

        // Acquire the per-database lock — NOT released here; released by Commit/Rollback/Cleanup.
        await dbLock.WaitAsync(ct);

        try
        {
            await engine.BeginTransactionAsync(ct);

            var txnId   = Guid.NewGuid().ToString("N");
            var session = new TransactionSession(txnId, owner, _timeoutSeconds, dbId, engine);
            _sessions[txnId] = session;

            BLiteMetrics.ActiveTransactions.Add(1);
            _logger.LogInformation(
                "Transaction {TxnId} started for user '{User}' on database '{Db}'.",
                txnId, owner.Username, dbId == "" ? "(default)" : dbId);

            return txnId;
        }
        catch
        {
            // If engine.Begin throws, release the lock so the server doesn't deadlock.
            dbLock.Release();
            throw;
        }
    }

    /// <summary>
    /// Commits the transaction identified by <paramref name="txnId"/>.
    /// Only the owning user may commit.
    /// </summary>
    public async Task CommitAsync(string txnId, BLiteUser caller, CancellationToken ct)
    {
        var session = RemoveSession(txnId, caller);
        try
        {
            await session.Engine.CommitAsync(ct);

            // Invalidate cache for every collection written in this transaction
            if (_cache.Enabled)
                foreach (var col in session.DirtyCollections)
                    _cache.Invalidate(session.DatabaseId, col);

            _logger.LogInformation("Transaction {TxnId} committed by '{User}'.", txnId, caller.Username);
        }
        finally
        {
            GetLock(session.DatabaseId).Release();
        }
    }

    /// <summary>
    /// Rolls back the transaction identified by <paramref name="txnId"/>.
    /// Only the owning user may roll back.
    /// </summary>
    public Task RollbackAsync(string txnId, BLiteUser caller, CancellationToken ct)
    {
        var session = RemoveSession(txnId, caller);
        return RollbackCoreAsync(session);
    }

    /// <summary>
    /// Validates that <paramref name="txnId"/> names an active session owned by
    /// <paramref name="caller"/>, refreshes the idle timer, and returns the session.
    /// Throws <see cref="RpcException"/> when the token is unknown, expired, or belongs
    /// to a different user.
    /// </summary>
    public TransactionSession RequireSession(string txnId, BLiteUser caller)
    {
        if (!_sessions.TryGetValue(txnId, out var session))
            throw new RpcException(new Status(
                StatusCode.NotFound,
                $"Transaction '{txnId}' not found. It may have expired or never existed."));

        if (session.IsExpired)
        {
            // Expire it here; cleanup service will also catch stragglers asynchronously.
            _ = CleanupSessionAsync(txnId);
            throw new RpcException(new Status(
                StatusCode.DeadlineExceeded,
                $"Transaction '{txnId}' has expired."));
        }

        session.RequireOwner(caller);
        session.Touch();
        return session;
    }

    /// <summary>
    /// Returns true if there is an active transaction on the given database.
    /// </summary>
    public bool HasActiveTransaction(string? dbId)
    {
        var key = CanonicalDbId(dbId);
        return _sessions.Values.Any(s => s.DatabaseId == key);
    }

    /// <summary>
    /// Called by <see cref="TransactionCleanupService"/> every 10 seconds.
    /// Rolls back any session that has exceeded its idle timeout.
    /// </summary>
    public async Task CleanupExpiredAsync(CancellationToken ct)
    {
        foreach (var (txnId, session) in _sessions)
        {
            if (!session.IsExpired) continue;

            _logger.LogWarning(
                "Transaction {TxnId} owned by '{User}' expired — rolling back.",
                txnId, session.Owner.Username);

            await CleanupSessionAsync(txnId);
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var txnId in _sessions.Keys.ToList())
            await CleanupSessionAsync(txnId);

        foreach (var sem in _locks.Values)
            sem.Dispose();

        _locks.Clear();
    }

    // ── Internals ─────────────────────────────────────────────────────────────

    private TransactionSession RemoveSession(string txnId, BLiteUser caller)
    {
        if (!_sessions.TryRemove(txnId, out var session))
            throw new RpcException(new Status(
                StatusCode.NotFound,
                $"Transaction '{txnId}' not found."));

        session.RequireOwner(caller);
        BLiteMetrics.ActiveTransactions.Add(-1);
        return session;
    }

    private async Task CleanupSessionAsync(string txnId)
    {
        if (!_sessions.TryRemove(txnId, out var session)) return;

        BLiteMetrics.ActiveTransactions.Add(-1);
        await RollbackCoreAsync(session);
    }

    private Task RollbackCoreAsync(TransactionSession session)
    {
        try
        {
            session.Engine.Rollback();
            _logger.LogInformation("Transaction {TxnId} rolled back.", session.TxnId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error rolling back transaction {TxnId}.", session.TxnId);
        }
        finally
        {
            GetLock(session.DatabaseId).Release();
        }
        return Task.CompletedTask;
    }

    /// <summary>
    /// Normalises a database-id the same way <see cref="EngineRegistry"/> does so the
    /// per-database semaphore key always matches the session key stored in the registry.
    /// </summary>
    private static string CanonicalDbId(string? id)
        => string.IsNullOrWhiteSpace(id) ? "" : id.Trim().ToLowerInvariant();
}
