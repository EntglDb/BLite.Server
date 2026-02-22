using System.Collections.Concurrent;
using BLite;
using BLite.Core;
using BLite.Server.Auth;
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
/// Manages explicit, token-scoped transactions on top of <see cref="BLiteEngine"/>.
///
/// <para>
/// Because <see cref="BLiteEngine"/> serialises transactions internally with
/// a <c>SemaphoreSlim(1,1)</c> there can only be <b>one active transaction</b>
/// at a time.  <see cref="TransactionManager"/> enforces that invariant at the
/// gRPC boundary: <see cref="BeginAsync"/> blocks until any current transaction
/// is committed or rolled back.
/// </para>
///
/// <para>
/// The semaphore is acquired on <see cref="BeginAsync"/> and released only by
/// <see cref="CommitAsync"/>, <see cref="RollbackAsync"/> or
/// <see cref="CleanupExpiredAsync"/>.
/// </para>
/// </summary>
public sealed class TransactionManager : IAsyncDisposable
{
    private readonly BLiteEngine             _engine;
    private readonly int                     _timeoutSeconds;
    private readonly ILogger<TransactionManager> _logger;

    // The global lock that serialises begin/commit/rollback across callers.
    private readonly SemaphoreSlim _lock = new(1, 1);

    // Active sessions keyed by token (UUID string).
    private readonly ConcurrentDictionary<string, TransactionSession> _sessions = new();

    public TransactionManager(
        BLiteEngine                      engine,
        IOptions<TransactionOptions>     opts,
        ILogger<TransactionManager>      logger)
    {
        _engine         = engine;
        _timeoutSeconds = opts.Value.TimeoutSeconds;
        _logger         = logger;
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /// <summary>
    /// Begins a new transaction on behalf of <paramref name="owner"/>.
    /// Blocks until any currently active transaction has been released.
    /// </summary>
    /// <returns>The opaque <c>txn_id</c> to pass in subsequent write RPCs.</returns>
    public async Task<string> BeginAsync(BLiteUser owner, CancellationToken ct)
    {
        // Acquire the global lock — NOT released here; released by Commit/Rollback/Cleanup.
        await _lock.WaitAsync(ct);

        try
        {
            await _engine.BeginTransactionAsync(ct);

            var txnId   = Guid.NewGuid().ToString("N");
            var session = new TransactionSession(txnId, owner, _timeoutSeconds);
            _sessions[txnId] = session;

            BLiteMetrics.ActiveTransactions.Add(1);
            _logger.LogInformation("Transaction {TxnId} started for user '{User}'.", txnId, owner.Username);

            return txnId;
        }
        catch
        {
            // If engine.Begin throws, release the lock so the server doesn't deadlock.
            _lock.Release();
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
            await _engine.CommitAsync(ct);
            _logger.LogInformation("Transaction {TxnId} committed by '{User}'.", txnId, caller.Username);
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Rolls back the transaction identified by <paramref name="txnId"/>.
    /// Only the owning user may roll back.
    /// </summary>
    public Task RollbackAsync(string txnId, BLiteUser caller, CancellationToken ct)
    {
        var session = RemoveSession(txnId, caller);
        return RollbackCoreAsync(txnId);
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
        foreach (var txnId in _sessions.Keys)
            await CleanupSessionAsync(txnId);

        _lock.Dispose();
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
        if (!_sessions.TryRemove(txnId, out _)) return;

        BLiteMetrics.ActiveTransactions.Add(-1);
        await RollbackCoreAsync(txnId);
    }

    private Task RollbackCoreAsync(string txnId)
    {
        try
        {
            _engine.Rollback();
            _logger.LogInformation("Transaction {TxnId} rolled back.", txnId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error rolling back transaction {TxnId}.", txnId);
        }
        finally
        {
            _lock.Release();
        }
        return Task.CompletedTask;
    }
}
