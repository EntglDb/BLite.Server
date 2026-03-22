using System.Collections.Concurrent;
using BLite.Core;
using BLite.Server.Auth;
using Grpc.Core;

namespace BLite.Server.Transactions;

/// <summary>
/// Represents a single server-managed transaction that is active for a specific
/// <see cref="BLiteUser"/>.
///
/// <para>
/// In BLite 3.8+ each active transaction is backed by a dedicated
/// <see cref="BLiteSession"/> so concurrent transactions on the same database
/// are fully isolated from one another without the need for a server-side semaphore.
/// The session is disposed (auto-rolling back any uncommitted changes) when the
/// <see cref="TransactionSession"/> is no longer needed.
/// </para>
/// </summary>
public sealed class TransactionSession : IDisposable
{
    public string      TxnId      { get; }
    public BLiteUser   Owner      { get; }
    /// <summary>The database key this transaction is running against (empty = default).</summary>
    public string      DatabaseId { get; }
    /// <summary>The isolated session that owns this transaction context.</summary>
    public BLiteSession Session   { get; }

    private DateTime _lastActivity;
    private readonly int _timeoutSeconds;
    private readonly ConcurrentBag<string> _dirtyCollections = [];
    private bool _disposed;

    public TransactionSession(
        string txnId, BLiteUser owner, int timeoutSeconds,
        string databaseId, BLiteSession session)
    {
        TxnId           = txnId;
        Owner           = owner;
        DatabaseId      = databaseId;
        Session         = session;
        _timeoutSeconds = timeoutSeconds;
        _lastActivity   = DateTime.UtcNow;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        // Disposing the BLiteSession auto-rolls back any uncommitted transaction.
        Session.Dispose();
    }

    /// <summary>Returns true when the session has not been used within the configured timeout.</summary>
    public bool IsExpired => (DateTime.UtcNow - _lastActivity).TotalSeconds > _timeoutSeconds;

    /// <summary>Refreshes the idle-timeout timer.</summary>
    public void Touch() => _lastActivity = DateTime.UtcNow;

    /// <summary>Marks a collection as written during this transaction for cache invalidation.</summary>
    public void MarkDirty(string physicalCollection)
        => _dirtyCollections.Add(physicalCollection);

    /// <summary>Returns the set of collections written during this transaction.</summary>
    public IReadOnlyCollection<string> DirtyCollections
        => _dirtyCollections.Distinct().ToList();

    /// <summary>
    /// Validates that <paramref name="caller"/> owns this session; throws
    /// <see cref="RpcException"/> with <see cref="StatusCode.PermissionDenied"/> otherwise.
    /// </summary>
    public void RequireOwner(BLiteUser caller)
    {
        if (!Owner.Username.Equals(caller.Username, StringComparison.OrdinalIgnoreCase))
            throw new RpcException(new Status(
                StatusCode.PermissionDenied,
                $"Transaction '{TxnId}' belongs to a different user."));
    }
}
