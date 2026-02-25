using System.Collections.Concurrent;
using BLite.Core;
using BLite.Server.Auth;
using Grpc.Core;

namespace BLite.Server.Transactions;

/// <summary>
/// Represents a single server-managed transaction that is active
/// for a specific <see cref="BLiteUser"/>.
/// </summary>
public sealed class TransactionSession
{
    public string      TxnId      { get; }
    public BLiteUser   Owner      { get; }
    /// <summary>The database (engine) this transaction is running against.</summary>
    public string      DatabaseId { get; }  // canonical key: empty string = default
    /// <summary>The resolved engine for this transaction.</summary>
    public BLiteEngine Engine     { get; }

    private DateTime _lastActivity;
    private readonly int _timeoutSeconds;
    private readonly ConcurrentBag<string> _dirtyCollections = [];

    public TransactionSession(
        string txnId, BLiteUser owner, int timeoutSeconds,
        string databaseId, BLiteEngine engine)
    {
        TxnId           = txnId;
        Owner           = owner;
        DatabaseId      = databaseId;
        Engine          = engine;
        _timeoutSeconds = timeoutSeconds;
        _lastActivity   = DateTime.UtcNow;
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
