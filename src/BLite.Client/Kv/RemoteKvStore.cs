// BLite.Client — RemoteKvStore
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

using BLite.Proto.V1;
using Google.Protobuf;
using Grpc.Core;

namespace BLite.Client.Kv;

/// <summary>
/// Wraps the <c>KvService</c> gRPC stub, exposing all Key-Value store operations.
/// Obtain via <see cref="BLiteClient.Kv"/>.
/// </summary>
public sealed class RemoteKvStore
{
    private readonly KvService.KvServiceClient _stub;
    private readonly Metadata _headers;

    internal RemoteKvStore(KvService.KvServiceClient stub, Metadata headers)
    {
        _stub    = stub;
        _headers = headers;
    }

    // ── Read ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// Returns the raw value for <paramref name="key"/>, or <c>null</c> when
    /// the key does not exist or has expired.
    /// </summary>
    public async Task<byte[]?> GetAsync(string key, CancellationToken ct = default)
    {
        var response = await _stub.GetAsync(
            new KvGetRequest { Key = key }, _headers, cancellationToken: ct);

        ThrowIfError(response.Error, nameof(GetAsync));
        return response.Found ? response.Value.ToByteArray() : null;
    }

    /// <summary>Returns <c>true</c> when <paramref name="key"/> exists and has not expired.</summary>
    public async Task<bool> ExistsAsync(string key, CancellationToken ct = default)
    {
        var response = await _stub.ExistsAsync(
            new KvKeyRequest { Key = key }, _headers, cancellationToken: ct);

        ThrowIfError(response.Error, nameof(ExistsAsync));
        return response.Exists;
    }

    /// <summary>
    /// Returns all keys visible to the current user, optionally filtered by
    /// <paramref name="prefix"/>.  An empty prefix returns all keys.
    /// </summary>
    public async Task<IReadOnlyList<string>> ScanKeysAsync(
        string prefix = "", CancellationToken ct = default)
    {
        var response = await _stub.ScanKeysAsync(
            new KvScanRequest { Prefix = prefix }, _headers, cancellationToken: ct);

        ThrowIfError(response.Error, nameof(ScanKeysAsync));
        return [.. response.Keys];
    }

    // ── Write ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Stores <paramref name="value"/> under <paramref name="key"/>.
    /// Pass a <paramref name="ttl"/> to make the entry expire automatically;
    /// omit it (or pass <c>null</c>) for a persistent entry.
    /// </summary>
    public async Task SetAsync(
        string key, byte[] value, TimeSpan? ttl = null, CancellationToken ct = default)
    {
        var response = await _stub.SetAsync(new KvSetRequest
        {
            Key   = key,
            Value = ByteString.CopyFrom(value),
            TtlMs = ttl.HasValue ? (long)ttl.Value.TotalMilliseconds : 0L
        }, _headers, cancellationToken: ct);

        ThrowIfError(response.Error, nameof(SetAsync));
    }

    /// <summary>
    /// Deletes <paramref name="key"/>.
    /// Returns <c>true</c> when the key existed, <c>false</c> when it was
    /// already absent.
    /// </summary>
    public async Task<bool> DeleteAsync(string key, CancellationToken ct = default)
    {
        var response = await _stub.DeleteAsync(
            new KvDeleteRequest { Key = key }, _headers, cancellationToken: ct);

        ThrowIfError(response.Error, nameof(DeleteAsync));
        return response.Success;
    }

    /// <summary>
    /// Resets the TTL on an existing key to <paramref name="ttl"/>.
    /// Returns <c>true</c> when the key was found and updated, <c>false</c>
    /// when the key did not exist or had already expired.
    /// </summary>
    public async Task<bool> RefreshAsync(
        string key, TimeSpan ttl, CancellationToken ct = default)
    {
        var response = await _stub.RefreshAsync(new KvRefreshRequest
        {
            Key   = key,
            TtlMs = (long)ttl.TotalMilliseconds
        }, _headers, cancellationToken: ct);

        ThrowIfError(response.Error, nameof(RefreshAsync));
        return response.Success;
    }

    // ── Batch ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Executes multiple set/delete operations in a single round-trip.
    /// Use the <paramref name="configure"/> delegate to add operations to the
    /// batch, then call this method to dispatch them atomically.
    /// </summary>
    /// <returns>Number of keys affected (created, updated, or deleted).</returns>
    public async Task<int> BatchAsync(
        Action<RemoteKvBatch> configure, CancellationToken ct = default)
    {
        var batch = new RemoteKvBatch();
        configure(batch);

        var request = new KvBatchRequest();
        request.Operations.AddRange(batch.BuildOperations());

        var response = await _stub.BatchAsync(request, _headers, cancellationToken: ct);
        ThrowIfError(response.Error, nameof(BatchAsync));
        return response.AffectedCount;
    }

    // ── Admin ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Removes all expired entries from the store immediately, without waiting
    /// for the next lazy-expiry check.
    /// Requires <c>Admin</c> permission.
    /// </summary>
    /// <returns>Number of entries removed.</returns>
    public async Task<int> PurgeExpiredAsync(CancellationToken ct = default)
    {
        var response = await _stub.PurgeExpiredAsync(
            new KvDbRequest(), _headers, cancellationToken: ct);

        ThrowIfError(response.Error, nameof(PurgeExpiredAsync));
        return response.PurgedCount;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static void ThrowIfError(string error, string method)
    {
        if (!string.IsNullOrEmpty(error))
            throw new InvalidOperationException($"{method} failed: {error}");
    }
}

/// <summary>
/// Fluent builder used with <see cref="RemoteKvStore.BatchAsync"/> to
/// accumulate set and delete operations before dispatch.
/// </summary>
public sealed class RemoteKvBatch
{
    private readonly List<KvBatchOp> _ops = [];

    /// <summary>Adds a set operation (create or overwrite) to the batch.</summary>
    public RemoteKvBatch Set(string key, byte[] value, TimeSpan? ttl = null)
    {
        _ops.Add(new KvBatchOp
        {
            Key      = key,
            Value    = ByteString.CopyFrom(value),
            TtlMs    = ttl.HasValue ? (long)ttl.Value.TotalMilliseconds : 0L,
            IsDelete = false
        });
        return this;
    }

    /// <summary>Adds a delete operation to the batch.</summary>
    public RemoteKvBatch Delete(string key)
    {
        _ops.Add(new KvBatchOp { Key = key, IsDelete = true });
        return this;
    }

    internal IEnumerable<KvBatchOp> BuildOperations() => _ops;
}
