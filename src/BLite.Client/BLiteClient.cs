// BLite.Client — BLiteClient
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

using BLite.Bson;
using BLite.Client.Admin;
using BLite.Client.Collections;
using BLite.Client.Internal;
using BLite.Client.Transactions;
using BLite.Core.Collections;
using BLite.Proto.V1;
using Grpc.Core;
using Grpc.Net.Client;

namespace BLite.Client;

/// <summary>
/// Entry point for all BLite Server interactions.
///
/// <para>
/// Create one <see cref="BLiteClient"/> per process (or per logical tenant)
/// and reuse it — the underlying <see cref="GrpcChannel"/> is thread-safe and
/// connection-pooled.  Dispose when the application shuts down.
/// </para>
///
/// <code>
/// await using var client = new BLiteClient(new BLiteClientOptions
/// {
///     Host   = "myserver",
///     Port   = 2626,
///     ApiKey = "blt_...",
///     UseTls = true
/// });
///
/// var orders = client.GetDynamicCollection("orders");
/// var users  = client.GetCollection&lt;ObjectId, User&gt;(new UserMapper());
/// var tx     = await client.BeginTransactionAsync();
/// </code>
/// </summary>
public sealed class BLiteClient : IAsyncDisposable
{
    private readonly GrpcChannel                            _channel;
    private readonly Metadata                               _headers;
    private readonly ClientKeyMap                           _keyMap;
    private readonly DynamicService.DynamicServiceClient    _dynStub;
    private readonly DocumentService.DocumentServiceClient  _docStub;
    private readonly AdminService.AdminServiceClient        _adminStub;
    private readonly TransactionService.TransactionServiceClient _txnStub;

    private bool _disposed;
    private readonly bool _ownsChannel;

    // ── Constructor ───────────────────────────────────────────────────────────

    public BLiteClient(BLiteClientOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        _channel     = GrpcChannel.ForAddress(options.ResolvedAddress);
        _ownsChannel = true;

        // API key is sent as a custom header on every gRPC call.
        _headers = new Metadata
        {
            { "x-api-key", options.ApiKey }
        };

        _dynStub   = new DynamicService.DynamicServiceClient(_channel);
        _docStub   = new DocumentService.DocumentServiceClient(_channel);
        _adminStub = new AdminService.AdminServiceClient(_channel);
        _txnStub   = new TransactionService.TransactionServiceClient(_channel);

        var metaStub = new MetadataService.MetadataServiceClient(_channel);
        _keyMap = new ClientKeyMap(metaStub, _headers);
    }

    /// <summary>
    /// For integration tests only — wraps a pre-configured channel without
    /// taking ownership (the channel is managed by the test fixture).
    /// </summary>
    internal BLiteClient(GrpcChannel channel, string apiKey)
    {
        _channel     = channel;
        _ownsChannel = false;

        _headers = new Metadata
        {
            { "x-api-key", apiKey }
        };

        _dynStub   = new DynamicService.DynamicServiceClient(_channel);
        _docStub   = new DocumentService.DocumentServiceClient(_channel);
        _adminStub = new AdminService.AdminServiceClient(_channel);
        _txnStub   = new TransactionService.TransactionServiceClient(_channel);

        var metaStub = new MetadataService.MetadataServiceClient(_channel);
        _keyMap = new ClientKeyMap(metaStub, _headers);
    }

    // ── Collection access ─────────────────────────────────────────────────────

    /// <summary>
    /// Returns a schema-less remote collection that mirrors
    /// <c>DynamicCollection</c>.
    /// </summary>
    public RemoteDynamicCollection GetDynamicCollection(string name)
    {
        ThrowIfDisposed();
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        return new RemoteDynamicCollection(name, _dynStub, _keyMap, _headers);
    }

    /// <summary>
    /// Returns a typed remote collection that mirrors
    /// <c>DocumentCollection&lt;TId, T&gt;</c>.
    /// </summary>
    /// <param name="mapper">
    /// The source-generated mapper for <typeparamref name="T"/>.
    /// Decorated with <c>[BLiteMapper]</c> on the entity class.
    /// </param>
    public RemoteCollection<TId, T> GetCollection<TId, T>(
        IDocumentMapper<TId, T> mapper)
        where T : class
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(mapper);
        return new RemoteCollection<TId, T>(
            mapper, _dynStub, _docStub, _keyMap, _headers);
    }

    // ── Collection management ─────────────────────────────────────────────────

    /// <summary>
    /// Returns the names of all collections visible to the current user.
    /// Namespace prefix is already stripped.
    /// </summary>
    public async Task<IReadOnlyList<string>> ListCollectionsAsync(
        CancellationToken ct = default)
    {
        ThrowIfDisposed();
        var response = await _dynStub.ListCollectionsAsync(
            new Empty(), _headers, cancellationToken: ct);
        return [.. response.Names];
    }

    /// <summary>Drops (deletes) the named collection and all its documents.</summary>
    public async Task<bool> DropCollectionAsync(
        string name, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        var response = await _dynStub.DropCollectionAsync(
            new DropCollectionRequest { Collection = name },
            _headers, cancellationToken: ct);
        return response.Success;
    }

    // ── Transactions ──────────────────────────────────────────────────────────

    /// <summary>
    /// Begins a new explicit transaction on the server.
    /// Use <c>await using</c> to ensure automatic rollback on failure.
    /// </summary>
    public async Task<RemoteTransaction> BeginTransactionAsync(
        CancellationToken ct = default)
    {
        ThrowIfDisposed();
        var response = await _txnStub.BeginAsync(
            new BeginTransactionRequest(), _headers, cancellationToken: ct);

        if (!string.IsNullOrEmpty(response.Error))
            throw new InvalidOperationException(
                $"BeginTransaction failed: {response.Error}");

        return new RemoteTransaction(response.TransactionId, _txnStub, _headers);
    }

    // ── Admin ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Admin operations (user management, permissions).
    /// The caller's API key must carry <c>Admin</c> permission.
    /// </summary>
    public RemoteAdminClient Admin
    {
        get
        {
            ThrowIfDisposed();
            return new RemoteAdminClient(_adminStub, _headers);
        }
    }

    // ── Key-map utilities ─────────────────────────────────────────────────────

    /// <summary>
    /// Forces a full reload of the server's global key map.
    /// Useful after reconnecting or when another client has registered new fields.
    /// Requires <c>Query</c> permission on <paramref name="anchorCollection"/>.
    /// </summary>
    public Task RefreshKeyMapAsync(
        string anchorCollection, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        return _keyMap.RefreshAsync(anchorCollection, ct);
    }

    // ── Disposal ──────────────────────────────────────────────────────────────

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        if (_ownsChannel)
        {
            await _channel.ShutdownAsync();
            _channel.Dispose();
        }
    }

    private void ThrowIfDisposed() =>
        ObjectDisposedException.ThrowIf(_disposed, this);
}
