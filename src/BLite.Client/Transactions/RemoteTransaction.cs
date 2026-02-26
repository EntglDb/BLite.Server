// BLite.Client — RemoteTransaction
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

using BLite.Proto.V1;
using Grpc.Core;

namespace BLite.Client.Transactions;

/// <summary>
/// Represents an explicit server-side transaction obtained via
/// <see cref="BLiteClient.BeginTransactionAsync"/>.
///
/// Pass a <see cref="RemoteTransaction"/> to collection write methods to enrol
/// them in the same atomic unit.  Call <see cref="CommitAsync"/> to persist, or
/// <see cref="RollbackAsync"/> to discard.  If the instance is disposed without
/// committing, <see cref="DisposeAsync"/> automatically rolls back.
/// </summary>
public sealed class RemoteTransaction : IAsyncDisposable
{
    private readonly TransactionService.TransactionServiceClient _stub;
    private readonly Metadata _headers;
    private bool _completed;

    /// <summary>
    /// The opaque transaction ID assigned by the server.
    /// Passed as <c>transaction_id</c> in every write RPC.
    /// </summary>
    public string TransactionId { get; }

    internal RemoteTransaction(
        string transactionId,
        TransactionService.TransactionServiceClient stub,
        Metadata headers)
    {
        TransactionId = transactionId;
        _stub         = stub;
        _headers      = headers;
    }

    /// <summary>Commits the transaction, making all enrolled writes permanent.</summary>
    public async Task CommitAsync(CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_completed, this);
        var resp = await _stub.CommitAsync(
            new TransactionRequest { TransactionId = TransactionId },
            _headers, cancellationToken: ct);

        _completed = true;

        if (!resp.Success)
            throw new InvalidOperationException(
                $"Transaction commit failed: {resp.Error}");
    }

    /// <summary>Explicitly rolls back the transaction.</summary>
    public async Task RollbackAsync(CancellationToken ct = default)
    {
        if (_completed) return;
        _completed = true;

        await _stub.RollbackAsync(
            new TransactionRequest { TransactionId = TransactionId },
            _headers, cancellationToken: ct);
    }

    /// <summary>
    /// Rolls back automatically if the transaction has not been committed.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_completed) return;
        try
        {
            await RollbackAsync();
        }
        catch
        {
            // Best-effort cleanup — swallow errors during dispose.
        }
    }
}
