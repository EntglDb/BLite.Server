using BLite.Proto.V1;
using BLite.Server.Auth;
using BLite.Server.Transactions;
using Grpc.Core;

namespace BLite.Server.Services;

/// <summary>
/// gRPC implementation of <c>TransactionService</c> defined in <c>blite.proto</c>.
///
/// <para>Flow:</para>
/// <list type="number">
///   <item>Client calls <c>Begin</c> → receives an opaque <c>transaction_id</c> (UUID).</item>
///   <item>Client passes <c>transaction_id</c> in the optional field of any write RPC
///         (Insert / Update / Delete / Bulk variants). Those writes execute without
///         auto-commit.</item>
///   <item>Client calls <c>Commit</c> or <c>Rollback</c> to finalise the transaction.</item>
/// </list>
///
/// <para>
/// At most one transaction can be active at a time (engine constraint).
/// <see cref="TransactionManager"/> serialises concurrent <c>Begin</c> requests.
/// Sessions that idle longer than <c>Transactions:TimeoutSeconds</c> are rolled back
/// automatically by <see cref="TransactionCleanupService"/>.
/// </para>
/// </summary>
public sealed class TransactionServiceImpl : TransactionService.TransactionServiceBase
{
    private readonly TransactionManager _txnManager;

    public TransactionServiceImpl(TransactionManager txnManager)
    {
        _txnManager = txnManager;
    }

    // ── Begin ──────────────────────────────────────────────────────────────────

    public override async Task<BeginTransactionResponse> Begin(
        BeginTransactionRequest request,
        ServerCallContext       context)
    {
        var user = BLiteServiceBase.GetCurrentUser(context);
        try
        {
            var txnId = await _txnManager.BeginAsync(user, context.CancellationToken);
            return new BeginTransactionResponse { TransactionId = txnId };
        }
        catch (RpcException)
        {
            throw;
        }
        catch (Exception ex)
        {
            return new BeginTransactionResponse { Error = ex.Message };
        }
    }

    // ── Commit ─────────────────────────────────────────────────────────────────

    public override async Task<MutationResponse> Commit(
        TransactionRequest request,
        ServerCallContext  context)
    {
        var user = BLiteServiceBase.GetCurrentUser(context);
        try
        {
            await _txnManager.CommitAsync(request.TransactionId, user, context.CancellationToken);
            return new MutationResponse { Success = true };
        }
        catch (RpcException)
        {
            throw;
        }
        catch (Exception ex)
        {
            return new MutationResponse { Success = false, Error = ex.Message };
        }
    }

    // ── Rollback ───────────────────────────────────────────────────────────────

    public override async Task<MutationResponse> Rollback(
        TransactionRequest request,
        ServerCallContext  context)
    {
        var user = BLiteServiceBase.GetCurrentUser(context);
        try
        {
            await _txnManager.RollbackAsync(request.TransactionId, user, context.CancellationToken);
            return new MutationResponse { Success = true };
        }
        catch (RpcException)
        {
            throw;
        }
        catch (Exception ex)
        {
            return new MutationResponse { Success = false, Error = ex.Message };
        }
    }
}
