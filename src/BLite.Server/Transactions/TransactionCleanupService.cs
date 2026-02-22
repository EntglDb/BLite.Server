namespace BLite.Server.Transactions;

/// <summary>
/// Background service that periodically rolls back expired transactions.
/// Runs every 10 seconds; the actual timeout is controlled by
/// <c>Transactions:TimeoutSeconds</c> in appsettings.
/// </summary>
public sealed class TransactionCleanupService : BackgroundService
{
    private static readonly TimeSpan _interval = TimeSpan.FromSeconds(10);

    private readonly TransactionManager              _manager;
    private readonly ILogger<TransactionCleanupService> _logger;

    public TransactionCleanupService(
        TransactionManager                  manager,
        ILogger<TransactionCleanupService>  logger)
    {
        _manager = manager;
        _logger  = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(_interval);

        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            try
            {
                await _manager.CleanupExpiredAsync(stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during transaction cleanup sweep.");
            }
        }
    }
}
