using TransactionsIngest.Configuration;
using TransactionsIngest.Data;

namespace TransactionsIngest.Services;

public sealed class TransactionIngestionService(
    TransactionsDbContext dbContext,
    IClock clock,
    ITransactionSnapshotProvider snapshotProvider,
    IngestOptions options)
{
    public async Task<IngestionRunSummary> ExecuteRunAsync(CancellationToken cancellationToken = default)
    {
        _ = dbContext;
        _ = snapshotProvider;
        _ = options;

        return await Task.FromResult(new IngestionRunSummary
        {
            RunId = Guid.NewGuid(),
            InsertedCount = 0,
            UpdatedCount = 0,
            RevokedCount = 0,
            FinalizedCount = 0
        });
    }
}
