using TransactionsIngest.DTOs;

namespace TransactionsIngest.Services;

public interface ITransactionSnapshotProvider
{
    Task<IReadOnlyList<IncomingTransactionDto>> LoadLast24HourSnapshotAsync(CancellationToken cancellationToken = default);
}
