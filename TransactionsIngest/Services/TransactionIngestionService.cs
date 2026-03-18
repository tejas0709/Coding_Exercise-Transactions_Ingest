using TransactionsIngest.Configuration;
using TransactionsIngest.Data;
using TransactionsIngest.Models;

namespace TransactionsIngest.Services;

public sealed class TransactionIngestionService(
    TransactionsDbContext dbContext,
    IClock clock,
    ITransactionSnapshotProvider snapshotProvider,
    IngestOptions options)
{
    public async Task<IngestionRunSummary> ExecuteRunAsync(CancellationToken cancellationToken = default)
    {
        if (options.SnapshotWindowHours <= 0)
        {
            throw new InvalidOperationException("Ingest snapshot window must be greater than zero.");
        }

        var now = clock.UtcNow;
        var summary = new IngestionRunSummary
        {
            RunId = Guid.NewGuid(),
            InsertedCount = 0,
            UpdatedCount = 0,
            RevokedCount = 0,
            FinalizedCount = 0
        };

        var incomingSnapshot = await snapshotProvider.LoadLast24HourSnapshotAsync(cancellationToken);
        var distinctIncoming = incomingSnapshot
            .GroupBy(x => x.TransactionId)
            .Select(group => group
                .OrderByDescending(x => EnsureUtc(x.Timestamp))
                .First())
            .ToList();

        var incomingIds = distinctIncoming.Select(x => x.TransactionId).ToHashSet();
        var existingIds = dbContext.Transactions
            .Where(x => incomingIds.Contains(x.TransactionId))
            .Select(x => x.TransactionId)
            .ToHashSet();

        foreach (var incoming in distinctIncoming)
        {
            if (existingIds.Contains(incoming.TransactionId))
            {
                continue;
            }

            dbContext.Transactions.Add(new TransactionRecord
            {
                TransactionId = incoming.TransactionId,
                CardNumber = MaskCardNumber(incoming.CardNumber),
                LocationCode = incoming.LocationCode,
                ProductName = incoming.ProductName,
                Amount = incoming.Amount,
                TransactionTimeUtc = EnsureUtc(incoming.Timestamp),
                Status = TransactionStatus.Active,
                CreatedAtUtc = now,
                UpdatedAtUtc = now
            });

            summary.InsertedCount++;
        }

        await dbContext.SaveChangesAsync(cancellationToken);
        return summary;
    }

    private static string MaskCardNumber(string cardNumber)
    {
        var digitsOnly = new string(cardNumber.Where(char.IsDigit).ToArray());
        if (digitsOnly.Length <= 4)
        {
            return digitsOnly;
        }

        var last4 = digitsOnly[^4..];
        return $"************{last4}";
    }

    private static DateTime EnsureUtc(DateTime timestamp)
    {
        return timestamp.Kind switch
        {
            DateTimeKind.Utc => timestamp,
            DateTimeKind.Local => timestamp.ToUniversalTime(),
            _ => DateTime.SpecifyKind(timestamp, DateTimeKind.Utc)
        };
    }
}
