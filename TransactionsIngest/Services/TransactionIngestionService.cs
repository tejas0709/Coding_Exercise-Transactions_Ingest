using System.Text.Json;
using TransactionsIngest.Configuration;
using TransactionsIngest.Data;
using TransactionsIngest.DTOs;
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
        var existingByTransactionId = dbContext.Transactions
            .Where(x => incomingIds.Contains(x.TransactionId))
            .ToDictionary(x => x.TransactionId);

        foreach (var incoming in distinctIncoming)
        {
            if (!existingByTransactionId.TryGetValue(incoming.TransactionId, out var existing))
            {
                var inserted = new TransactionRecord
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
                };

                dbContext.Transactions.Add(inserted);
                AddAudit(inserted, summary.RunId, "Inserted", now, [
                    new AuditChange { Field = "Status", OldValue = null, NewValue = TransactionStatus.Active.ToString() }
                ]);

                summary.InsertedCount++;
                continue;
            }

            if (existing.Status == TransactionStatus.Finalized)
            {
                continue;
            }

            var changes = GetChanges(existing, incoming);
            if (changes.Count == 0)
            {
                continue;
            }

            existing.CardNumber = MaskCardNumber(incoming.CardNumber);
            existing.LocationCode = incoming.LocationCode;
            existing.ProductName = incoming.ProductName;
            existing.Amount = incoming.Amount;
            existing.TransactionTimeUtc = EnsureUtc(incoming.Timestamp);
            existing.UpdatedAtUtc = now;

            AddAudit(existing, summary.RunId, "Updated", now, changes);
            summary.UpdatedCount++;
        }

        await dbContext.SaveChangesAsync(cancellationToken);
        return summary;
    }

    private static List<AuditChange> GetChanges(TransactionRecord existing, IncomingTransactionDto incoming)
    {
        var changes = new List<AuditChange>();
        var maskedCard = MaskCardNumber(incoming.CardNumber);
        if (!string.Equals(existing.CardNumber, maskedCard, StringComparison.Ordinal))
        {
            changes.Add(new AuditChange
            {
                Field = "CardNumber",
                OldValue = existing.CardNumber,
                NewValue = maskedCard
            });
        }

        if (!string.Equals(existing.LocationCode, incoming.LocationCode, StringComparison.Ordinal))
        {
            changes.Add(new AuditChange
            {
                Field = "LocationCode",
                OldValue = existing.LocationCode,
                NewValue = incoming.LocationCode
            });
        }

        if (!string.Equals(existing.ProductName, incoming.ProductName, StringComparison.Ordinal))
        {
            changes.Add(new AuditChange
            {
                Field = "ProductName",
                OldValue = existing.ProductName,
                NewValue = incoming.ProductName
            });
        }

        if (existing.Amount != incoming.Amount)
        {
            changes.Add(new AuditChange
            {
                Field = "Amount",
                OldValue = existing.Amount.ToString("0.00"),
                NewValue = incoming.Amount.ToString("0.00")
            });
        }

        var incomingTime = EnsureUtc(incoming.Timestamp);
        if (existing.TransactionTimeUtc != incomingTime)
        {
            changes.Add(new AuditChange
            {
                Field = "TransactionTimeUtc",
                OldValue = existing.TransactionTimeUtc.ToString("O"),
                NewValue = incomingTime.ToString("O")
            });
        }

        return changes;
    }

    private void AddAudit(TransactionRecord transaction, Guid runId, string action, DateTime occurredAtUtc, IReadOnlyList<AuditChange> changes)
    {
        dbContext.TransactionAudits.Add(new TransactionAudit
        {
            TransactionRecord = transaction,
            TransactionId = transaction.TransactionId,
            RunId = runId,
            Action = action,
            ChangesJson = JsonSerializer.Serialize(changes),
            OccurredAtUtc = occurredAtUtc
        });
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
