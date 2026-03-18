using System.Text.Json;
using TransactionsIngest.Configuration;
using TransactionsIngest.DTOs;

namespace TransactionsIngest.Services;

public sealed class JsonSnapshotProvider(IngestOptions options, IClock clock) : ITransactionSnapshotProvider
{
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public async Task<IReadOnlyList<IncomingTransactionDto>> LoadLast24HourSnapshotAsync(CancellationToken cancellationToken = default)
    {
        var snapshotPath = ResolveSnapshotPath(options.SnapshotPath);
        if (!File.Exists(snapshotPath))
        {
            throw new FileNotFoundException($"Snapshot file not found at '{snapshotPath}'.");
        }

        await using var stream = File.OpenRead(snapshotPath);
        var payload = await JsonSerializer.DeserializeAsync<List<IncomingTransactionDto>>(stream, SerializerOptions, cancellationToken);
        if (payload is null)
        {
            return [];
        }

        var now = clock.UtcNow;
        var windowStart = now.AddHours(-options.SnapshotWindowHours);

        return payload
            .Where(IsValid)
            .Select(x => new IncomingTransactionDto
            {
                TransactionId = x.TransactionId,
                CardNumber = x.CardNumber.Trim(),
                LocationCode = Truncate(x.LocationCode.Trim(), 20),
                ProductName = Truncate(x.ProductName.Trim(), 20),
                Amount = decimal.Round(x.Amount, 2, MidpointRounding.AwayFromZero),
                Timestamp = EnsureUtc(x.Timestamp)
            })
            .Where(x => x.Timestamp >= windowStart && x.Timestamp <= now)
            .ToList();
    }

    private static string ResolveSnapshotPath(string configuredPath)
    {
        if (Path.IsPathRooted(configuredPath))
        {
            return configuredPath;
        }

        return Path.Combine(AppContext.BaseDirectory, configuredPath);
    }

    private static bool IsValid(IncomingTransactionDto transaction)
    {
        return transaction.TransactionId > 0
               && !string.IsNullOrWhiteSpace(transaction.CardNumber)
               && !string.IsNullOrWhiteSpace(transaction.LocationCode)
               && !string.IsNullOrWhiteSpace(transaction.ProductName);
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

    private static string Truncate(string value, int maxLength)
    {
        return value.Length <= maxLength ? value : value[..maxLength];
    }
}
