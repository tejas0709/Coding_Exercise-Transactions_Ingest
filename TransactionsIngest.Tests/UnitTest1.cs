using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using TransactionsIngest.Configuration;
using TransactionsIngest.Data;
using TransactionsIngest.DTOs;
using TransactionsIngest.Services;

namespace TransactionsIngest.Tests;

public sealed class TransactionIngestionServiceTests
{
    [Fact]
    public async Task ExecuteRunAsync_WithEmptySnapshot_CompletesSuccessfully()
    {
        var now = new DateTime(2026, 3, 18, 12, 0, 0, DateTimeKind.Utc);
        await using var fixture = await SqliteFixture.CreateAsync();
        await using var context = fixture.CreateDbContext();

        var service = CreateService(context, now, []);
        var summary = await service.ExecuteRunAsync();

        Assert.NotEqual(Guid.Empty, summary.RunId);
    }

    private static TransactionIngestionService CreateService(
        TransactionsDbContext context,
        DateTime now,
        IReadOnlyList<IncomingTransactionDto> snapshot)
    {
        var options = new IngestOptions
        {
            EnableFinalization = true,
            SnapshotWindowHours = 24
        };

        return new TransactionIngestionService(
            context,
            new FakeClock(now),
            new InMemorySnapshotProvider(snapshot),
            options);
    }

    private sealed class InMemorySnapshotProvider(IReadOnlyList<IncomingTransactionDto> snapshot) : ITransactionSnapshotProvider
    {
        public Task<IReadOnlyList<IncomingTransactionDto>> LoadLast24HourSnapshotAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(snapshot);
    }

    private sealed class FakeClock(DateTime utcNow) : IClock
    {
        public DateTime UtcNow { get; } = utcNow;
    }

    private sealed class SqliteFixture : IAsyncDisposable
    {
        private readonly SqliteConnection _connection;

        private SqliteFixture(SqliteConnection connection)
        {
            _connection = connection;
        }

        public static async Task<SqliteFixture> CreateAsync()
        {
            var connection = new SqliteConnection("DataSource=:memory:");
            await connection.OpenAsync();

            var options = new DbContextOptionsBuilder<TransactionsDbContext>()
                .UseSqlite(connection)
                .Options;

            await using var context = new TransactionsDbContext(options);
            await context.Database.EnsureCreatedAsync();

            return new SqliteFixture(connection);
        }

        public TransactionsDbContext CreateDbContext()
        {
            var options = new DbContextOptionsBuilder<TransactionsDbContext>()
                .UseSqlite(_connection)
                .Options;
            return new TransactionsDbContext(options);
        }

        public async ValueTask DisposeAsync()
        {
            await _connection.DisposeAsync();
        }
    }
}
