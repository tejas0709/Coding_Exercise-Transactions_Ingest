using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using TransactionsIngest.Configuration;
using TransactionsIngest.Data;
using TransactionsIngest.DTOs;
using TransactionsIngest.Models;
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

    [Fact]
    public async Task ExecuteRunAsync_UpdatesExistingRecord_AndWritesAudit()
    {
        var now = new DateTime(2026, 3, 18, 12, 0, 0, DateTimeKind.Utc);
        await using var fixture = await SqliteFixture.CreateAsync();

        await using (var seedContext = fixture.CreateDbContext())
        {
            seedContext.Transactions.Add(new TransactionRecord
            {
                TransactionId = 1001,
                CardNumber = "************1111",
                LocationCode = "STO-01",
                ProductName = "Wireless Mouse",
                Amount = 19.99m,
                TransactionTimeUtc = now.AddHours(-2),
                Status = TransactionStatus.Active,
                CreatedAtUtc = now.AddHours(-2),
                UpdatedAtUtc = now.AddHours(-2)
            });
            await seedContext.SaveChangesAsync();
        }

        await using (var context = fixture.CreateDbContext())
        {
            var service = CreateService(
                context,
                now,
                [
                    new IncomingTransactionDto
                    {
                        TransactionId = 1001,
                        CardNumber = "4111111111111111",
                        LocationCode = "STO-09",
                        ProductName = "Wireless Mouse",
                        Amount = 21.99m,
                        Timestamp = now.AddHours(-1)
                    }
                ]);

            var summary = await service.ExecuteRunAsync();

            Assert.Equal(0, summary.InsertedCount);
            Assert.Equal(1, summary.UpdatedCount);

            var updated = await context.Transactions.SingleAsync(x => x.TransactionId == 1001);
            Assert.Equal("STO-09", updated.LocationCode);
            Assert.Equal(21.99m, updated.Amount);

            var updateAudit = await context.TransactionAudits.SingleAsync(x => x.TransactionId == 1001 && x.Action == "Updated");
            Assert.NotNull(updateAudit);
        }
    }

    [Fact]
    public async Task ExecuteRunAsync_RevokesMissingInWindowRecords()
    {
        var now = new DateTime(2026, 3, 18, 12, 0, 0, DateTimeKind.Utc);
        await using var fixture = await SqliteFixture.CreateAsync();

        await using (var seedContext = fixture.CreateDbContext())
        {
            seedContext.Transactions.AddRange(
                new TransactionRecord
                {
                    TransactionId = 1001,
                    CardNumber = "************1111",
                    LocationCode = "STO-01",
                    ProductName = "Wireless Mouse",
                    Amount = 19.99m,
                    TransactionTimeUtc = now.AddHours(-3),
                    Status = TransactionStatus.Active,
                    CreatedAtUtc = now.AddHours(-3),
                    UpdatedAtUtc = now.AddHours(-3)
                },
                new TransactionRecord
                {
                    TransactionId = 1002,
                    CardNumber = "************0002",
                    LocationCode = "STO-02",
                    ProductName = "USB-C Cable",
                    Amount = 25m,
                    TransactionTimeUtc = now.AddHours(-2),
                    Status = TransactionStatus.Active,
                    CreatedAtUtc = now.AddHours(-2),
                    UpdatedAtUtc = now.AddHours(-2)
                });
            await seedContext.SaveChangesAsync();
        }

        await using (var context = fixture.CreateDbContext())
        {
            var service = CreateService(
                context,
                now,
                [
                    new IncomingTransactionDto
                    {
                        TransactionId = 1002,
                        CardNumber = "4000000000000002",
                        LocationCode = "STO-02",
                        ProductName = "USB-C Cable",
                        Amount = 25m,
                        Timestamp = now.AddHours(-2)
                    }
                ]);

            var summary = await service.ExecuteRunAsync();

            Assert.Equal(1, summary.RevokedCount);
            var revoked = await context.Transactions.SingleAsync(x => x.TransactionId == 1001);
            Assert.Equal(TransactionStatus.Revoked, revoked.Status);

            var revokeAudit = await context.TransactionAudits.SingleAsync(x => x.TransactionId == 1001 && x.Action == "Revoked");
            Assert.NotNull(revokeAudit);
        }
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
