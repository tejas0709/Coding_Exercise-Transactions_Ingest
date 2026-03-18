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

    [Fact]
    public async Task ExecuteRunAsync_IsIdempotent_ForSameSnapshot()
    {
        var now = new DateTime(2026, 3, 18, 12, 0, 0, DateTimeKind.Utc);
        await using var fixture = await SqliteFixture.CreateAsync();

        var snapshot = new List<IncomingTransactionDto>
        {
            new()
            {
                TransactionId = 1001,
                CardNumber = "4111111111111111",
                LocationCode = "STO-01",
                ProductName = "Wireless Mouse",
                Amount = 19.99m,
                Timestamp = now.AddHours(-1)
            },
            new()
            {
                TransactionId = 1002,
                CardNumber = "4000000000000002",
                LocationCode = "STO-02",
                ProductName = "USB-C Cable",
                Amount = 25m,
                Timestamp = now.AddHours(-2)
            }
        };

        await using (var firstContext = fixture.CreateDbContext())
        {
            var service = CreateService(firstContext, now, snapshot);
            await service.ExecuteRunAsync();
        }

        await using (var secondContext = fixture.CreateDbContext())
        {
            var service = CreateService(secondContext, now, snapshot);
            var secondSummary = await service.ExecuteRunAsync();

            Assert.Equal(0, secondSummary.InsertedCount);
            Assert.Equal(0, secondSummary.UpdatedCount);
            Assert.Equal(0, secondSummary.RevokedCount);
            Assert.Equal(0, secondSummary.FinalizedCount);

            var transactionCount = await secondContext.Transactions.CountAsync();
            var auditCount = await secondContext.TransactionAudits.CountAsync();

            Assert.Equal(2, transactionCount);
            Assert.Equal(2, auditCount);
        }
    }

    [Fact]
    public async Task ExecuteRunAsync_DoesNotModifyFinalizedRecords()
    {
        var now = new DateTime(2026, 3, 18, 12, 0, 0, DateTimeKind.Utc);
        await using var fixture = await SqliteFixture.CreateAsync();

        await using (var seedContext = fixture.CreateDbContext())
        {
            seedContext.Transactions.Add(new TransactionRecord
            {
                TransactionId = 3001,
                CardNumber = "************1111",
                LocationCode = "STO-01",
                ProductName = "Original Item",
                Amount = 10.50m,
                TransactionTimeUtc = now.AddHours(-30),
                Status = TransactionStatus.Finalized,
                CreatedAtUtc = now.AddHours(-30),
                UpdatedAtUtc = now.AddHours(-30)
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
                        TransactionId = 3001,
                        CardNumber = "4111111111111111",
                        LocationCode = "STO-99",
                        ProductName = "Changed Item",
                        Amount = 99.99m,
                        Timestamp = now.AddHours(-1)
                    }
                ]);

            var summary = await service.ExecuteRunAsync();

            Assert.Equal(0, summary.InsertedCount);
            Assert.Equal(0, summary.UpdatedCount);
            Assert.Equal(0, summary.RevokedCount);
            Assert.Equal(0, summary.FinalizedCount);

            var record = await context.Transactions.SingleAsync(x => x.TransactionId == 3001);
            Assert.Equal(TransactionStatus.Finalized, record.Status);
            Assert.Equal("STO-01", record.LocationCode);
            Assert.Equal("Original Item", record.ProductName);
            Assert.Equal(10.50m, record.Amount);
        }
    }

    [Fact]
    public async Task ExecuteRunAsync_DuplicateTransactionIdInSnapshot_UsesLatestTimestamp()
    {
        var now = new DateTime(2026, 3, 18, 12, 0, 0, DateTimeKind.Utc);
        await using var fixture = await SqliteFixture.CreateAsync();

        await using (var context = fixture.CreateDbContext())
        {
            var service = CreateService(
                context,
                now,
                [
                    new IncomingTransactionDto
                    {
                        TransactionId = 4001,
                        CardNumber = "4111111111111111",
                        LocationCode = "STO-NEW",
                        ProductName = "Latest Version",
                        Amount = 30.00m,
                        Timestamp = now.AddHours(-1)
                    },
                    new IncomingTransactionDto
                    {
                        TransactionId = 4001,
                        CardNumber = "4111111111111111",
                        LocationCode = "STO-OLD",
                        ProductName = "Old Version",
                        Amount = 15.00m,
                        Timestamp = now.AddHours(-5)
                    }
                ]);

            var summary = await service.ExecuteRunAsync();

            Assert.Equal(1, summary.InsertedCount);
            Assert.Equal(0, summary.UpdatedCount);

            var record = await context.Transactions.SingleAsync(x => x.TransactionId == 4001);
            Assert.Equal("STO-NEW", record.LocationCode);
            Assert.Equal("Latest Version", record.ProductName);
            Assert.Equal(30.00m, record.Amount);
            Assert.Equal(now.AddHours(-1), record.TransactionTimeUtc);
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
