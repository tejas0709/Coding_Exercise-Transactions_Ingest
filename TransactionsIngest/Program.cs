using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using TransactionsIngest.Configuration;
using TransactionsIngest.Data;
using TransactionsIngest.Services;

var configuration = new ConfigurationBuilder()
	.SetBasePath(AppContext.BaseDirectory)
	.AddJsonFile("appsettings.json", optional: false, reloadOnChange: false)
	.AddEnvironmentVariables(prefix: "TI_")
	.Build();

var ingestOptions = configuration.GetSection(IngestOptions.SectionName).Get<IngestOptions>()
	?? throw new InvalidOperationException("Failed to load ingest options from configuration.");

var connectionString = configuration.GetConnectionString("DefaultConnection");
if (string.IsNullOrWhiteSpace(connectionString))
{
	throw new InvalidOperationException("Connection string 'DefaultConnection' is required.");
}

var dbContextOptions = new DbContextOptionsBuilder<TransactionsDbContext>()
	.UseSqlite(connectionString)
	.Options;

await using var dbContext = new TransactionsDbContext(dbContextOptions);
await dbContext.Database.EnsureCreatedAsync();

var clock = new SystemClock();
var snapshotProvider = new JsonSnapshotProvider(ingestOptions, clock);
var ingestService = new TransactionIngestionService(dbContext, clock, snapshotProvider, ingestOptions);

var summary = await ingestService.ExecuteRunAsync();

Console.WriteLine("Transactions ingest run completed.");
Console.WriteLine($"RunId: {summary.RunId}");
Console.WriteLine($"Inserted: {summary.InsertedCount}");
Console.WriteLine($"Updated: {summary.UpdatedCount}");
Console.WriteLine($"Revoked: {summary.RevokedCount}");
Console.WriteLine($"Finalized: {summary.FinalizedCount}");
Console.WriteLine("Hello, World!");
