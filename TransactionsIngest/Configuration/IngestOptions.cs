namespace TransactionsIngest.Configuration;

public sealed class IngestOptions
{
    public const string SectionName = "Ingest";

    public string SnapshotPath { get; init; } = "Data/sample-transactions.json";
    public bool EnableFinalization { get; init; } = true;
    public int SnapshotWindowHours { get; init; } = 24;
}
