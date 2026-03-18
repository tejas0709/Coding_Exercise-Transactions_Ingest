namespace TransactionsIngest.Services;

public sealed class IngestionRunSummary
{
    public required Guid RunId { get; init; }
    public int InsertedCount { get; set; }
    public int UpdatedCount { get; set; }
    public int RevokedCount { get; set; }
    public int FinalizedCount { get; set; }
}
