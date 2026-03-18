namespace TransactionsIngest.Models;

public sealed class TransactionAudit
{
    public long Id { get; set; }
    public long TransactionRecordId { get; set; }
    public int TransactionId { get; set; }
    public Guid RunId { get; set; }
    public string Action { get; set; } = string.Empty;
    public string ChangesJson { get; set; } = "[]";
    public DateTime OccurredAtUtc { get; set; }

    public TransactionRecord TransactionRecord { get; set; } = default!;
}
