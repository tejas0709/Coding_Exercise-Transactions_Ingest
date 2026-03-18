namespace TransactionsIngest.Models;

public sealed class TransactionRecord
{
    public long Id { get; set; }
    public int TransactionId { get; set; }
    public string CardNumber { get; set; } = string.Empty;
    public string LocationCode { get; set; } = string.Empty;
    public string ProductName { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public DateTime TransactionTimeUtc { get; set; }
    public TransactionStatus Status { get; set; } = TransactionStatus.Active;
    public DateTime CreatedAtUtc { get; set; }
    public DateTime UpdatedAtUtc { get; set; }

    public ICollection<TransactionAudit> Audits { get; set; } = new List<TransactionAudit>();
}
