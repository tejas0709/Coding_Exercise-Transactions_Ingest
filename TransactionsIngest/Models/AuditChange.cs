namespace TransactionsIngest.Models;

public sealed class AuditChange
{
    public required string Field { get; init; }
    public required string? OldValue { get; init; }
    public required string? NewValue { get; init; }
}
