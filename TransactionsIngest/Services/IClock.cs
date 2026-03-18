namespace TransactionsIngest.Services;

public interface IClock
{
    DateTime UtcNow { get; }
}
