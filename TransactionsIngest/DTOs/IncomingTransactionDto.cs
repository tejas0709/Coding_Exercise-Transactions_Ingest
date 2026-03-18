using System.Text.Json.Serialization;

namespace TransactionsIngest.DTOs;

public sealed class IncomingTransactionDto
{
    [JsonPropertyName("transactionId")]
    public int TransactionId { get; init; }

    [JsonPropertyName("cardNumber")]
    public string CardNumber { get; init; } = string.Empty;

    [JsonPropertyName("locationCode")]
    public string LocationCode { get; init; } = string.Empty;

    [JsonPropertyName("productName")]
    public string ProductName { get; init; } = string.Empty;

    [JsonPropertyName("amount")]
    public decimal Amount { get; init; }

    [JsonPropertyName("timestamp")]
    public DateTime Timestamp { get; init; }
}
