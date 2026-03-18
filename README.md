# Transactions Ingest (Coding Exercise)

## Repository

- GitHub: https://github.com/tejas0709/Coding_Exercise-Transactions_Ingest.git

## Time Tracking

- Estimated before implementation: 6.0 hours
- Actual implementation time: 4.5 hours

## Problem Summary

This project implements a single-run ingestion console app for retail transaction snapshots where events can be delayed and out of order. Each run ingests the last-24-hour snapshot, upserts by `TransactionId`, records audit changes, revokes missing in-window records, optionally finalizes older records, and preserves idempotent behavior.

## Tech Stack

- .NET 10 console application
- Entity Framework Core (code-first)
- SQLite
- xUnit tests

## Functional Coverage

1. Single-run execution with no scheduler in app.
2. Snapshot fetched from mocked JSON source.
3. Upsert by `TransactionId`:
   - insert for new records
   - update for changed records
   - field-level audit history
4. Revocation for missing records within 24-hour window.
5. Optional finalization for records older than window.
6. Idempotent repeated runs and one DB transaction per run.

## Project Structure

```
Refactor/
  CodingExercise.TransactionsIngest.slnx
  README.md
  TransactionsIngest/
    Configuration/
      IngestOptions.cs
    Data/
      TransactionsDbContext.cs
      sample-transactions.json
    DTOs/
      IncomingTransactionDto.cs
    Models/
      TransactionRecord.cs
      TransactionAudit.cs
      TransactionStatus.cs
      AuditChange.cs
    Services/
      IClock.cs
      SystemClock.cs
      ITransactionSnapshotProvider.cs
      JsonSnapshotProvider.cs
      IngestionRunSummary.cs
      TransactionIngestionService.cs
    Program.cs
    appsettings.json
    TransactionsIngest.csproj
  TransactionsIngest.Tests/
    UnitTest1.cs
    TransactionsIngest.Tests.csproj
```

## Data Model

### TransactionRecord

- `TransactionId` (unique business key)
- `CardNumber` (masked, max length 20)
- `LocationCode` (max length 20)
- `ProductName` (max length 20)
- `Amount` (`decimal(18,2)`)
- `TransactionTimeUtc` (UTC)
- `Status` (`Active`, `Revoked`, `Finalized`)
- `CreatedAtUtc`, `UpdatedAtUtc`

### TransactionAudit

- `TransactionRecordId` (FK)
- `TransactionId`
- `RunId`
- `Action`
- `ChangesJson`
- `OccurredAtUtc`

## Configuration

```json
{
  "ConnectionStrings": {
    "DefaultConnection": "Data Source=transactions.db"
  },
  "Ingest": {
    "SnapshotPath": "Data/sample-transactions.json",
    "EnableFinalization": true,
    "SnapshotWindowHours": 24
  }
}
```

## Build, Run, Test

From `Refactor/`:

```bash
dotnet restore

dotnet build TransactionsIngest/TransactionsIngest.csproj

dotnet run --project TransactionsIngest/TransactionsIngest.csproj

dotnet test TransactionsIngest.Tests/TransactionsIngest.Tests.csproj
```

## Design Decisions and Assumptions

- `TransactionId` is the source-of-truth identifier for reconciliation.
- Card numbers are masked (last 4 only) for privacy.
- Timestamps are normalized to UTC.
- Snapshot duplicates are normalized deterministically by latest timestamp.
- Finalized records are treated as immutable.

## Problems Encountered and Resolutions

1. Package alignment issues during dependency setup.
   - Resolved by pinning compatible package versions.
2. Keeping tests realistic without external dependencies.
   - Used SQLite in-memory fixtures for integration-style tests.
3. Ensuring audit payloads are verifiable.
   - Added assertions for update and revoke audit JSON details.

## Highlights

- Clear separation by layers (`Models`, `Data`, `Services`, `DTOs`, `Configuration`).
- Transaction-safe single-run ingestion flow.
- Deterministic and idempotent behavior.
- Focused tests for update, revoke, idempotency, finalization, and duplicate inputs.
