using Microsoft.EntityFrameworkCore;
using TransactionsIngest.Models;

namespace TransactionsIngest.Data;

public sealed class TransactionsDbContext(DbContextOptions<TransactionsDbContext> options) : DbContext(options)
{
    public DbSet<TransactionRecord> Transactions => Set<TransactionRecord>();
    public DbSet<TransactionAudit> TransactionAudits => Set<TransactionAudit>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<TransactionRecord>(entity =>
        {
            entity.ToTable("transactions");
            entity.HasKey(x => x.Id);
            entity.HasIndex(x => x.TransactionId).IsUnique();

            entity.Property(x => x.TransactionId).IsRequired();
            entity.Property(x => x.CardNumber).IsRequired().HasMaxLength(20);
            entity.Property(x => x.LocationCode).IsRequired().HasMaxLength(20);
            entity.Property(x => x.ProductName).IsRequired().HasMaxLength(20);
            entity.Property(x => x.Amount).HasPrecision(18, 2);
            entity.Property(x => x.TransactionTimeUtc).IsRequired();
            entity.Property(x => x.CreatedAtUtc).IsRequired();
            entity.Property(x => x.UpdatedAtUtc).IsRequired();
            entity.Property(x => x.Status).HasConversion<string>().HasMaxLength(20).IsRequired();
        });

        modelBuilder.Entity<TransactionAudit>(entity =>
        {
            entity.ToTable("transaction_audits");
            entity.HasKey(x => x.Id);

            entity.Property(x => x.TransactionId).IsRequired();
            entity.Property(x => x.RunId).IsRequired();
            entity.Property(x => x.Action).IsRequired().HasMaxLength(30);
            entity.Property(x => x.ChangesJson).IsRequired();
            entity.Property(x => x.OccurredAtUtc).IsRequired();

            entity.HasOne(x => x.TransactionRecord)
                .WithMany(x => x.Audits)
                .HasForeignKey(x => x.TransactionRecordId)
                .OnDelete(DeleteBehavior.Cascade);
        });
    }
}
