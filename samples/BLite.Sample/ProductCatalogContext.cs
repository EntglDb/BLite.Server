// BLite.Sample — ProductCatalogContext
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Remote document context.  Two source generators cooperate at compile time:
//
//   BLite.SourceGenerators (from the BLite core repo):
//     - Analyses the Products property and generates the C-BSON mapper for Product
//     - Generates the InitializeCollections() override that assigns Products
//
//   BLite.Client.SourceGenerators (from this repo):
//     - Generates: ProductCatalogContext(BLiteClient client) : base()
//     - Generates: CreateCollection<TId,T> override → client.GetDocumentCollection(mapper)
//
// Usage:
//   services.AddSingleton(sp => new ProductCatalogContext(sp.GetRequiredService<BLiteClient>()));
//   ...
//   app.MapGet("/products", async (ProductCatalogContext db, CancellationToken ct) =>
//       TypedResults.Ok(await db.Products.AsQueryable().ToListAsync(ct)));

using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Metadata;

namespace BLite.Sample;

public partial class ProductCatalogContext : DocumentDbContext
{
    public IDocumentCollection<ObjectId, Product> Products { get; set; } = null!;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<Product>()
            .HasKey(p => p.Id);
    }
}
