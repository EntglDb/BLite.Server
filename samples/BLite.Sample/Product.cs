// BLite.Sample — Product entity
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

using BLite.Bson;

namespace BLite.Sample;

/// <summary>
/// Product catalog entity.  The source generator discovers it through the
/// <see cref="ProductCatalogContext.Products"/> property and produces:
///   • a C-BSON serialiser/deserialiser (<c>ProductCatalogContext_*_Mappers.BLite_Sample_ProductMapper</c>)
///   • the <c>InitializeCollections</c> override that wires the collection property
/// Default collection name: "products" (lowercase class name + "s").
/// </summary>
public class Product
{
    [BsonId]
    public ObjectId Id { get; set; }

    public string Name { get; set; } = string.Empty;

    public string Category { get; set; } = string.Empty;

    public double Price { get; set; }

    public int Stock { get; set; }

    public bool Active { get; set; } = true;

    public DateTime CreatedAt { get; set; }
}
