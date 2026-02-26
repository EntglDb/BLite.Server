// BLite.Client.IntegrationTests — TestProduct entity
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Simple entity decorated with [DocumentMapper] so that the BLite source
// generator produces a TestProducts_Mappers.TestProduct_BLite_Client_IntegrationTests_Infrastructure_TestProductMapper
// (or similar) for use in TypedCollectionTests and QueryableTests.

using BLite.Bson;

namespace BLite.Client.IntegrationTests.Infrastructure;

/// <summary>
/// Test entity used by TypedCollectionTests and QueryableTests.
/// The source generator produces a mapper from the [DocumentMapper] attribute.
/// </summary>
[DocumentMapper("test_products")]
public class TestProduct
{
    [BsonId]
    public int Id { get; set; }

    public string Name { get; set; } = string.Empty;

    public decimal Price { get; set; }

    public int Stock { get; set; }
}
