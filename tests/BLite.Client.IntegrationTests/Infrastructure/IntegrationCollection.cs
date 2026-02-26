// BLite.Client.IntegrationTests — IntegrationCollection
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

namespace BLite.Client.IntegrationTests.Infrastructure;

/// <summary>
/// Declares an xUnit test collection that shares a single
/// <see cref="BLiteServerFixture"/> across all integration test classes.
///
/// All test classes that are part of this collection run sequentially
/// against the same in-process server instance and the same temp database.
/// Isolation is achieved at the collection level, not the class level:
/// every test creates its own unique collection name via
/// <see cref="IntegrationTestBase.UniqueCollection()"/>.
/// </summary>
[CollectionDefinition("Integration")]
public sealed class IntegrationCollection : ICollectionFixture<BLiteServerFixture> { }
