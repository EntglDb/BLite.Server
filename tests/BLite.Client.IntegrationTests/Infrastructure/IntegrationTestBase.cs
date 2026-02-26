// BLite.Client.IntegrationTests — IntegrationTestBase
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

using BLite.Client;
using BLite.Client.IntegrationTests.Infrastructure;

namespace BLite.Client.IntegrationTests;

/// <summary>
/// Base class for all BLite client integration tests.
/// Concrete test classes must be annotated with <c>[Collection("Integration")]</c>
/// so that xUnit injects the shared <see cref="BLiteServerFixture"/> defined in
/// <see cref="IntegrationCollection"/>.
/// </summary>
public abstract class IntegrationTestBase
{
    protected readonly BLiteServerFixture Fixture;

    protected IntegrationTestBase(BLiteServerFixture fixture)
    {
        Fixture = fixture;
    }

    /// <summary>
    /// Creates a <see cref="BLiteClient"/> authenticated as root.
    /// Always <c>await using</c> to release gRPC resources when the test ends.
    /// </summary>
    protected BLiteClient CreateClient(string? apiKey = null)
        => Fixture.CreateClient(apiKey);

    /// <summary>
    /// Returns a unique collection name so each test works in isolation
    /// even inside the same fixture instance.
    /// </summary>
    protected static string UniqueCollection()
        => $"test_{Guid.NewGuid():N}";
}
