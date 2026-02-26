// BLite.Client.IntegrationTests — BLiteServerFixture
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Starts the BLite.Server in-process via WebApplicationFactory<Program> and
// exposes a shared GrpcChannel pointing to the TestServer.
// One fixture instance is created per test class (IClassFixture<T> scope).

using BLite.Client;
using BLite.Server.Auth;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;

namespace BLite.Client.IntegrationTests.Infrastructure;

/// <summary>
/// Spins up the BLite.Server in-process for integration tests.
/// Each test class gets its own fixture instance (fresh DB, fresh server).
///
/// The server's appsettings.json (copied alongside the test binary) already
/// contains a default Auth:RootKey, so we override it with a well-known test
/// key and send that key in every gRPC request.
/// </summary>
public sealed class BLiteServerFixture : IAsyncLifetime
{
    /// <summary>Root API key injected into the server for all integration tests.</summary>
    public const string RootApiKey = "test-e2e-root-key";

    private WebApplicationFactory<Program>? _factory;
    private GrpcChannel?                    _channel;
    private string                          _dbPath = string.Empty;

    // ── IAsyncLifetime ────────────────────────────────────────────────────────

    public async Task InitializeAsync()
    {
        _dbPath = Path.Combine(
            Path.GetTempPath(),
            $"blite_e2e_{Guid.NewGuid():N}.db");

        // Environment variables always win over appsettings.json in ASP.NET Core.
        // Use __ (double underscore) as the hierarchy separator.
        // Safe to set process-wide because there is exactly one fixture instance
        // per test run (ICollectionFixture<BLiteServerFixture>).
        Environment.SetEnvironmentVariable("Auth__RootKey",             RootApiKey);
        Environment.SetEnvironmentVariable("BLiteServer__DatabasePath", _dbPath);
        Environment.SetEnvironmentVariable("Telemetry__Enabled",        "false");

        // Override the Kestrel multi-port configuration so that the TestServer
        // does not apply RequireHost() constraints to REST routes.
        // When Rest:Url and Studio:Url are empty, portsAreSeparate == false
        // and MapBliteRestApi() registers routes without any host filter.
        _factory = new WebApplicationFactory<Program>()
            .WithWebHostBuilder(b =>
            {
                b.UseSetting("Kestrel:Endpoints:Rest:Url",    "");
                b.UseSetting("Kestrel:Endpoints:Studio:Url",  "");
            });

        // Touch CreateClient() to trigger TestServer startup (runs Program.cs).
        _ = _factory.CreateClient();

        // Bootstrap: seed the root user into UserRepository while still in dev
        // mode (no users yet).  Once any AdminTest creates the first real user
        // the server exits dev mode; without this call the root API key would
        // be unknown and every subsequent gRPC call would return Unauthenticated.
        var userRepo = _factory.Services.GetRequiredService<UserRepository>();
        await userRepo.EnsureRootAsync(RootApiKey);

        // Allow HTTP/2 over plain text (h2c) — required for the in-process
        // TestServer which does not use TLS.
        AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

        // Build a gRPC channel that routes through the in-process TestServer.
        // HttpHandler = TestServer.CreateHandler() bypasses TCP entirely.
        var handler = _factory.Server.CreateHandler();
        _channel = GrpcChannel.ForAddress("http://localhost", new GrpcChannelOptions
        {
            HttpHandler = handler
        });
    }

    public async Task DisposeAsync()
    {
        _channel?.Dispose();

        if (_factory is not null)
            await _factory.DisposeAsync();

        // Restore environment variables to avoid leaking test config into other
        // processes that might share the same environment.
        Environment.SetEnvironmentVariable("Auth__RootKey",             null);
        Environment.SetEnvironmentVariable("BLiteServer__DatabasePath", null);
        Environment.SetEnvironmentVariable("Telemetry__Enabled",        null);

        if (File.Exists(_dbPath))       File.Delete(_dbPath);
        var wal = _dbPath + ".wal";
        if (File.Exists(wal))           File.Delete(wal);
    }

    // ── Public helpers ────────────────────────────────────────────────────────

    /// <summary>
    /// Creates a <see cref="BLiteClient"/> connected to the in-process server.
    /// The client does NOT own the underlying channel; the fixture manages its
    /// lifetime.  Use <c>await using</c> to ensure proper disposal of the client.
    /// </summary>
    /// <summary>
    /// Creates a <see cref="BLiteClient"/> connected to the in-process server.
    /// In dev mode any (or empty) API key is accepted; pass one only when a
    /// specific identity is needed for permission tests.
    /// </summary>
    public BLiteClient CreateClient(string? apiKey = null)
        => new BLiteClient(_channel!, apiKey ?? RootApiKey);

    /// <summary>
    /// Creates an <see cref="HttpClient"/> routed through the in-process TestServer,
    /// pre-configured with the root API key header so REST endpoints pass auth.
    /// </summary>
    public HttpClient CreateHttpClient(string? apiKey = null)
    {
        var client = _factory!.CreateClient();
        client.DefaultRequestHeaders.Add("x-api-key", apiKey ?? RootApiKey);
        return client;
    }
}
