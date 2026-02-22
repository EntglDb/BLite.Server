// BLite.Server — ASP.NET Core gRPC host
// Copyright (C) 2026 Luca Fabbri
// Licensed under the GNU Affero General Public License v3.0 (AGPL-3.0)
// See LICENSE in the repository root for full license text.

using BLite.Core;
using BLite.Server.Auth;
using BLite.Server.Services;
using BLite.Server.Telemetry;
using BLite.Server.Transactions;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using OpenTelemetry.Metrics;

var builder = WebApplication.CreateBuilder(args);

// ── Configuration ─────────────────────────────────────────────────────────────
var serverConfig = builder.Configuration.GetSection("BLiteServer");
var dbPath       = serverConfig.GetValue<string>("DatabasePath") ?? "blite.db";
var rootKey      = builder.Configuration.GetValue<string>("Auth:RootKey");
var otlpEndpoint = builder.Configuration.GetValue<string>("Telemetry:Otlp:Endpoint");
var telemetryOn  = builder.Configuration.GetValue<bool?>("Telemetry:Enabled") ?? true;
var consoleTelem = builder.Configuration.GetValue<bool?>("Telemetry:Console") ?? builder.Environment.IsDevelopment();

// ── Services ──────────────────────────────────────────────────────────────────

// BLiteEngine — single instance, owns the storage kernel
builder.Services.AddSingleton<BLiteEngine>(_ => new BLiteEngine(dbPath));

// Auth pipeline
builder.Services.AddSingleton<UserRepository>();
builder.Services.AddSingleton<ApiKeyValidator>();
builder.Services.AddSingleton<AuthorizationService>();

// Transactions
builder.Services.Configure<TransactionOptions>(
    builder.Configuration.GetSection("Transactions"));
builder.Services.AddSingleton<TransactionManager>();
builder.Services.AddHostedService<TransactionCleanupService>();

// OpenTelemetry
if (telemetryOn)
{
    var serviceName = builder.Configuration.GetValue<string>("Telemetry:ServiceName") ?? "blite-server";
    builder.Services.AddOpenTelemetry()
        .ConfigureResource(r => r.AddService(serviceName))
        .WithTracing(t =>
        {
            t.AddAspNetCoreInstrumentation()
             .AddSource(BLiteMetrics.ServiceName);
            if (consoleTelem)  t.AddConsoleExporter();
            if (!string.IsNullOrWhiteSpace(otlpEndpoint))
                t.AddOtlpExporter(o => o.Endpoint = new Uri(otlpEndpoint));
        })
        .WithMetrics(m =>
        {
            m.AddAspNetCoreInstrumentation()
             .AddRuntimeInstrumentation()
             .AddMeter(BLiteMetrics.ServiceName);
            if (consoleTelem)  m.AddConsoleExporter();
            if (!string.IsNullOrWhiteSpace(otlpEndpoint))
                m.AddOtlpExporter(o => o.Endpoint = new Uri(otlpEndpoint));
        });
}

// gRPC
builder.Services.AddGrpc(options =>
{
    options.Interceptors.Add<TelemetryInterceptor>();
    options.EnableDetailedErrors = builder.Environment.IsDevelopment();
    options.MaxReceiveMessageSize = 16 * 1024 * 1024; // 16 MB
    options.MaxSendMessageSize    = 16 * 1024 * 1024;
});

builder.Services.AddGrpcReflection(); // enable grpcurl introspection in Development

// ── Build & configure pipeline ───────────────────────────────────────────────
var app = builder.Build();

// ── Bootstrap: load users from DB, ensure root exists ────────────────────────
var userRepo = app.Services.GetRequiredService<UserRepository>();
await userRepo.LoadAllAsync();

if (!string.IsNullOrWhiteSpace(rootKey))
{
    await userRepo.EnsureRootAsync(rootKey);
    app.Logger.LogInformation("Root user bootstrapped (key from Auth:RootKey).");
}
else
{
    app.Logger.LogWarning(
        "Auth:RootKey is not set — server running in open/dev mode (all requests accepted as root).");
}

if (app.Environment.IsDevelopment())
    app.MapGrpcReflectionService();

// API key authentication — resolves caller identity for every request
app.UseMiddleware<ApiKeyMiddleware>();

// gRPC service endpoints
app.MapGrpcService<DynamicServiceImpl>();
app.MapGrpcService<DocumentServiceImpl>();
app.MapGrpcService<MetadataServiceImpl>();
app.MapGrpcService<AdminServiceImpl>();
app.MapGrpcService<TransactionServiceImpl>();

app.MapGet("/", () =>
    "BLite Server is running. Use a gRPC client to connect (see blitedb.com/docs).");

app.Run();
