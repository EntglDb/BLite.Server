// BLite.Server — ASP.NET Core gRPC host
// Copyright (C) 2026 Luca Fabbri
// Licensed under the GNU Affero General Public License v3.0 (AGPL-3.0)
// See LICENSE in the repository root for full license text.

using BLite.Core;
using BLite.Core.Storage;
using BLite.Server;
using BLite.Server.Auth;
using BLite.Server.Components;
using BLite.Server.Services;
using BLite.Server.Studio;
using BLite.Server.Telemetry;
using BLite.Server.Transactions;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using OpenTelemetry.Metrics;

var builder = WebApplication.CreateBuilder(args);

// ── Configuration ─────────────────────────────────────────────────────────────
var serverConfig  = builder.Configuration.GetSection("BLiteServer");
var dbPath        = serverConfig.GetValue<string>("DatabasePath") ?? "blite.db";
var databasesDir  = serverConfig.GetValue<string>("DatabasesDirectory") ?? "data/tenants";
var pageSizeBytes = serverConfig.GetValue<int>("MaxPageSizeBytes");
if (pageSizeBytes <= 0) pageSizeBytes = 16384;
var pageConfig = new PageFileConfig
{
    PageSize        = pageSizeBytes,
    InitialFileSize = Math.Max(2 * 1024 * 1024, (long)pageSizeBytes * 128),
    Access          = System.IO.MemoryMappedFiles.MemoryMappedFileAccess.ReadWrite
};
var rootKey      = builder.Configuration.GetValue<string>("Auth:RootKey");
var otlpEndpoint = builder.Configuration.GetValue<string>("Telemetry:Otlp:Endpoint");
var telemetryOn  = builder.Configuration.GetValue<bool?>("Telemetry:Enabled") ?? true;
var consoleTelem = builder.Configuration.GetValue<bool?>("Telemetry:Console") ?? builder.Environment.IsDevelopment();

// ── Services ──────────────────────────────────────────────────────────────────

// SetupService — first-run wizard state (singleton, no DI deps so we create it early)
var setupService = new SetupService(builder.Configuration);
setupService.Load();
builder.Services.AddSingleton(setupService);

// EngineRegistry — manages system + tenant engines
// The system engine hosts the _users collection; tenant engines live in DatabasesDirectory.
var systemEngine   = new BLiteEngine(dbPath, pageConfig);
var engineRegistry = new EngineRegistry(systemEngine, databasesDir, pageConfig);
engineRegistry.ScanDirectory();
builder.Services.AddSingleton(engineRegistry);

// Auth pipeline
builder.Services.AddSingleton<UserRepository>(sp =>
    new UserRepository(sp.GetRequiredService<EngineRegistry>().SystemEngine));
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

// ── Studio (Blazor Server on separate port) ──────────────────────────────────
var studioEnabled = builder.Configuration.GetValue<bool?>("Studio:Enabled") ?? false;
if (studioEnabled)
{
    builder.Services.AddRazorComponents()
        .AddInteractiveServerComponents();
    builder.Services.AddScoped<StudioService>();
}

// ── Build & configure pipeline ───────────────────────────────────────────────
var app = builder.Build();

// ── Bootstrap: load users from DB, ensure root exists ────────────────────────
var userRepo = app.Services.GetRequiredService<UserRepository>();
await userRepo.LoadAllAsync();

if (setupService.IsSetupComplete)
{
    // Root user was already created via the setup wizard — nothing to do.
    app.Logger.LogInformation("Server setup complete. Root key is managed in the user database.");
}
else if (!string.IsNullOrWhiteSpace(rootKey))
{
    await userRepo.EnsureRootAsync(rootKey);
    app.Logger.LogInformation("Root user bootstrapped from appsettings Auth:RootKey.");
}
else
{
    app.Logger.LogWarning(
        "Auth:RootKey is not configured and setup is not complete. " +
        "Open the Studio at the configured port and complete the setup wizard.");
}

if (app.Environment.IsDevelopment())
    app.MapGrpcReflectionService();

// Static files — must precede endpoint mapping (only needed for Studio CSS/JS)
if (studioEnabled)
    app.UseStaticFiles();

// API key authentication — only for gRPC requests; Studio uses its own pipeline
app.UseWhen(
    ctx => ctx.Request.ContentType?.StartsWith("application/grpc") == true,
    branch => branch.UseMiddleware<ApiKeyMiddleware>());

// Antiforgery — required by Blazor Server
if (studioEnabled)
    app.UseAntiforgery();

// gRPC service endpoints
app.MapGrpcService<DynamicServiceImpl>();
app.MapGrpcService<DocumentServiceImpl>();
app.MapGrpcService<MetadataServiceImpl>();
app.MapGrpcService<AdminServiceImpl>();
app.MapGrpcService<TransactionServiceImpl>();

// Health-check endpoint (available on all ports)
if (!studioEnabled)
{
    app.MapGet("/", () =>
        "BLite Server is running. Use a gRPC client to connect (see blitedb.com/docs).");
}

// AGPL-3.0 §13 — network use disclosure: expose source URL on all ports
var sourceUrl = app.Configuration.GetValue<string>("License:SourceUrl")
                ?? "https://github.com/blitedb/BLite.Server";
app.MapGet("/source", () => Results.Redirect(sourceUrl, permanent: false))
   .ExcludeFromDescription();
app.MapGet("/.well-known/source", () => Results.Json(new
   { source = sourceUrl, license = "AGPL-3.0" }))
   .ExcludeFromDescription();

// ── Studio: Blazor Server endpoints ──────────────────────────────────────────
if (studioEnabled)
{
    app.MapRazorComponents<App>()
        .AddInteractiveServerRenderMode();
    app.Logger.LogInformation(
        "BLite Studio enabled — open the Kestrel 'Studio' endpoint in a browser.");
}

app.Run();

// Expose Program to allow WebApplicationFactory<Program> from the integration test assembly.
public partial class Program { }
