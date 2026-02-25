// BLite.Server — ASP.NET Core gRPC host
// Copyright (C) 2026 Luca Fabbri
// Licensed under the GNU Affero General Public License v3.0 (AGPL-3.0)
// See LICENSE in the repository root for full license text.

using BLite.Core;
using BLite.Core.Storage;
using BLite.Server;
using BLite.Server.Auth;
using BLite.Server.Components;
using BLite.Server.Rest;
using BLite.Server.Services;
using BLite.Server.Caching;
using BLite.Server.Studio;
using BLite.Server.Telemetry;
using BLite.Server.Transactions;
using Microsoft.AspNetCore.Authentication;
using Microsoft.OpenApi;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Scalar.AspNetCore;

var builder = WebApplication.CreateBuilder(args);

// ── Configuration ─────────────────────────────────────────────────────────────
var serverConfig = builder.Configuration.GetSection("BLiteServer");
var dbPath = serverConfig.GetValue<string>("DatabasePath") ?? "blite.db";
var databasesDir = serverConfig.GetValue<string>("DatabasesDirectory") ?? "data/tenants";
var pageSizeBytes = serverConfig.GetValue<int>("MaxPageSizeBytes");
if (pageSizeBytes <= 0) pageSizeBytes = 16384;
var pageConfig = new PageFileConfig
{
    PageSize = pageSizeBytes,
    GrowthBlockSize = Math.Max(1024 * 1024, pageSizeBytes * 64),
    Access = System.IO.MemoryMappedFiles.MemoryMappedFileAccess.ReadWrite
};
var otlpEndpoint = builder.Configuration.GetValue<string>("Telemetry:Otlp:Endpoint");
var telemetryOn = builder.Configuration.GetValue<bool?>("Telemetry:Enabled") ?? true;
var consoleTelem = builder.Configuration.GetValue<bool?>("Telemetry:Console") ?? builder.Environment.IsDevelopment();

// ── Services ──────────────────────────────────────────────────────────────────

// SetupService — first-run wizard state (singleton, no DI deps so we create it early)
var setupService = new SetupService(builder.Configuration);
setupService.Load();
builder.Services.AddSingleton(setupService);

// EngineRegistry — manages system + tenant engines
// The system engine hosts the _users collection; tenant engines live in DatabasesDirectory.
var systemEngine = new BLiteEngine(dbPath, pageConfig);
var engineRegistry = new EngineRegistry(systemEngine, dbPath, databasesDir, pageConfig);
engineRegistry.ScanDirectory();
builder.Services.AddSingleton(engineRegistry);

// Auth pipeline
builder.Services.AddSingleton<UserRepository>(sp =>
    new UserRepository(sp.GetRequiredService<EngineRegistry>().SystemEngine));
builder.Services.AddSingleton<ApiKeyValidator>();
builder.Services.AddSingleton<AuthorizationService>();

// REST API
builder.Services.AddScoped<RestAuthFilter>();

// OpenAPI / Scalar UI
builder.Services.AddOpenApi(opts =>
{
    opts.AddDocumentTransformer((doc, _, _) =>
    {
        doc.Info = new OpenApiInfo
        {
            Title = "BLite REST API",
            Version = "v1",
            Description = "HTTP management API for BLite Server. " +
                          "Authenticate with the `x-api-key` header or `Authorization: Bearer <key>`.",
        };

        doc.Components ??= new OpenApiComponents();
        doc.Components.SecuritySchemes = new Dictionary<string, IOpenApiSecurityScheme>
        {
            ["ApiKey"] = new OpenApiSecurityScheme
            {
                Type = SecuritySchemeType.ApiKey,
                In = ParameterLocation.Header,
                Name = "x-api-key",
                Description = "BLite API key (header: `x-api-key`)",
            },
            ["Bearer"] = new OpenApiSecurityScheme
            {
                Type = SecuritySchemeType.Http,
                Scheme = "bearer",
                Description = "BLite API key as Bearer token (`Authorization: Bearer <key>`)",
            },
        };

        return Task.CompletedTask;
    });
});

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
            if (consoleTelem) t.AddConsoleExporter();
            if (!string.IsNullOrWhiteSpace(otlpEndpoint))
                t.AddOtlpExporter(o =>
                {
                    o.Protocol = OpenTelemetry.Exporter.OtlpExportProtocol.HttpProtobuf;
                    o.Endpoint = new Uri(otlpEndpoint + "/v1/traces");
                });
        })
        .WithMetrics(m =>
        {
            m.AddAspNetCoreInstrumentation()
             .AddRuntimeInstrumentation()
             .AddMeter(BLiteMetrics.ServiceName);
            if (consoleTelem) m.AddConsoleExporter();
            if (!string.IsNullOrWhiteSpace(otlpEndpoint))
                m.AddOtlpExporter(o =>
                {
                    o.Protocol = OpenTelemetry.Exporter.OtlpExportProtocol.HttpProtobuf;
                    o.Endpoint = new Uri(otlpEndpoint + "/v1/metrics");
                });
        });
}

// gRPC
builder.Services.AddGrpc(options =>
{
    options.Interceptors.Add<TelemetryInterceptor>();
    options.EnableDetailedErrors = builder.Environment.IsDevelopment();
    options.MaxReceiveMessageSize = 16 * 1024 * 1024; // 16 MB
    options.MaxSendMessageSize = 16 * 1024 * 1024;
});

builder.Services.AddGrpcReflection(); // enable grpcurl introspection in Development

// Query cache (optional — disabled by default, enabled via QueryCache:Enabled)
builder.Services.Configure<QueryCacheOptions>(
    builder.Configuration.GetSection("QueryCache"));
builder.Services.AddMemoryCache(opts =>
{
    var maxBytes = builder.Configuration.GetValue<long?>("QueryCache:MaxSizeBytes");
    if (maxBytes > 0) opts.SizeLimit = maxBytes;
});
builder.Services.AddSingleton<QueryCacheService>();

// ── Studio (Blazor Server on separate port) ──────────────────────────────────
var studioEnabled = builder.Configuration.GetValue<bool?>("Studio:Enabled") ?? false;
if (studioEnabled)
{
    builder.Services.AddRazorComponents()
        .AddInteractiveServerComponents();
    builder.Services.AddCascadingAuthenticationState();
    builder.Services.AddScoped<StudioService>();
    builder.Services.AddScoped<StudioSession>();
    builder.Services.AddSingleton<SignInTokenCache>();
    // Cookie authentication for the Studio UI
    builder.Services.AddAuthentication("StudioCookie")
        .AddCookie("StudioCookie", opts =>
        {
            opts.LoginPath        = "/login";
            opts.Cookie.Name      = "blite_studio";
            opts.Cookie.HttpOnly  = true;
            opts.Cookie.SameSite  = SameSiteMode.Lax;
            opts.ExpireTimeSpan   = TimeSpan.FromDays(7);
            opts.SlidingExpiration = true;
        });
}

// ── Build & configure pipeline ───────────────────────────────────────────────
var app = builder.Build();

// ── Host filters: bind REST and Studio to their dedicated Kestrel endpoints ──
// Extract "*:<port>" patterns from the Kestrel endpoint URLs so RequireHost()
// can pin each route group to the correct TCP port.
static string? HostFilter(string? url)
{
    if (string.IsNullOrEmpty(url)) return null;
    try
    {
        var uri = new Uri(url.Replace("*", "localhost").Replace("+", "localhost"));
        return $"*:{uri.Port}";
    }
    catch { return null; }
}

var restHostFilter   = HostFilter(app.Configuration["Kestrel:Endpoints:Rest:Url"]);
var studioHostFilter = HostFilter(app.Configuration["Kestrel:Endpoints:Studio:Url"]);
// Apply filters only when the two endpoints have actually different ports.
bool portsAreSeparate = restHostFilter is not null
                     && studioHostFilter is not null
                     && restHostFilter != studioHostFilter;

// ── Bootstrap: load users from DB, ensure root exists ────────────────────────
var userRepo = app.Services.GetRequiredService<UserRepository>();
await userRepo.LoadAllAsync();

if (setupService.IsSetupComplete)
{
    // Root user was already created via the setup wizard — nothing to do.
    app.Logger.LogInformation("Server setup complete. Root key is managed in the user database.");
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

// Cookie auth for Studio UI
if (studioEnabled)
    app.UseAuthentication();

// Antiforgery — required by Blazor Server
if (studioEnabled)
    app.UseAntiforgery();

// gRPC service endpoints
app.MapGrpcService<DynamicServiceImpl>();
app.MapGrpcService<DocumentServiceImpl>();
app.MapGrpcService<MetadataServiceImpl>();
app.MapGrpcService<AdminServiceImpl>();
app.MapGrpcService<TransactionServiceImpl>();

// REST API endpoints (/api/v1/...)
app.MapBliteRestApi(portsAreSeparate ? restHostFilter : null);

// OpenAPI spec + Scalar interactive UI — follow the REST port
var openApi = app.MapOpenApi();
var scalar  = app.MapScalarApiReference(opts =>
{
    opts.Title = "BLite REST API";
    opts.OpenApiRoutePattern = "/openapi/v1.json";
    opts.DefaultHttpClient = new(ScalarTarget.Shell, ScalarClient.Curl);
});
if (portsAreSeparate)
{
    openApi.RequireHost(restHostFilter!);
    scalar.RequireHost(restHostFilter!);
}

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
    // Sign-in: Blazor validates the key, issues a one-time token, and redirects here.
    // This is a real HTTP request, so HttpContext.SignInAsync works fine.
    var signIn = app.MapGet("/studio/sign-in", async (HttpContext ctx, string? t, SignInTokenCache cache) =>
    {
        if (string.IsNullOrWhiteSpace(t))
            return Results.Redirect("/login");

        var username = cache.Consume(t);
        if (username is null)
            return Results.Redirect("/login");

        var claims    = new[] { new System.Security.Claims.Claim(System.Security.Claims.ClaimTypes.Name, username) };
        var identity  = new System.Security.Claims.ClaimsIdentity(claims, "StudioCookie");
        var principal = new System.Security.Claims.ClaimsPrincipal(identity);
        await ctx.SignInAsync("StudioCookie", principal);
        return Results.Redirect("/");
    });

    // Sign-out: clears the cookie and returns to the login page.
    var signOut = app.MapGet("/studio/logout", async (HttpContext ctx) =>
    {
        await ctx.SignOutAsync("StudioCookie");
        return Results.Redirect("/login");
    });

    var blazor = app.MapRazorComponents<App>()
        .AddInteractiveServerRenderMode();

    // When ports are separated, pin every Blazor/Studio route to the Studio port.
    if (portsAreSeparate)
    {
        signIn.RequireHost(studioHostFilter!);
        signOut.RequireHost(studioHostFilter!);
        blazor.RequireHost(studioHostFilter!);
    }

    app.Logger.LogInformation(
        "BLite Studio enabled — open the Kestrel 'Studio' endpoint in a browser.");
}

app.Run();

// Expose Program to allow WebApplicationFactory<Program> from the integration test assembly.
public partial class Program { }
