// BLite.Server — ASP.NET Core gRPC host
// Copyright (C) 2026 Luca Fabbri
// Licensed under the GNU Affero General Public License v3.0 (AGPL-3.0)
// See LICENSE in the repository root for full license text.

using BLite.Core;
using BLite.Server.Auth;
using BLite.Server.Services;

var builder = WebApplication.CreateBuilder(args);

// ── Configuration ─────────────────────────────────────────────────────────────
var serverConfig = builder.Configuration.GetSection("BLiteServer");
var dbPath       = serverConfig.GetValue<string>("DatabasePath") ?? "blite.db";
var rootKey      = builder.Configuration.GetValue<string>("Auth:RootKey");

// ── Services ──────────────────────────────────────────────────────────────────

// BLiteEngine — single instance, owns the storage kernel
builder.Services.AddSingleton<BLiteEngine>(_ => new BLiteEngine(dbPath));

// Auth pipeline
builder.Services.AddSingleton<UserRepository>();
builder.Services.AddSingleton<ApiKeyValidator>();
builder.Services.AddSingleton<AuthorizationService>();

// gRPC
builder.Services.AddGrpc(options =>
{
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
app.MapGrpcService<AdminServiceImpl>();

app.MapGet("/", () =>
    "BLite Server is running. Use a gRPC client to connect (see blitedb.com/docs).");

app.Run();
