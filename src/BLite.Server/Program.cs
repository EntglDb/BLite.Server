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

// ── Services ──────────────────────────────────────────────────────────────────

// BLiteEngine is the single entry point into the storage kernel.
// Registered as Singleton — one engine instance per server process.
builder.Services.AddSingleton<BLiteEngine>(_ => new BLiteEngine(dbPath));

// gRPC
builder.Services.AddGrpc(options =>
{
    options.EnableDetailedErrors = builder.Environment.IsDevelopment();
    options.MaxReceiveMessageSize = 16 * 1024 * 1024; // 16 MB
    options.MaxSendMessageSize    = 16 * 1024 * 1024;
});

// API Key authentication middleware
builder.Services.AddSingleton<ApiKeyValidator>(sp =>
{
    var keys = builder.Configuration
        .GetSection("Auth:ApiKeys")
        .Get<List<string>>() ?? [];
    return new ApiKeyValidator(keys);
});

builder.Services.AddGrpcReflection(); // enable grpcurl introspection in Development

// ── Build & configure pipeline ───────────────────────────────────────────────
var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapGrpcReflectionService();
}

// API key check applied to all gRPC endpoints
app.UseMiddleware<ApiKeyMiddleware>();

// gRPC service endpoints
app.MapGrpcService<DynamicServiceImpl>();
app.MapGrpcService<DocumentServiceImpl>();

app.MapGet("/", () =>
    "BLite Server is running. Use a gRPC client to connect (see blitedb.com/docs).");

app.Run();
