# BLite.Server — AI Agent Instructions

This file describes the architecture, conventions, and rules for working on the
**BLite.Server** repository. Read it in full before making any changes.

---

## 1. Repository overview

| Repository | Role |
|---|---|
| `github.com/EntglDb/BLite` (sibling at `../BLite`) | Core storage engine (`BLite.Core`), BSON library (`BLite.Bson`), client SDK (`BLite`) |
| `github.com/EntglDb/BLite.Server` (this repo) | gRPC server, REST API, Blazor management Studio |

The server **never** copies the engine source — it references it via a local
`ProjectReference`. Do **not** modify files under `../BLite` unless explicitly asked.

### Solution projects

```
BLite.Server   — ASP.NET Core 10 host (gRPC + REST + Blazor Studio)
BLite.Proto    — Protobuf contracts + generated gRPC stubs
BLite.Core     — (sibling) storage engine, not modified here
BLite.Bson     — (sibling) BSON serialization, not modified here
BLite          — (sibling) client SDK, not modified here
```

---

## 2. Runtime ports (appsettings.json)

| Endpoint | URL | Protocol |
|---|---|---|
| gRPC | `https://*:2626` | HTTP/2 only |
| REST API | `https://*:2627` | HTTP/1.1 + HTTP/2 |
| Studio (Blazor) | `https://*:2628` | HTTP/1.1 + HTTP/2 |

REST and Studio run on dedicated ports when **both** `Kestrel:Endpoints:Rest` and
`Kestrel:Endpoints:Studio` are configured with different URLs. If only `Studio` is
configured (legacy single-port mode), all non-gRPC traffic is served on that one port
without any `RequireHost` restriction — backward-compatible with older deployments.

The Studio is enabled via `"Studio": { "Enabled": true }`. It is **off by default**
in production images.

---

## 3. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         BLite.Server                            │
│                                                                 │
│  ┌──────────────┐   ┌───────────────────┐   ┌───────────────┐  │
│  │  gRPC layer  │   │   REST /api/v1    │   │ Blazor Studio │  │
│  │  DynamicSvc  │   │   (Minimal APIs)  │   │  (port 2627)  │  │
│  │  DocumentSvc │   │   BLQL + CRUD     │   │               │  │
│  │  AdminSvc    │   │                   │   │  StudioService│  │
│  │  MetadataSvc │   │  PermissionFilter │   │  StudioSession│  │
│  │  TxnSvc      │   │  RestAuthFilter   │   │               │  │
│  └──────┬───────┘   └────────┬──────────┘   └───────┬───────┘  │
│         │                    │                       │          │
│         └──────────┬─────────┘                       │          │
│                    ▼                                  │          │
│           ┌─────────────────┐                        │          │
│           │  EngineRegistry │◄───────────────────────┘          │
│           │  (singleton)    │                                    │
│           └────────┬────────┘                                    │
│                    │  one BLiteEngine per database               │
│                    ▼                                             │
│           ┌─────────────────┐                                    │
│           │   BLiteEngine   │  ../BLite (sibling repo)          │
│           │  + Collections  │                                    │
│           └─────────────────┘                                    │
└─────────────────────────────────────────────────────────────────┘
```

### Key singletons

| Type | Role |
|---|---|
| `EngineRegistry` | Maps `database_id → BLiteEngine`. System engine (key `""`) hosts `_users`. |
| `UserRepository` | In-memory `ConcurrentDictionary` over the `_users` collection. Hot path: O(1) key hash lookup. |
| `ApiKeyValidator` | Resolves an API key to a `BLiteUser`. In dev mode (no users), returns a synthetic `DevRoot`. |
| `AuthorizationService` | Checks `BLiteOperation` flags against a user's `PermissionEntry` list. |
| `TransactionManager` | One active transaction per database at a time (enforced by a per-db `SemaphoreSlim`). |

### Key scoped services

| Type | Scope | Role |
|---|---|---|
| `RestAuthFilter` | Scoped (per request) | `IEndpointFilter` that resolves API key and stores `BLiteUser` in `HttpContext.Items`. |
| `StudioService` | Scoped (per circuit) | Blazor façade over `EngineRegistry` + `UserRepository`. |
| `StudioSession` | Scoped (per circuit) | Carries Studio authentication state for one Blazor Server circuit. |

---

## 4. Auth model

### API authentication (gRPC + REST)

Every gRPC request goes through `ApiKeyMiddleware` (reads `x-api-key` header).
Every REST request goes through `RestAuthFilter` (reads `x-api-key` or
`Authorization: Bearer <key>`). Both store the resolved `BLiteUser` in
`HttpContext.Items[nameof(BLiteUser)]`.

### Permission model

```csharp
[Flags]
public enum BLiteOperation
{
    None   = 0,
    Query  = 1,
    Insert = 2,
    Update = 4,
    Delete = 8,
    Drop   = 16,
    Admin  = 32,
    Write  = Insert | Update | Delete,
    All    = Query | Write | Drop | Admin
}
```

Each `BLiteUser` has a `IReadOnlyList<PermissionEntry>` where `PermissionEntry`
is `(string Collection, BLiteOperation Ops)`. Collection `"*"` matches any
collection.

### Namespace isolation

Users with a non-null `Namespace` field work inside a transparent prefix.
The physical collection name is `"<namespace>:<logical_name>"`.
`NamespaceResolver.Resolve(user, logicalName)` does this translation.
Always use physical names when calling the engine directly.

### Database isolation

Users with a non-null `DatabaseId` are restricted to one tenant engine.
`EngineRegistry.GetEngine(user.DatabaseId)` returns the correct engine.
`NullIfDefault(dbId)` maps the `"default"` URL sentinel to `null` (system engine).

---

## 5. REST API

### Endpoint structure

All endpoints live under `/api/v1` and are grouped in separate extension classes:

```
src/BLite.Server/Rest/
  RestApiExtensions.cs          — MapBliteRestApi() entry, shared utilities
  PermissionFilter.cs           — IEndpointFilter: auth check before handler
  RestApiDatabasesExtensions.cs — /databases
  RestApiCollectionsExtensions.cs — /{dbId}/collections
  RestApiDocumentsExtensions.cs — /{dbId}/{collection}/documents
  RestApiBlqlExtensions.cs      — /{dbId}/{collection}/query (BLQL)
  RestApiUsersExtensions.cs     — /users
  BLiteErrors.cs                — ErrorOr factory + IResult mapper
  RestAuthFilter.cs             — IEndpointFilter: API key resolution
```

### Adding a new REST endpoint

1. Add the `MapXxx(this RouteGroupBuilder g)` method in the appropriate file.
2. Add `.AddEndpointFilter(new PermissionFilter(op, collection?, checkDb:))` to the endpoint.
   - Admin-only endpoints: use group-level filter with `BLiteOperation.Admin, "*"`.
   - Collection-scoped endpoints: use per-endpoint filter with `checkDb: true`.
3. Add `.WithSummary(…).WithDescription(…)` for OpenAPI documentation.
4. Retrieve the current user inside the handler via:
   ```csharp
   var user = (BLiteUser)ctx.Items[nameof(BLiteUser)]!;
   ```
5. Map errors using `BLiteErrors.Xyz().ToResult()` or `result.ToResult(v => Results.Ok(v))`.
6. Call `MapXxx()` from `RestApiExtensions.MapBliteRestApi()`.

### PermissionFilter parameters

```csharp
new PermissionFilter(
    BLiteOperation op,       // required: operation to check
    string? collection,      // null → read from {collection} route param (fallback "*")
    bool checkDb = false     // true → validate {dbId} against user.DatabaseId
)
```

### Error handling pattern

```csharp
// Factory (BLiteErrors.cs) → returns ErrorOr<T>
return BLiteErrors.DocumentNotFound(id).ToResult();          // Error → IResult
return result.ToResult(v => Results.Created("/…", v));       // Success path
return Results.ValidationProblem(new Dictionary<string, string[]>{ … });
```

---

## 6. gRPC services

### Service implementations

```
src/BLite.Server/Services/
  BLiteServiceBase.cs       — shared helpers: GetCurrentUser(), AuthorizeWithUser()
  DynamicServiceImpl.cs     — schema-less: Insert, Update, Delete, Query, FindById, InsertBulk
  DocumentServiceImpl.cs    — typed (BSON payload): same operations + typed streaming
  AdminServiceImpl.cs       — user management, database management (Admin only)
  MetadataServiceImpl.cs    — collection/index introspection
  TransactionServiceImpl.cs — Begin, Commit, Rollback
```

All services inherit `BLiteServiceBase`. Authorization is checked via
`AuthorizeWithUser(context, collection, operation)` which:
1. Resolves the `BLiteUser` from `HttpContext.Items`
2. Calls `AuthorizationService.CheckPermission`
3. Calls `NamespaceResolver.Resolve` for the physical collection name
4. Returns `(physicalCollection, user)`

### Query execution

Both `DynamicService.Query` and `DocumentService.Query` delegate to:
```csharp
QueryDescriptorExecutor.ExecuteAsync(engine, descriptor, ct)
// returns IAsyncEnumerable<BsonDocument>
```

The `QueryDescriptor` is deserialized from `QueryDescriptorSerializer.Deserialize(bytes)`.
It contains: WHERE filter, ORDER BY, Skip, Take, SELECT projection, collection name.

### Transaction-aware writes

When `request.TransactionId` is non-empty, writes go to the transaction engine:
```csharp
var session = _txnManager.RequireSession(request.TransactionId, user);
id = await session.Engine.GetOrCreateCollection(col).InsertAsync(doc, ct);
```

---

## 7. Blazor Studio

### Component locations

```
src/BLite.Server/Components/
  App.razor                     — root, sets InteractiveServer globally
  Routes.razor                  — router, default layout = StudioLayout
  Layout/
    StudioLayout.razor          — sidebar + auth gate (checks StudioSession)
    SetupLayout.razor           — used by /setup and /login (centered card)
    StudioNavMenu.razor         — sidebar navigation links
  Pages/
    Setup.razor                 — first-run wizard (/setup)
    Login.razor                 — Studio login (/login)
    Dashboard.razor             — /
    Databases.razor             — /databases
    Collections.razor           — /collections
    Documents.razor             — /documents
    Users.razor                 — /users
```

### Studio authentication flow

1. `StudioLayout.OnInitialized` checks `Studio.IsSetupComplete` → `/setup` if false.
2. Then checks `Session.IsAuthenticated` → `/login` if false.
3. `Login.razor` calls `Studio.ValidateStudioKey(key)` which requires a valid key
   **and** `Admin` permission on `"*"`.
4. On success: `Session.Login(username)` + `Nav.NavigateTo("/")`.
5. Logout: `Session.Logout()` + navigate to `/login`.

`StudioSession` is **scoped per Blazor Server circuit** — it resets on page reload.

### StudioService

`StudioService` is the single façade for all Studio UI operations. Inject it as
`@inject StudioService Studio` in Blazor pages. Do **not** inject `EngineRegistry`
or `UserRepository` directly into components.

### CSS conventions

The Studio uses a custom dark theme defined in `wwwroot/css/studio.css`.

Key CSS variables:
```css
--bg, --bg-card, --bg-hover, --bg-input
--border, --text, --text-dim
--accent, --accent-bg
--green, --red, --orange
--radius: 6px
--mono, --sans
```

Key layout classes:
- `.studio-root` — flex row (sidebar + main)
- `.studio-sidebar` — 220 px wide, flex-column
- `.studio-main` — flex:1, scrollable
- `.doc-split` — two-column list + editor panel
- `.data-table` — standard table
- `.toolbar` — flex row of controls with `gap`
- `.panel` / `.panel-body` — collapsible card (`<details>`)
- `.btn`, `.btn-primary`, `.btn-danger`, `.btn-small` — buttons
- `.input` — text inputs and selects
- `.badge`, `.badge-active`, `.badge-idle` — status badges
- `.alert-ok`, `.alert-error` — feedback messages
- `.modal-backdrop` / `.modal` — modal dialogs
- `.source-badge` — sidebar footer links/buttons
- `.sidebar-divider` — thin `<hr>` separator in sidebar

---

## 8. Coding conventions

### Language and target

- **C# 14**, **net10.0**
- Nullable reference types enabled (`<Nullable>enable`)
- Implicit usings enabled

### Naming

- Types, methods, properties: `PascalCase`
- Private fields: `_camelCase`
- Local variables and parameters: `camelCase`
- Constants: `PascalCase`

### File-level layout

```csharp
// BLite.Server — <short description>
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// <longer description if needed>

using ...;

namespace BLite.Server.<SubNamespace>;
```

### No-noise rule

Do **not** add comments that paraphrase the code. Only add comments that explain
*why*, describe a non-obvious invariant, or serve as section separators
(e.g., `// ── Auth helpers ──────`).

### Error handling

- REST layer: use `ErrorOr` + `BLiteErrors` factory. Never throw exceptions for
  expected business errors; return `IResult` via `.ToResult()`.
- gRPC layer: throw `RpcException` for expected errors; let unhandled exceptions
  propagate to the gRPC interceptor.
- Blazor components: catch exceptions in event handlers and set an `_errorMessage`
  field displayed in the UI. Never `throw` from a Blazor event handler.

### Async

- All I/O methods must be `async Task` / `async ValueTask` with `CancellationToken`.
- Do not use `.Result` or `.Wait()`.
- Pass `CancellationToken` from the gRPC `ServerCallContext.CancellationToken` or
  the Blazor `CancellationToken` (where available).

### Dependency injection

- Prefer constructor injection.
- Singletons can only depend on other singletons.
- Scoped services (e.g. `StudioService`) can depend on singletons — not vice versa.
- Never resolve services manually with `GetRequiredService` inside application code
  (only in `Program.cs` bootstrap).

---

## 9. Configuration reference (appsettings.json)

```json
{
  "BLiteServer": {
    "DatabasePath": "blite.db",          // system database file path
    "MaxPageSizeBytes": 16384,           // BLiteEngine page size
    "DatabasesDirectory": "data/tenants" // tenant .db files
  },
  "Studio": {
    "Enabled": true                      // enable Blazor Studio + REST API
  },
  "Kestrel": {
    "Endpoints": {
      "Grpc":   { "Url": "https://*:2626", "Protocols": "Http2" },
      "Studio": { "Url": "https://*:2627", "Protocols": "Http1AndHttp2" }
    }
  },
  "Transactions": {
    "TimeoutSeconds": 60
  },
  "Telemetry": {
    "Enabled": true,
    "ServiceName": "blite-server",
    "Console": null,
    "Otlp": { "Endpoint": "https://..." }
  },
  "License": {
    "SourceUrl": "https://github.com/EntglDb/BLite.Server"
  }
}
```

---

## 10. Key invariants — never break these

1. **Never** store a plaintext API key. Keys are stored as SHA-256 hex hashes
   in `_users`. The plaintext is returned once at creation/rotation only.

2. **Always** resolve physical collection names via `NamespaceResolver.Resolve`
   before calling the engine. Storing a logical name in the engine is a data isolation bug.

3. **Always** include `dbId` in cache/cache-invalidation keys. Missing it causes
   cross-tenant data leaks.

4. **Never** access `EngineRegistry.GetEngine(null)` directly for tenant data.
   Use `GetEngine(user.DatabaseId)` so the correct engine is selected.

5. The `_users` collection lives in the **system engine only**. Never store users
   in a tenant engine, and never pass a tenant engine to `UserRepository`.

6. Collections whose names start with `_` are **reserved**. The REST layer filters
   them out from list responses. Never expose them to end users without `Admin` permission.

7. `StudioSession.IsAuthenticated` is **circuit-scoped** (in-memory only). It is
   not a substitute for API-level authentication on the gRPC/REST layer.

8. The `TransactionManager` holds a `SemaphoreSlim(1,1)` per database. Any code
   that awaits inside a transaction must eventually call `CommitAsync` or `RollbackAsync`,
   or the semaphore will never be released (server deadlock on that database).

---

## 11. Build and run

```bash
# Build
dotnet build src/BLite.Server/BLite.Server.csproj

# Run (development)
dotnet run --project src/BLite.Server

# Open Studio
# https://localhost:2627

# gRPC endpoint
# https://localhost:2626
```

After a clean start with no `server-setup.json` file, navigate to `https://localhost:2627/setup`
to create the root user.

---

## 12. Adding a new Studio page

1. Create `src/BLite.Server/Components/Pages/MyPage.razor` with `@page "/my-page"`.
2. Inject `@inject StudioService Studio` — do not inject engine/repository directly.
3. Use the existing CSS classes; do not add inline styles except for minor layout tweaks.
4. Add the navigation link in `StudioNavMenu.razor`.
5. Auth is handled by `StudioLayout` — no need to check `Session.IsAuthenticated`
   inside the page itself.

---

## 13. Adding a new `BLiteOperation` permission flag

1. Add the new flag to `BLiteOperation` in `src/BLite.Server/Auth/Permission.cs`
   (keep it a power of 2).
2. Update `AuthorizationService.CheckPermission` if the new flag needs special
   semantics (e.g., reserved-collection access).
3. Update the `_allOps` array in `Users.razor` so the Studio shows the new checkbox.
4. Update the OpenAPI description of the `PermissionRequest` DTO.
