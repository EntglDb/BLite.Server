// BLite.Sample — Product-Catalog REST API
// Demonstrates BLite.Client usage against a local BLite Server instance.
//
// Pre-requisites:
//   1. A running BLite Server (dotnet run --project src/BLite.Server)
//   2. A valid API key in appsettings.Development.json  →  BLite:ApiKey
//
// Endpoints (all documented in Scalar at /scalar/v1):
//   POST   /products           — insert a new product
//   GET    /products           — list products (optional ?limit=N, default 50)
//   GET    /products/{id}      — find by ObjectId hex
//   PUT    /products/{id}      — update an existing product (partial)
//   DELETE /products/{id}      — delete a product
//   POST   /products/search    — server-side filter / sort / paging

using System.Text.Json;
using System.Text.Json.Nodes;
using BLite.Bson;
using BLite.Client;
using BLite.Proto;
using Microsoft.AspNetCore.Http.HttpResults;
using Scalar.AspNetCore;

// ── Builder ───────────────────────────────────────────────────────────────────

var builder = WebApplication.CreateBuilder(args);

// BLiteClient is a singleton — reuse the GrpcChannel across all requests.
builder.Services.AddSingleton(sp =>
{
    var opts = builder.Configuration
        .GetSection("BLite")
        .Get<BLiteClientOptions>()
        ?? throw new InvalidOperationException(
            "Missing 'BLite' configuration section in appsettings.json. " +
            "Set Host, Port, ApiKey and UseTls.");
    return new BLiteClient(opts);
});

builder.Services.AddOpenApi();

// ── App ───────────────────────────────────────────────────────────────────────

var app = builder.Build();

app.MapOpenApi();                          // /openapi/v1.json
app.MapScalarApiReference(options =>
{
    options.Title = "BLite Sample — Products API";
});                                        // /scalar/v1

// ── Internal constants ────────────────────────────────────────────────────────

const string Collection = "products";
string[] ProductFields = ["name", "category", "price", "stock", "active", "createdAt"];

// ── Mapping helpers ───────────────────────────────────────────────────────────

static bool TryParseObjectId(string hex, out BsonId id)
{
    if (hex.Length == 24)
    {
        try
        {
            id = new BsonId(new ObjectId(Convert.FromHexString(hex)));
            return true;
        }
        catch { /* invalid bytes */ }
    }
    id = default;
    return false;
}

static ProductResponse? DocToProduct(BsonDocument doc)
{
    if (!doc.TryGetId(out var bsonId)) return null;
    doc.TryGetString("name",     out var name);
    doc.TryGetString("category", out var category);
    var price     = doc.TryGetValue("price",     out var pv) ? pv.AsDouble   : 0d;
    var stock     = doc.TryGetInt32("stock",     out var sv) ? sv            : 0;
    var active    = doc.TryGetValue("active",    out var av) ? av.AsBoolean  : true;
    var createdAt = doc.TryGetValue("createdAt", out var dv) ? dv.AsDateTime : DateTime.MinValue;
    return new ProductResponse(bsonId.ToString(), name ?? "", category ?? "", price, stock, active, createdAt);
}

static ScalarValue ToScalarValue(JsonNode? json)
{
    if (json is not JsonValue jv) return ScalarValue.Null();
    var elem = jv.GetValue<JsonElement>();
    return elem.ValueKind switch
    {
        JsonValueKind.True   => ScalarValue.From(true),
        JsonValueKind.False  => ScalarValue.From(false),
        JsonValueKind.Number => elem.TryGetInt32(out var i32) ? ScalarValue.From(i32) :
                                elem.TryGetInt64(out var i64) ? ScalarValue.From(i64) :
                                ScalarValue.From(elem.GetDouble()),
        JsonValueKind.String => ScalarValue.From(elem.GetString()!),
        _                    => ScalarValue.Null()
    };
}

static FilterOp ParseOp(string op) => op.ToLowerInvariant() switch
{
    "eq"                    => FilterOp.Eq,
    "neq" or "ne"           => FilterOp.NotEq,
    "lt"                    => FilterOp.Lt,
    "lte" or "lteq"         => FilterOp.LtEq,
    "gt"                    => FilterOp.Gt,
    "gte" or "gteq"         => FilterOp.GtEq,
    "startswith"            => FilterOp.StartsWith,
    "contains"              => FilterOp.Contains,
    _ => throw new ArgumentException(
        $"Unknown op '{op}'. Valid: eq / neq / lt / lte / gt / gte / startsWith / contains.")
};

// ── POST /products ────────────────────────────────────────────────────────────

app.MapPost("/products", async Task<Created<ProductIdResponse>> (
    CreateProductRequest req,
    BLiteClient client,
    CancellationToken ct) =>
{
    var col = client.GetDynamicCollection(Collection);
    var doc = await col.NewDocumentAsync(ProductFields, b => b
        .AddString  ("name",      req.Name)
        .AddString  ("category",  req.Category)
        .AddDouble  ("price",     req.Price)
        .AddInt32   ("stock",     req.Stock)
        .AddBoolean ("active",    req.Active)
        .AddDateTime("createdAt", DateTime.UtcNow), ct);

    var id = await col.InsertAsync(doc, ct: ct);
    return TypedResults.Created($"/products/{id}", new ProductIdResponse(id.ToString()));
})
.WithName("CreateProduct")
.WithSummary("Insert a new product into the catalog.")
.WithTags("Products");

// ── GET /products ─────────────────────────────────────────────────────────────

app.MapGet("/products", async Task<Ok<List<ProductResponse>>> (
    int? limit,
    BLiteClient client,
    CancellationToken ct) =>
{
    var col = client.GetDynamicCollection(Collection);
    var descriptor = new QueryDescriptor { Take = Math.Min(limit ?? 50, 500) };
    var result = new List<ProductResponse>();
    await foreach (var doc in col.QueryAsync(descriptor, ct))
    {
        var p = DocToProduct(doc);
        if (p is not null) result.Add(p);
    }
    return TypedResults.Ok(result);
})
.WithName("ListProducts")
.WithSummary("Return up to 'limit' products (default 50, max 500).")
.WithTags("Products");

// ── GET /products/{id} ────────────────────────────────────────────────────────

app.MapGet("/products/{id}", async Task<Results<Ok<ProductResponse>, NotFound, BadRequest<string>>> (
    string id,
    BLiteClient client,
    CancellationToken ct) =>
{
    if (!TryParseObjectId(id, out var bsonId))
        return TypedResults.BadRequest("'id' must be a 24-char lowercase hex ObjectId.");

    var col = client.GetDynamicCollection(Collection);
    var doc = await col.FindByIdAsync(bsonId, ct);

    if (doc is null) return TypedResults.NotFound();
    var p = DocToProduct(doc);
    return p is not null ? TypedResults.Ok(p) : (Results<Ok<ProductResponse>, NotFound, BadRequest<string>>)TypedResults.NotFound();
})
.WithName("GetProduct")
.WithSummary("Find a product by its ObjectId.")
.WithTags("Products");

// ── PUT /products/{id} ────────────────────────────────────────────────────────

app.MapPut("/products/{id}", async Task<Results<Ok<ProductIdResponse>, NotFound, BadRequest<string>>> (
    string id,
    UpdateProductRequest req,
    BLiteClient client,
    CancellationToken ct) =>
{
    if (!TryParseObjectId(id, out var bsonId))
        return TypedResults.BadRequest("'id' must be a 24-char lowercase hex ObjectId.");

    var col = client.GetDynamicCollection(Collection);
    var doc = await col.NewDocumentAsync(ProductFields, b =>
    {
        if (req.Name     is not null) b.AddString  ("name",     req.Name);
        if (req.Category is not null) b.AddString  ("category", req.Category);
        if (req.Price    is not null) b.AddDouble  ("price",    req.Price.Value);
        if (req.Stock    is not null) b.AddInt32   ("stock",    req.Stock.Value);
        if (req.Active   is not null) b.AddBoolean ("active",   req.Active.Value);
    }, ct);

    var ok = await col.UpdateAsync(bsonId, doc, ct: ct);
    return ok
        ? TypedResults.Ok(new ProductIdResponse(id))
        : (Results<Ok<ProductIdResponse>, NotFound, BadRequest<string>>)TypedResults.NotFound();
})
.WithName("UpdateProduct")
.WithSummary("Update fields of an existing product (omit unchanged fields).")
.WithTags("Products");

// ── DELETE /products/{id} ─────────────────────────────────────────────────────

app.MapDelete("/products/{id}", async Task<Results<Ok<DeletedResponse>, NotFound, BadRequest<string>>> (
    string id,
    BLiteClient client,
    CancellationToken ct) =>
{
    if (!TryParseObjectId(id, out var bsonId))
        return TypedResults.BadRequest("'id' must be a 24-char lowercase hex ObjectId.");

    var col = client.GetDynamicCollection(Collection);
    var ok  = await col.DeleteAsync(bsonId, ct: ct);
    return ok
        ? TypedResults.Ok(new DeletedResponse(id))
        : (Results<Ok<DeletedResponse>, NotFound, BadRequest<string>>)TypedResults.NotFound();
})
.WithName("DeleteProduct")
.WithSummary("Delete a product by its ObjectId.")
.WithTags("Products");

// ── POST /products/search ─────────────────────────────────────────────────────

app.MapPost("/products/search", async Task<Results<Ok<List<ProductResponse>>, BadRequest<string>>> (
    SearchRequest req,
    BLiteClient client,
    CancellationToken ct) =>
{
    var descriptor = new QueryDescriptor
    {
        Skip = req.Skip ?? 0,
        Take = Math.Min(req.Take ?? 50, 500)
    };

    if (req.Field is not null && req.Op is not null)
    {
        FilterOp op;
        try   { op = ParseOp(req.Op); }
        catch (ArgumentException ex) { return TypedResults.BadRequest(ex.Message); }
        descriptor.Where = new BinaryFilter { Field = req.Field, Op = op, Value = ToScalarValue(req.Value) };
    }

    if (req.OrderBy is not null)
        descriptor.OrderBy.Add(new SortSpec { Field = req.OrderBy, Descending = req.Descending ?? false });

    var col = client.GetDynamicCollection(Collection);
    var result = new List<ProductResponse>();
    await foreach (var doc in col.QueryAsync(descriptor, ct))
    {
        var p = DocToProduct(doc);
        if (p is not null) result.Add(p);
    }
    return TypedResults.Ok(result);
})
.WithName("SearchProducts")
.WithSummary("Server-side filter, sort and paging.")
.WithDescription("""
    Filter a single field with one of: eq, neq, lt, lte, gt, gte, startsWith, contains.
    All fields are optional — omit filter/orderBy to list all.

    Example body:
    {
      "field":      "category",
      "op":         "eq",
      "value":      "electronics",
      "orderBy":    "price",
      "descending": false,
      "skip":       0,
      "take":       20
    }
    """)
.WithTags("Products");

// ── Run ───────────────────────────────────────────────────────────────────────

app.Run();

// ── Response models ───────────────────────────────────────────────────────────

record ProductResponse(
    string   Id,
    string   Name,
    string   Category,
    double   Price,
    int      Stock,
    bool     Active,
    DateTime CreatedAt);

record ProductIdResponse(string Id);

record DeletedResponse(string Id);

// ── Request models ────────────────────────────────────────────────────────────

record CreateProductRequest(
    string Name,
    string Category,
    double Price,
    int    Stock,
    bool   Active = true);

record UpdateProductRequest(
    string?  Name     = null,
    string?  Category = null,
    double?  Price    = null,
    int?     Stock    = null,
    bool?    Active   = null);

/// <summary>
/// Server-side search request.
/// Value accepts any JSON scalar (string, number, bool).
/// </summary>
record SearchRequest(
    string?    Field      = null,
    string?    Op         = null,
    JsonNode?  Value      = null,
    string?    OrderBy    = null,
    bool?       Descending = null,
    int?        Skip       = null,
    int?        Take       = null);

