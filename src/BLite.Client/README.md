# BLite.Client

Official .NET SDK for BLite Server over gRPC.

## What You Get

- Typed collections (`IDocumentCollection<TId, T>`)
- Dynamic BSON collections
- LINQ query push-down
- Transactions
- Key-value API
- Admin API (users and tenants)

## Install

```bash
dotnet add package BLite.Client
```

## 1. Connect To BLite Server

```csharp
using BLite.Client;

await using var client = new BLiteClient(new BLiteClientOptions
{
    Host = "localhost",
    Port = 2626,
    ApiKey = "your-api-key",
    UseTls = false
});
```

`Port` must point to the gRPC endpoint (`2626` by default).

## 2. Typed Collections (Recommended)

```csharp
using BLite.Bson;

[DocumentMapper("users")]
public class User
{
    [BsonId]
    public ObjectId Id { get; set; }

    public string Name { get; set; } = string.Empty;
    public int Age { get; set; }
}
```

Use the generated mapper (for example `UserMapper`) to get a typed collection:

```csharp
var users = client.GetDocumentCollection<ObjectId, User>(new UserMapper());

await users.InsertAsync(new User
{
    Id = ObjectId.NewObjectId(),
    Name = "Alice",
    Age = 30
});

var adults = await users.AsQueryable()
    .Where(x => x.Age >= 18)
    .OrderBy(x => x.Name)
    .ToListAsync();
```

## 3. Dynamic BSON Collections

```csharp
using BLite.Bson;
using BLite.Proto;

var sensors = client.GetDynamicCollection("sensors");

var doc = await sensors.NewDocumentAsync(["name", "temp"], b => b
    .AddString("name", "room-1")
    .AddDouble("temp", 21.5));

_ = await sensors.InsertAsync(doc);

var q = new QueryDescriptor
{
    Collection = "sensors",
    Where = new BinaryFilter
    {
        Field = "temp",
        Op = FilterOp.Gt,
        Value = ScalarValue.From(20)
    }
};

await foreach (var item in sensors.QueryAsync(q))
{
    // process item
}
```

## 4. Transactions

```csharp
await using var tx = await client.BeginTransactionAsync();

await users.InsertAsync(new User
{
    Id = ObjectId.NewObjectId(),
    Name = "Bob",
    Age = 41
}, tx);

await tx.CommitAsync();
```

## 5. Key-Value API

```csharp
using System.Text;

await client.Kv.SetAsync("session:abc", Encoding.UTF8.GetBytes("payload"));
var value = await client.Kv.GetAsync("session:abc");
```

## 6. Admin API

```csharp
using BLite.Core.Query;

var createdKey = await client.Admin.CreateUserAsync(
    username: "reporting",
    namespaceName: null,
    databaseId: null,
    permissions:
    [
        new UserPermission
        {
            Collection = "reports",
            Ops = BLiteOperation.Query
        }
    ]);
```

## Notes

- BLite Server must be reachable on gRPC endpoint.
- In production prefer `UseTls = true` and secure secret handling for API keys.
- Unsupported LINQ operators are applied client-side after streaming.

## Links

- BLite Server: https://github.com/EntglDb/BLite.Server
- BLite engine: https://github.com/EntglDb/BLite

