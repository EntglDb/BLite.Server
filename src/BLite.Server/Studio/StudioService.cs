// BLite.Server — StudioService (Blazor Server façade)
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// In-process façade over EngineRegistry + UserRepository.
// Injected as Scoped into Blazor components — no gRPC hop needed.

using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO.Compression;
using System.Text.Json;
using BLite.Bson;
using BLite.Core;
using BLite.Core.Indexing;
using BLite.Core.Query.Blql;
using BLite.Core.Storage;
using BLite.Core.Text;
using BLite.Server.Auth;
using BLite.Server.Embedding;

namespace BLite.Server.Studio;

/// <summary>
/// Provides all operations the Studio UI needs, calling the BLite services
/// directly in-process (no gRPC round-trip).
/// </summary>
public sealed class StudioService
{
    private readonly EngineRegistry      _registry;
    private readonly UserRepository      _users;
    private readonly SetupService        _setup;
    private readonly ApiKeyValidator     _validator;
    private readonly AuthorizationService _authz;
    private readonly EmbeddingService    _embedding;
    private readonly string _sourceUrl;

    public StudioService(EngineRegistry registry, UserRepository users,
        SetupService setup, ApiKeyValidator validator,
        AuthorizationService authz, EmbeddingService embedding, IConfiguration config)
    {
        _registry  = registry;
        _users     = users;
        _setup     = setup;
        _validator = validator;
        _authz     = authz;
        _embedding = embedding;
        _sourceUrl = config.GetValue<string>("License:SourceUrl")
                     ?? "https://github.com/blitedb/BLite.Server";
    }

    /// <summary>True once the setup wizard has been completed.</summary>
    public bool IsSetupComplete => _setup.IsSetupComplete;

    /// <summary>
    /// Validates an API key and returns the resolved user if the key is valid,
    /// the user is active, and the user has Admin access.
    /// Returns <c>null</c> on any failure.
    /// </summary>
    public BLiteUser? ValidateStudioKey(string key)
    {
        var user = _validator.Resolve(key);
        if (user is null || !user.Active) return null;
        return _authz.CheckPermission(user, "*", BLiteOperation.Admin) ? user : null;
    }

    /// <summary>
    /// Returns the URL where the complete corresponding source code can be obtained,
    /// as required by AGPL-3.0 §13 (network use disclosure).
    /// </summary>
    public string GetSourceUrl() => _sourceUrl;

    // ── Server info ───────────────────────────────────────────────────────────

    public ServerInfo GetServerInfo() => new(
        Version:       typeof(BLiteEngine).Assembly.GetName().Version?.ToString() ?? "0.0.0",
        Uptime:        DateTime.UtcNow - Process.GetCurrentProcess().StartTime.ToUniversalTime(),
        TenantCount:   _registry.ListTenants().Count,
        UserCount:     _users.ListAll().Count,
        DatabasesDir:  _registry.DatabasesDirectory,
        SourceUrl:     _sourceUrl);

    // ── Tenants ───────────────────────────────────────────────────────────────

    public IReadOnlyList<TenantEntry> ListTenants() => _registry.ListTenants();

    public async Task ProvisionAsync(string databaseId)
        => await _registry.ProvisionAsync(databaseId);

    public async Task DeprovisionAsync(string databaseId, bool deleteFiles)
        => await _registry.DeprovisionAsync(databaseId, deleteFiles);

    /// <summary>
    /// Creates an in-memory ZIP backup of the specified database using a hot checkpoint-and-copy.
    /// Pass <c>null</c> for the system database.
    /// Returns the ZIP bytes and a timestamped filename.
    /// </summary>
    public async Task<(byte[] Data, string FileName)> GetBackupAsync(
        string? databaseId, CancellationToken ct = default)
    {
        var engine  = _registry.GetEngine(databaseId);
        var label   = string.IsNullOrWhiteSpace(databaseId) ? "system" : databaseId.Trim().ToLowerInvariant();
        var stamp   = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss");
        var zipName = $"blite-backup-{label}-{stamp}.zip";

        // Write to a temp file so the engine can stream to it directly,
        // then wrap into a ZIP for download.
        var tempDb = Path.Combine(Path.GetTempPath(), $"blite-bkp-{Guid.NewGuid():N}.db");
        try
        {
            await engine.BackupAsync(tempDb, ct);

            using var ms = new MemoryStream();
            using (var zip = new ZipArchive(ms, ZipArchiveMode.Create, leaveOpen: true))
            {
                var entry = zip.CreateEntry($"{label}.db", CompressionLevel.Fastest);
                await using var entryStream = entry.Open();
                await using var fs = new FileStream(tempDb, FileMode.Open, FileAccess.Read, FileShare.None);
                await fs.CopyToAsync(entryStream, ct);
            }
            return (ms.ToArray(), zipName);
        }
        finally
        {
            if (File.Exists(tempDb)) File.Delete(tempDb);
        }
    }

    // ── Users ─────────────────────────────────────────────────────────────────

    public IReadOnlyList<BLiteUser> ListUsers() => _users.ListAll();

    /// <summary>Reloads the user list from storage into the in-memory cache.</summary>
    public Task ReloadUsersAsync(CancellationToken ct = default)
        => _users.LoadAllAsync(ct);

    public async Task<string> CreateUserAsync(
        string username, string? ns, string? databaseId,
        string collection = "*", BLiteOperation ops = BLiteOperation.None)
    {
        var perms = new List<PermissionEntry>
        {
            new(collection, ops)
        };
        var (_, plainKey) = await _users.CreateAsync(username, ns, perms, databaseId);
        return plainKey;
    }

    public Task RevokeUserAsync(string username)   => _users.RevokeAsync(username);
    public Task<bool> DeleteUserAsync(string username) => _users.DeleteUserAsync(username);

    /// <summary>Returns the user record by username, or null if not found.</summary>
    public BLiteUser? GetUser(string username)
        => _users.ListAll().FirstOrDefault(u =>
               u.Username.Equals(username, StringComparison.OrdinalIgnoreCase));

    /// <summary>Replaces the permission list for a user.</summary>
    public Task<bool> UpdatePermissionsAsync(
        string username, IReadOnlyList<PermissionEntry> perms,
        CancellationToken ct = default)
        => _users.UpdatePermissionsAsync(username, perms, ct);

    /// <summary>
    /// Completes the setup wizard: sets the root key and persists the setup-complete marker.
    /// After this call <see cref="IsSetupComplete"/> returns true and the wizard is no
    /// longer shown.
    /// </summary>
    public async Task CompleteSetupAsync(string rootKey)
    {
        await _users.EnsureRootAsync(rootKey);
        await _setup.MarkCompleteAsync();
    }

    public async Task<string?> RotateKeyAsync(string username)
        => await _users.RotateKeyAsync(username);

    // ── Collections ───────────────────────────────────────────────────────────

    public List<CollectionInfo> ListCollections(string? databaseId)
    {
        var engine = _registry.GetEngine(databaseId);
        return engine.ListCollections()
            .Where(n => !n.StartsWith("_")) // hide internal collections
            .Select(n => new CollectionInfo(n))
            .ToList();
    }

    public bool DropCollection(string? databaseId, string collection)
    {
        var engine = _registry.GetEngine(databaseId);
        return engine.DropCollection(collection);
    }

    /// <summary>
    /// Parses a JSON string and inserts the resulting document into the specified collection.
    /// The collection is created automatically if it does not exist.
    /// Returns the assigned BsonId.
    /// </summary>
    public async Task<BsonId> InsertDocumentFromJsonAsync(
        string? databaseId, string collection, string json,
        CancellationToken ct = default)
    {
        var engine = _registry.GetEngine(databaseId);

        // Register all JSON field names first — BsonSpanWriter requires every key
        // to be in the engine's persisted dictionary before serialization.
        engine.RegisterKeys(CollectJsonKeys(json));

        var keyMap     = (ConcurrentDictionary<string, ushort>)engine.GetKeyMap();
        var reverseMap = (ConcurrentDictionary<ushort, string>)engine.GetKeyReverseMap();
        var doc        = BsonJsonConverter.FromJson(json, keyMap, reverseMap);
        return await engine.InsertAsync(collection, doc, ct);
    }

    /// Extracts every JSON property name from a JSON object (recursively, including nested
    /// objects and arrays) so they can be pre-registered in the engine dictionary.
    private static IEnumerable<string> CollectJsonKeys(string json)
    {
        var keys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        using var jdoc = JsonDocument.Parse(json);
        CollectElementKeys(jdoc.RootElement, keys);
        return keys;
    }

    private static void CollectElementKeys(JsonElement el, HashSet<string> keys)
    {
        if (el.ValueKind == JsonValueKind.Object)
        {
            foreach (var prop in el.EnumerateObject())
            {
                if (prop.Name != "_id") keys.Add(prop.Name);
                CollectElementKeys(prop.Value, keys);
            }
        }
        else if (el.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in el.EnumerateArray())
                CollectElementKeys(item, keys);
        }
    }

    /// <summary>
    /// Ensures a collection exists (creates it if absent).
    /// New collections appear in ListCollections only after at least one document is stored.
    /// This method inserts a tiny marker document immediately so the collection is visible.
    /// The marker has __studio_init = true and can be deleted by the user.
    /// </summary>
    public async Task EnsureCollectionAsync(string? databaseId, string collectionName,
        CancellationToken ct = default)
    {
        var engine = _registry.GetEngine(databaseId);
        var col    = engine.GetOrCreateCollection(collectionName);
        // A bare GetOrCreateCollection won't persist; insert + delete is the canonical way
        // to materialise an empty-looking collection.
        var doc = col.CreateDocument(["__studio_init"], b => b.AddBoolean("__studio_init", true));
        var id  = await col.InsertAsync(doc, ct);
        await col.DeleteAsync(id, ct);
        await engine.CommitAsync(ct);
    }

    private static void AppendJsonProperty(BsonDocumentBuilder b, string name, JsonElement el)
    {
        switch (el.ValueKind)
        {
            case JsonValueKind.String:
                // Try parse as DateTime
                if (el.TryGetDateTime(out var dt))  { b.AddDateTime(name, dt);          break; }
                if (Guid.TryParse(el.GetString(), out var g)) { b.AddGuid(name, g);     break; }
                b.AddString(name, el.GetString()!);
                break;
            case JsonValueKind.Number:
                if (el.TryGetInt64(out var l))        { b.AddInt64(name, l);             break; }
                b.AddDouble(name, el.GetDouble());
                break;
            case JsonValueKind.True:  b.AddBoolean(name, true);  break;
            case JsonValueKind.False: b.AddBoolean(name, false); break;
            case JsonValueKind.Null:  b.AddNull(name);            break;
            default: // Object or Array → store as JSON string for now
                b.AddString(name, el.GetRawText());
                break;
        }
    }

    // ── Collection metadata ───────────────────────────────────────────────────

    /// <summary>Returns document count and ID type for a single collection.</summary>
    public (int Count, string IdType) GetCollectionMeta(string? databaseId, string collection)
    {
        var engine = _registry.GetEngine(databaseId);
        var col    = engine.GetOrCreateCollection(collection);
        return (col.Count(), col.IdType.ToString());
    }

    // ── Index management ──────────────────────────────────────────────────────

    /// <summary>
    /// Describes a field discovered by sampling documents in a collection.
    /// Used to populate the index creation form in the Studio UI.
    /// </summary>
    public sealed record FieldSample(string Path, BsonType Type, BsonType? ArrayItemType = null)
    {
        /// <summary>True when the array items are numeric (candidate for a Vector index).</summary>
        public bool IsNumericArray =>
            Type == BsonType.Array &&
            (ArrayItemType == BsonType.Double || ArrayItemType == BsonType.Int32 || ArrayItemType == BsonType.Int64);

        /// <summary>True when the array holds [lat, lon] coordinates (candidate for a Spatial index).</summary>
        public bool IsCoordinateArray =>
            Type == BsonType.Array &&
            (ArrayItemType == BsonType.Double || ArrayItemType == BsonType.Int32);
    }

    /// <summary>Returns the names of all secondary indexes on a collection.</summary>
    public IReadOnlyList<string> ListIndexes(string? databaseId, string collection)
    {
        var engine = _registry.GetEngine(databaseId);
        var col    = engine.GetOrCreateCollection(collection);
        return col.ListIndexes();
    }

    /// <summary>Returns typed descriptors for all secondary indexes of a collection.</summary>
    public IReadOnlyList<DynamicIndexDescriptor> GetIndexDescriptors(string? databaseId, string collection)
    {
        var engine = _registry.GetEngine(databaseId);
        return engine.GetIndexDescriptors(collection);
    }

    /// <summary>
    /// Samples up to <paramref name="sampleSize"/> documents to discover field names and types.
    /// One entry per unique field name; first type seen wins.
    /// </summary>
    public IReadOnlyList<FieldSample> SampleFields(string? databaseId, string collection, int sampleSize = 20)
    {
        var engine     = _registry.GetEngine(databaseId);
        var col        = engine.GetOrCreateCollection(collection);
        var discovered = new Dictionary<string, (BsonType Type, BsonType? ArrayItemType)>(StringComparer.OrdinalIgnoreCase);

        foreach (var doc in col.FindAll().Take(sampleSize))
        {
            foreach (var (name, value) in doc.EnumerateFields())
            {
                if (name == "_id" || discovered.ContainsKey(name))
                    continue;

                BsonType? arrayItemType = null;
                if (value.Type == BsonType.Array)
                {
                    var arr = value.AsArray;
                    arrayItemType = arr.Count > 0 ? arr[0].Type : null;
                }
                discovered[name] = (value.Type, arrayItemType);
            }
        }

        return discovered
            .OrderBy(kvp => kvp.Key)
            .Select(kvp => new FieldSample(kvp.Key, kvp.Value.Type, kvp.Value.ArrayItemType))
            .ToList();
    }

    /// <summary>Creates a secondary B-Tree index on the specified field and commits.</summary>
    public async Task CreateIndexAsync(
        string? databaseId, string collection,
        string field, string? name, bool unique,
        CancellationToken ct = default)
    {
        var engine = _registry.GetEngine(databaseId);
        var col    = engine.GetOrCreateCollection(collection);
        col.CreateIndex(field, string.IsNullOrWhiteSpace(name) ? null : name.Trim(), unique);
        await engine.CommitAsync(ct);
    }

    /// <summary>Creates a Vector (HNSW) index on the specified field and commits.</summary>
    public async Task CreateVectorIndexAsync(
        string? databaseId, string collection,
        string field, int dimensions, VectorMetric metric, string? name,
        CancellationToken ct = default)
    {
        var engine = _registry.GetEngine(databaseId);
        var col    = engine.GetOrCreateCollection(collection);
        col.CreateVectorIndex(field, dimensions, metric, string.IsNullOrWhiteSpace(name) ? null : name.Trim());
        await engine.CommitAsync(ct);
    }

    /// <summary>Creates a Spatial (R-Tree) index on the specified field and commits.</summary>
    public async Task CreateSpatialIndexAsync(
        string? databaseId, string collection,
        string field, string? name,
        CancellationToken ct = default)
    {
        var engine = _registry.GetEngine(databaseId);
        var col    = engine.GetOrCreateCollection(collection);
        col.CreateSpatialIndex(field, string.IsNullOrWhiteSpace(name) ? null : name.Trim());
        await engine.CommitAsync(ct);
    }

    /// <summary>Drops a secondary index by name and commits. Returns false if not found.</summary>
    public async Task<bool> DropIndexAsync(
        string? databaseId, string collection, string indexName,
        CancellationToken ct = default)
    {
        var engine = _registry.GetEngine(databaseId);
        var col    = engine.GetOrCreateCollection(collection);
        var ok     = col.DropIndex(indexName);
        if (ok) await engine.CommitAsync(ct);
        return ok;
    }

    // ── BLQL query ────────────────────────────────────────────────────────────

    /// <summary>
    /// Executes a BLQL query against a collection and returns matching documents as
    /// <see cref="DocumentRow"/> for display.
    /// </summary>
    public Task<List<DocumentRow>> RunBlqlQueryAsync(
        string? databaseId, string collection,
        string? filterJson, string? sortJson,
        int skip, int take,
        CancellationToken ct = default)
    {
        var engine = _registry.GetEngine(databaseId);
        var col    = engine.GetOrCreateCollection(collection);

        var query = col.Query();

        if (!string.IsNullOrWhiteSpace(filterJson))
            query = query.Filter(BlqlFilterParser.Parse(filterJson));

        if (!string.IsNullOrWhiteSpace(sortJson))
            query = query.Sort(sortJson);

        if (skip > 0) query = query.Skip(skip);
        if (take > 0) query = query.Take(take);

        var rows = new List<DocumentRow>();
        foreach (var doc in query.AsEnumerable())
            rows.Add(DocumentToRow(doc));

        return Task.FromResult(rows);
    }

    public async Task<List<DocumentRow>> GetDocumentsAsync(
        string? databaseId, string collection, int skip, int take,
        CancellationToken ct = default)
    {
        var engine = _registry.GetEngine(databaseId);
        var col    = engine.GetOrCreateCollection(collection);
        var rows   = new List<DocumentRow>();
        int index  = 0;

        await foreach (var doc in col.FindAllAsync(ct))
        {
            if (index++ < skip) continue;
            if (rows.Count >= take) break;

            rows.Add(DocumentToRow(doc));
        }

        return rows;
    }

    public async Task<int> CountDocumentsAsync(
        string? databaseId, string collection, CancellationToken ct = default)
    {
        var engine = _registry.GetEngine(databaseId);
        var col    = engine.GetOrCreateCollection(collection);
        int count  = 0;
        await foreach (var _ in col.FindAllAsync(ct))
            count++;
        return count;
    }

    public async Task<bool> DeleteDocumentAsync(
        string? databaseId, string collection, BsonId id,
        CancellationToken ct = default)
    {
        var engine = _registry.GetEngine(databaseId);
        return await engine.DeleteAsync(collection, id, ct);
    }

    /// <summary>
    /// Fetches a single document by ID and serialises it to indented JSON.
    /// </summary>
    public async Task<string> GetDocumentAsJsonAsync(
        string? databaseId, string collection, BsonId id,
        CancellationToken ct = default)
    {
        var engine = _registry.GetEngine(databaseId);
        var doc    = await engine.FindByIdAsync(collection, id, ct);
        return doc is null ? null : BsonJsonConverter.ToJson(doc, indented: true);
    }

    // ── Embedding ────────────────────────────────────────────────────────────

    /// <summary>Returns information about the currently loaded embedding model.</summary>
    public EmbeddingModelInfo GetEmbeddingModelInfo() => _embedding.Info;

    /// <summary>
    /// Loads a new embedding model from the specified directory.
    /// Throws if model.onnx or tokenizer.json are missing.
    /// </summary>
    public void LoadEmbeddingModel(string directory)
    {
        var fullPath = Path.GetFullPath(directory);
        if (!Directory.Exists(fullPath))
            throw new DirectoryNotFoundException($"Directory '{fullPath}' does not exist.");

        _embedding.LoadFromDirectory(fullPath);
    }

    /// <summary>Encodes <paramref name="text"/> and returns the L2-normalised float vector.</summary>
    public float[] ComputeEmbedding(string text) => _embedding.Embed(text);

    /// <summary>
    /// Normalizes <paramref name="text"/> through the standard embedding pipeline:
    /// ASCII fold → lowercase → punctuation removal → whitespace collapse.
    /// </summary>
    public static string NormalizeText(string text) => TextNormalizer.Normalize(text);

    /// <summary>
    /// Builds the embedding-ready text for <paramref name="document"/> using its
    /// <see cref="VectorSourceConfig"/>, then normalizes and returns it.
    /// Returns <see cref="string.Empty"/> if the collection has no VectorSource configured
    /// or if all fields produce empty text.
    /// </summary>
    public string BuildAndNormalizeEmbeddingText(string? databaseId, string collection, BsonDocument document)
    {
        var config = GetVectorSource(databaseId, collection);
        return config == null ? string.Empty : TextNormalizer.BuildEmbeddingText(document, config);
    }

    /// <summary>
    /// Normalizes the text and then computes the embedding vector.
    /// Convenience shortcut for the full pipeline: raw text → normalize → embed.
    /// </summary>
    public float[] NormalizeAndEmbed(string text) => _embedding.Embed(TextNormalizer.Normalize(text));

    // ── VectorSource Configuration ────────────────────────────────────────────

    /// <summary>
    /// Gets the VectorSource configuration for a collection, or null if not configured.
    /// </summary>
    public VectorSourceConfig? GetVectorSource(string? databaseId, string collection)
    {
        var engine = _registry.GetEngine(databaseId);
        return engine.GetVectorSource(collection);
    }

    /// <summary>
    /// Sets or updates the VectorSource configuration for a collection.
    /// Pass null to clear the configuration.
    /// </summary>
    public void SetVectorSource(string? databaseId, string collection, VectorSourceConfig? config)
    {
        var engine = _registry.GetEngine(databaseId);
        engine.SetVectorSource(collection, config);
    }

    /// <summary>
    /// Returns true if the collection has at least one Vector (HNSW) secondary index.
    /// </summary>
    public bool HasVectorIndex(string? databaseId, string collection)
    {
        var engine = _registry.GetEngine(databaseId);
        return engine.GetIndexDescriptors(collection).Any(d => d.Type == IndexType.Vector);
    }

    /// <summary>
    /// Builds the normalized embedding text for the document with the given <paramref name="id"/>
    /// using the collection's VectorSource configuration.
    /// Returns <see cref="string.Empty"/> if the document is not found or no VectorSource is configured.
    /// </summary>
    public string BuildEmbeddingTextById(string? databaseId, string collection, BsonId id)
    {
        var engine = _registry.GetEngine(databaseId);
        var doc    = engine.FindById(collection, id);
        if (doc == null) return string.Empty;
        var config = engine.GetVectorSource(collection);
        return config == null ? string.Empty : TextNormalizer.BuildEmbeddingText(doc, config);
    }

    /// <summary>
    /// Parses <paramref name="json"/> and replaces the document with the specified ID.
    /// </summary>
    public async Task<bool> UpdateDocumentFromJsonAsync(
        string? databaseId, string collection, BsonId id, string json,
        CancellationToken ct = default)
    {
        var engine = _registry.GetEngine(databaseId);
        engine.RegisterKeys(CollectJsonKeys(json));
        var keyMap     = (ConcurrentDictionary<string, ushort>)engine.GetKeyMap();
        var reverseMap = (ConcurrentDictionary<ushort, string>)engine.GetKeyReverseMap();
        var doc        = BsonJsonConverter.FromJson(json, keyMap, reverseMap);
        return await engine.UpdateAsync(collection, id, doc, ct);
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private static DocumentRow DocumentToRow(BsonDocument doc)
    {
        // Build a dictionary of field→string for display
        var fields = new Dictionary<string, string>();

        doc.TryGetId(out var id);

        foreach (var (name, val) in doc.EnumerateFields())
        {
            if (name == "_id") continue;
            fields[name] = FormatValue(val);
        }

        return new DocumentRow(id, fields);
    }

    private static string FormatValue(BsonValue val)
    {
        if (val.IsNull)     return "null";
        if (val.IsString)   return val.AsString;
        if (val.IsInt32)    return val.AsInt32.ToString();
        if (val.IsInt64)    return val.AsInt64.ToString();
        if (val.IsDouble)   return val.AsDouble.ToString("G");
        if (val.IsBoolean)  return val.AsBoolean ? "true" : "false";
        if (val.IsDateTime) return val.AsDateTime.ToString("O");
        if (val.IsObjectId) return val.AsObjectId.ToString();
        if (val.IsCoordinates) return $"({val.AsCoordinates.Lat}, {val.AsCoordinates.Lon})";
        if (val.IsArray)    return $"[{string.Join(", ", val.AsArray.Select(FormatValue))}]";
        if (val.IsDocument) return "{...}";
        if (val.IsBinary)   return $"<binary {val.AsBinary.Length} bytes>";
        if (val.IsDecimal)  return val.AsDecimal.ToString("G");
        if (val.IsGuid)     return val.AsGuid.ToString();
        if(val.IsTimestamp) return val.AsTimestamp.ToString("O");
        return val.ToString() ?? "???";
    }
}

// ── DTOs ──────────────────────────────────────────────────────────────────────

public sealed record ServerInfo(
    string Version, TimeSpan Uptime, int TenantCount, int UserCount, string DatabasesDir,
    string SourceUrl);

public sealed record CollectionInfo(string Name);

public sealed record DocumentRow(BsonId Id, Dictionary<string, string> Fields);
