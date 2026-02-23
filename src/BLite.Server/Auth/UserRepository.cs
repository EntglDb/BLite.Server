// BLite.Server — User repository
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Stores users in the reserved "_users" collection inside the BLite engine.
// An in-memory ConcurrentDictionary keyed by ApiKeyHash provides O(1) lookup
// at request time (the hot path avoids hitting the storage engine).
//
// Document schema (all fields stored as BSON strings / int):
//   _id        → BsonId (ObjectId auto-generated)
//   username   → string  (unique)
//   key_hash   → string  (hex SHA-256)
//   namespace  → string  (nullable, stored as empty string when null)
//   active     → bool
//   created_at → DateTime
//   perms      → base64(MessagePack<List<PermProto>>)
//
// Permissions are serialized as MessagePack so we don't need a sub-document model.
// A future version may switch to a proper nested BsonDocument once BLite gains
// first-class array-of-documents support.

using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using BLite.Bson;
using BLite.Core;
using MessagePack;

namespace BLite.Server.Auth;

public sealed class UserRepository
{
    private const string Collection = "_users";

    private readonly BLiteEngine _engine;

    // Hot-path lookup: ApiKeyHash → BLiteUser
    private readonly ConcurrentDictionary<string, BLiteUser> _byKey  = new(StringComparer.Ordinal);
    // Secondary index: Username → BsonId (for updates/deletes)
    private readonly ConcurrentDictionary<string, BsonId>    _byName = new(StringComparer.OrdinalIgnoreCase);

    public UserRepository(BLiteEngine engine) => _engine = engine;

    // ── Bootstrap ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Ensures the root user exists.  Called once at startup by <c>Program.cs</c>.
    /// If a user named "root" is already present the call is a no-op (the existing
    /// key is NOT overwritten — rotate via AdminService).
    /// </summary>
    public async Task EnsureRootAsync(string rootApiKey, CancellationToken ct = default)
    {
        await LoadAllAsync(ct);

        if (_byName.ContainsKey("root")) return;

        var user = new BLiteUser(
            Username:    "root",
            ApiKeyHash:  HashKey(rootApiKey),
            Namespace:   null,
            Permissions: [new PermissionEntry("*", BLiteOperation.All)],
            Active:      true,
            CreatedAt:   DateTime.UtcNow);

        await PersistNewAsync(user, ct);
    }

    // ── Hot-path lookup ───────────────────────────────────────────────────────

    /// <summary>Returns the user whose key hashes to <paramref name="apiKey"/>, or null.</summary>
    public BLiteUser? FindByKey(string apiKey)
    {
        var hash = HashKey(apiKey);
        _byKey.TryGetValue(hash, out var user);
        return user;
    }

    // ── CRUD ──────────────────────────────────────────────────────────────────

    public async Task<(BLiteUser User, string PlainKey)> CreateAsync(
        string username, string? ns,
        IReadOnlyList<PermissionEntry> perms,
        string? databaseId = null,
        CancellationToken ct = default)
    {
        if (_byName.ContainsKey(username))
            throw new InvalidOperationException($"User '{username}' already exists.");

        var plainKey = GenerateKey();
        var user = new BLiteUser(username, HashKey(plainKey), ns, perms, true, DateTime.UtcNow,
            DatabaseId: string.IsNullOrWhiteSpace(databaseId) ? null : databaseId);
        await PersistNewAsync(user, ct);
        return (user, plainKey);
    }

    public async Task<bool> RevokeAsync(string username, CancellationToken ct = default)
    {
        if (!_byName.TryGetValue(username, out var id)) return false;

        var col = _engine.GetOrCreateCollection(Collection);
        var doc = await col.FindByIdAsync(id, ct);
        if (doc is null) return false;

        var updated = col.CreateDocument(
            ["username", "key_hash", "namespace", "active", "created_at", "perms", "database_id"],
            b => RebuildBuilder(b, doc, active: false));

        var ok = await col.UpdateAsync(id, updated, ct);

        if (ok)
        {
            // Invalidate cache
            var oldUser = _byKey.Values.FirstOrDefault(u => u.Username.Equals(username, StringComparison.OrdinalIgnoreCase));
            if (oldUser is not null) _byKey.TryRemove(oldUser.ApiKeyHash, out _);
        }

        return ok;
    }

    /// <summary>
    /// Permanently deletes a user from storage and all in-memory caches.
    /// Unlike Revoke, the username can be reused after deletion.
    /// Returns false if the user was not found.
    /// </summary>
    public async Task<bool> DeleteUserAsync(string username, CancellationToken ct = default)
    {
        if (!_byName.TryGetValue(username, out var id)) return false;

        var col = _engine.GetOrCreateCollection(Collection);
        var ok  = await col.DeleteAsync(id, ct);

        if (ok)
        {
            _byName.TryRemove(username, out _);
            var old = _byKey.Values.FirstOrDefault(u =>
                u.Username.Equals(username, StringComparison.OrdinalIgnoreCase));
            if (old is not null) _byKey.TryRemove(old.ApiKeyHash, out _);
        }

        return ok;
    }

    public async Task<string?> RotateKeyAsync(string username, CancellationToken ct = default)
    {
        if (!_byName.TryGetValue(username, out var id)) return null;

        var col = _engine.GetOrCreateCollection(Collection);
        var doc = await col.FindByIdAsync(id, ct);
        if (doc is null) return null;

        var plainKey = GenerateKey();
        var newHash  = HashKey(plainKey);

        var updated = col.CreateDocument(
            ["username", "key_hash", "namespace", "active", "created_at", "perms", "database_id"],
            b => RebuildBuilder(b, doc, keyHash: newHash));

        var ok = await col.UpdateAsync(id, updated, ct);
        if (!ok) return null;

        // Update cache: remove old hash, add new
        var oldUser = _byKey.Values.FirstOrDefault(u => u.Username.Equals(username, StringComparison.OrdinalIgnoreCase));
        if (oldUser is not null)
        {
            _byKey.TryRemove(oldUser.ApiKeyHash, out _);
            var newUser = oldUser with { ApiKeyHash = newHash };
            _byKey[newHash] = newUser;
        }

        return plainKey;
    }

    public async Task<bool> UpdatePermissionsAsync(
        string username, IReadOnlyList<PermissionEntry> perms, CancellationToken ct = default)
    {
        if (!_byName.TryGetValue(username, out var id)) return false;

        var col = _engine.GetOrCreateCollection(Collection);
        var doc = await col.FindByIdAsync(id, ct);
        if (doc is null) return false;

        var permsBytes = Convert.ToBase64String(MessagePackSerializer.Serialize(ToProtoPerms(perms)));
        var updated = col.CreateDocument(
            ["username", "key_hash", "namespace", "active", "created_at", "perms", "database_id"],
            b => RebuildBuilder(b, doc, permsBytes: permsBytes));

        var ok = await col.UpdateAsync(id, updated, ct);
        if (ok)
        {
            var oldUser = _byKey.Values.FirstOrDefault(u =>
                u.Username.Equals(username, StringComparison.OrdinalIgnoreCase));
            if (oldUser is not null)
                _byKey[oldUser.ApiKeyHash] = oldUser with { Permissions = perms };
        }

        return ok;
    }

    public IReadOnlyList<BLiteUser> ListAll()
        => _byKey.Values.OrderBy(u => u.Username).ToList();

    // ── Load from storage ─────────────────────────────────────────────────────

    public async Task LoadAllAsync(CancellationToken ct = default)
    {
        var col = _engine.GetOrCreateCollection(Collection);

        await foreach (var doc in col.FindAllAsync(ct))
        {
            var user = DocToUser(doc);
            if (user is null) continue;
            _byKey[user.ApiKeyHash] = user;
            if (user.StoredId.HasValue)
                _byName[user.Username] = user.StoredId.Value;
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private async Task PersistNewAsync(BLiteUser user, CancellationToken ct)
    {
        var col       = _engine.GetOrCreateCollection(Collection);
        var permsB64  = Convert.ToBase64String(
            MessagePackSerializer.Serialize(ToProtoPerms(user.Permissions)));

        var doc = col.CreateDocument(
            ["username", "key_hash", "namespace", "active", "created_at", "perms", "database_id"],
            b => b
                .AddString("username",    user.Username)
                .AddString("key_hash",    user.ApiKeyHash)
                .AddString("namespace",   user.Namespace   ?? "")
                .AddBoolean("active",     user.Active)
                .AddDateTime("created_at", user.CreatedAt)
                .AddString("perms",       permsB64)
                .AddString("database_id", user.DatabaseId ?? ""));

        var id = await col.InsertAsync(doc, ct);
        _byKey[user.ApiKeyHash] = user with { StoredId = id };
        _byName[user.Username]  = id;
    }

    private static BLiteUser? DocToUser(BsonDocument doc)
    {
        if (!doc.TryGetId(out var id))         return null;
        if (!doc.TryGetString("username",    out var username)   || username is null) return null;
        if (!doc.TryGetString("key_hash",    out var keyHash)    || keyHash  is null) return null;
        if (!doc.TryGetString("namespace",   out var nsRaw))     nsRaw      = null;
        if (!doc.TryGetString("perms",       out var permsB64)   || permsB64 is null) return null;
        doc.TryGetString("database_id", out var dbIdRaw);
        string? dbId = string.IsNullOrEmpty(dbIdRaw) ? null : dbIdRaw;

        bool active = true;
        if (doc.TryGetValue("active", out var activeBv))
            active = activeBv.AsBoolean;

        DateTime createdAt = DateTime.MinValue;
        if (doc.TryGetValue("created_at", out var dtBv))
            createdAt = dtBv.AsDateTime;

        string? ns = string.IsNullOrEmpty(nsRaw) ? null : nsRaw;

        List<PermissionEntry> perms = [];
        try
        {
            var proto = MessagePackSerializer.Deserialize<List<PermProto>>(
                Convert.FromBase64String(permsB64));
            perms = proto.Select(p => new PermissionEntry(p.C, (BLiteOperation)p.O)).ToList();
        }
        catch { /* corrupt entry — treat as no permissions */ }

        return new BLiteUser(username, keyHash, ns, perms, active, createdAt, id, dbId);
    }

    /// <summary>
    /// Rebuilds a user document from an existing one, overriding only the specified fields.
    /// </summary>
    private static BsonDocumentBuilder RebuildBuilder(
        BsonDocumentBuilder b, BsonDocument src,
        bool?   active     = null,
        string? keyHash    = null,
        string? permsBytes = null)
    {
        src.TryGetString("username",    out var u);
        src.TryGetString("key_hash",    out var k);
        src.TryGetString("namespace",   out var n);
        src.TryGetValue("created_at",   out var ca);
        src.TryGetString("perms",       out var p);
        src.TryGetString("database_id", out var d);

        b.AddString("username",    u   ?? "");
        b.AddString("key_hash",    keyHash   ?? k ?? "");
        b.AddString("namespace",   n   ?? "");
        b.AddBoolean("active",     active    ?? (src.TryGetValue("active", out var a) && a.AsBoolean));
        b.AddDateTime("created_at", ca.IsNull ? DateTime.UtcNow : ca.AsDateTime);
        b.AddString("perms",       permsBytes ?? p ?? "");
        b.AddString("database_id", d   ?? "");
        return b;
    }

    private static string GenerateKey()
    {
        Span<byte> buf = stackalloc byte[32];
        RandomNumberGenerator.Fill(buf);
        return Convert.ToHexStringLower(buf);
    }

    public static string HashKey(string key)
    {
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(key));
        return Convert.ToHexStringLower(hash);
    }

    // ── MessagePack-serializable permission proto ─────────────────────────────

    [MessagePackObject]
    public sealed class PermProto
    {
        [Key(0)] public string C { get; set; } = "*";   // Collection
        [Key(1)] public int    O { get; set; }           // Ops (as int)
    }

    private static List<PermProto> ToProtoPerms(IReadOnlyList<PermissionEntry> perms)
        => perms.Select(p => new PermProto { C = p.Collection, O = (int)p.Ops }).ToList();
}
