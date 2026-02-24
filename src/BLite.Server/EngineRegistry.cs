// BLite.Server — EngineRegistry
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Manages one BLiteEngine per database-id, providing physical isolation
// between tenants.  Supports hot-provisioning of new databases at runtime
// without restarting the server.
//
// Design:
//   • The system / default engine is mapped to the empty-string key "" and is
//     always pre-initialised.  It hosts the _users collection.
//   • Named tenant engines live in DatabasesDirectory/<id>.db and are opened
//     lazily on first access (GetEngine) or eagerly via ProvisionAsync.
//   • Database IDs are normalised (trimmed + lowercased) before use as map keys
//     so that "ACME" and "acme" always resolve to the same engine.

using System.Collections.Concurrent;
using BLite.Core;
using BLite.Core.Storage;

namespace BLite.Server;

/// <summary>
/// Describes a discovered tenant database (file on disk, active or not yet opened).
/// </summary>
public sealed record TenantEntry(string DatabaseId, string DatabasePath, bool IsActive);

/// <summary>
/// Thread-safe registry that maps a <c>database_id</c> to its <see cref="BLiteEngine"/>.
///
/// <para>
/// The <b>system engine</b> (key = empty string) is always live and hosts user metadata.
/// </para>
///
/// <para>
/// Named tenant engines live under <see cref="DatabasesDirectory"/> and are opened
/// lazily on first <see cref="GetEngine"/> call.  New databases can be created at runtime
/// via <see cref="ProvisionAsync"/> without restarting the server.
/// </para>
/// </summary>
public sealed class EngineRegistry : IDisposable
{
    private const string DefaultKey = "";

    /// <summary>Directory that contains tenant <c>.db</c> files.</summary>
    public string DatabasesDirectory { get; }

    /// <summary>Absolute path to the system (default) database file.</summary>
    public string SystemDatabasePath { get; }

    private readonly ConcurrentDictionary<string, BLiteEngine> _active
        = new(StringComparer.OrdinalIgnoreCase);

    private readonly PageFileConfig _defaultPageConfig;
    private bool _disposed;

    /// <param name="systemEngine">
    ///   Pre-created engine for the system (default) database.
    ///   This engine is <em>not</em> disposed when individual tenants are deprovisioned;
    ///   it is only disposed when the registry itself is disposed.
    /// </param>
    /// <param name="databasesDirectory">
    ///   Directory where tenant <c>.db</c> files are stored.
    ///   Created automatically if it does not exist.
    /// </param>
    /// <param name="defaultPageConfig">
    ///   Page-file configuration applied to all lazily-opened tenant engines.
    /// </param>
    public EngineRegistry(
        BLiteEngine     systemEngine,
        string          systemDatabasePath,
        string          databasesDirectory,
        PageFileConfig  defaultPageConfig)
    {
        _active[DefaultKey]  = systemEngine;
        SystemDatabasePath   = Path.GetFullPath(systemDatabasePath);
        DatabasesDirectory   = databasesDirectory;
        _defaultPageConfig   = defaultPageConfig;

        if (!string.IsNullOrEmpty(databasesDirectory))
            Directory.CreateDirectory(databasesDirectory);
    }

    // ── Lookup ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Returns the <see cref="BLiteEngine"/> for the given <paramref name="databaseId"/>.
    /// </summary>
    /// <param name="databaseId">
    ///   Database identifier.  <c>null</c> or empty → system (default) engine.
    ///   Any other value → named tenant; opened lazily if the <c>.db</c> file exists.
    /// </param>
    /// <exception cref="InvalidOperationException">
    ///   Thrown when <paramref name="databaseId"/> is non-empty but no <c>.db</c> file
    ///   is present in <see cref="DatabasesDirectory"/>.  Call <see cref="ProvisionAsync"/>
    ///   to create a new database.
    /// </exception>
    public BLiteEngine GetEngine(string? databaseId)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var key = Normalise(databaseId);

        // Fast path: already open
        if (_active.TryGetValue(key, out var existing))
            return existing;

        // Lazy-open: check for an existing .db file in the tenants directory
        var dbPath = GetDbPath(key);
        if (!File.Exists(dbPath))
            throw new InvalidOperationException(
                $"Database '{key}' does not exist. " +
                "Provision it first via AdminService.ProvisionTenant.");

        return _active.GetOrAdd(key, _ => new BLiteEngine(dbPath, _defaultPageConfig));
    }

    /// <summary>The system (default) engine — hosts user metadata.</summary>
    public BLiteEngine SystemEngine => GetEngine(null);

    // ── Provisioning ─────────────────────────────────────────────────────────

    /// <summary>
    /// Creates a new tenant database on disk and registers it in the active set.
    /// No server restart is required.
    /// </summary>
    /// <exception cref="ArgumentException">If <paramref name="databaseId"/> is empty.</exception>
    /// <exception cref="InvalidOperationException">If the database is already active.</exception>
    public Task ProvisionAsync(string databaseId, CancellationToken ct = default)
    {
        var key = Normalise(databaseId);
        if (string.IsNullOrEmpty(key))
            throw new ArgumentException("Cannot provision the default (system) database.", nameof(databaseId));

        if (_active.ContainsKey(key))
            throw new InvalidOperationException($"Database '{key}' is already active.");

        var dbPath = GetDbPath(key);
        var engine = new BLiteEngine(dbPath, _defaultPageConfig);

        if (!_active.TryAdd(key, engine))
        {
            engine.Dispose();
            throw new InvalidOperationException(
                $"Concurrent provisioning race: database '{key}' was registered by another request.");
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Closes the tenant engine and (optionally) deletes it from disk.
    /// Satisfies GDPR right-to-erasure when <paramref name="deleteFiles"/> is <c>true</c>.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    ///   Thrown when attempting to deprovision the default database, or when
    ///   <paramref name="databaseId"/> is not currently active.
    /// </exception>
    public Task DeprovisionAsync(string databaseId, bool deleteFiles, CancellationToken ct = default)
    {
        var key = Normalise(databaseId);
        if (string.IsNullOrEmpty(key))
            throw new InvalidOperationException("Cannot deprovision the default (system) database.");

        if (!_active.TryRemove(key, out var engine))
            throw new InvalidOperationException($"Database '{key}' is not active.");

        engine.Dispose();

        if (deleteFiles)
        {
            var dbPath = GetDbPath(key);
            if (File.Exists(dbPath))           File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal"))  File.Delete(dbPath + ".wal");
        }

        return Task.CompletedTask;
    }

    // ── Discovery ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Returns metadata about all tenant databases found in <see cref="DatabasesDirectory"/>.
    /// A database is "active" if its engine is currently open in memory.
    /// </summary>
    public IReadOnlyList<TenantEntry> ListTenants()
    {
        var result = new List<TenantEntry>();

        if (string.IsNullOrEmpty(DatabasesDirectory) || !Directory.Exists(DatabasesDirectory))
            return result;

        foreach (var file in Directory.GetFiles(DatabasesDirectory, "*.db"))
        {
            var id = Path.GetFileNameWithoutExtension(file).ToLowerInvariant();
            result.Add(new TenantEntry(id, file, _active.ContainsKey(id)));
        }

        return result;
    }

    /// <summary>
    /// Ensures <see cref="DatabasesDirectory"/> exists.
    /// Called once at startup; lazy open means no engines are opened here.
    /// </summary>
    public void ScanDirectory()
    {
        if (!string.IsNullOrEmpty(DatabasesDirectory))
            Directory.CreateDirectory(DatabasesDirectory);
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        foreach (var engine in _active.Values)
            try { engine.Dispose(); } catch { /* best-effort */ }

        _active.Clear();
    }

    // ── Path helpers ──────────────────────────────────────────────────────────

    /// <summary>
    /// Returns the absolute path of the <c>.db</c> file for the given database.
    /// Pass <c>null</c> or empty string for the system (default) database.
    /// </summary>
    public string GetDatabasePath(string? databaseId)
    {
        var key = Normalise(databaseId);
        return string.IsNullOrEmpty(key)
            ? SystemDatabasePath
            : Path.GetFullPath(GetDbPath(key));
    }

    // ── Internals ─────────────────────────────────────────────────────────────

    private string GetDbPath(string key)
    {
        if (string.IsNullOrEmpty(DatabasesDirectory))
            throw new InvalidOperationException(
                "DatabasesDirectory is not configured. " +
                "Add 'BLiteServer:DatabasesDirectory' to appsettings.json.");

        return Path.Combine(DatabasesDirectory, $"{key}.db");
    }

    private static string Normalise(string? databaseId)
        => string.IsNullOrWhiteSpace(databaseId) ? DefaultKey
                                                  : databaseId.Trim().ToLowerInvariant();
}
