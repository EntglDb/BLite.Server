// BLite.Server — SetupService
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Tracks whether the one-time setup wizard has been completed.
// State is persisted in a small JSON file placed next to the main database.

using System.Text.Json;

namespace BLite.Server.Studio;

/// <summary>
/// Tracks whether the initial server setup wizard has been completed.
/// Persists to <c>server-setup.json</c> in the same folder as the main database file.
/// Registered as a singleton in DI.
/// </summary>
public sealed class SetupService
{
    private readonly string _markerPath;
    private volatile bool  _complete;

    public SetupService(IConfiguration config)
    {
        var dbPath  = config.GetValue<string>("BLiteServer:DatabasePath") ?? "blite.db";
        var dir     = Path.GetDirectoryName(Path.GetFullPath(dbPath))
                      ?? Directory.GetCurrentDirectory();
        _markerPath = Path.Combine(dir, "server-setup.json");
    }

    /// <summary>
    /// True once the setup wizard has been completed at least once.
    /// </summary>
    public bool IsSetupComplete => _complete;

    /// <summary>
    /// Reads persisted state from disk. Call once at startup before serving requests.
    /// </summary>
    public void Load()
    {
        _complete = File.Exists(_markerPath);
    }

    /// <summary>
    /// Writes the setup-complete marker to disk.
    /// </summary>
    public async Task MarkCompleteAsync()
    {
        var json = JsonSerializer.Serialize(new
        {
            completed_at = DateTime.UtcNow.ToString("O"),
            note         = "Root key is stored in the user database — DO NOT edit or delete this file."
        });

        var dir = Path.GetDirectoryName(_markerPath);
        if (!string.IsNullOrEmpty(dir))
            Directory.CreateDirectory(dir);

        await File.WriteAllTextAsync(_markerPath, json);
        _complete = true;
    }
}
