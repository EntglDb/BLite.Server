// BLite.Client — ClientKeyMap
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Manages the client-side replica of the server's global C-BSON key dictionary.
//
// The BLite C-BSON format encodes field names as ushort IDs assigned by the
// server's StorageEngine dictionary. Clients must synchronise their local map
// with the server before serialising or deserialising typed documents, so that
// the IDs embedded in bytes are identical on both sides.
//
// Two usage patterns:
//   • RegisterAsync(collection, keys) — typed path (DocumentService):
//       registers only mapper.UsedKeys and caches their server-assigned IDs.
//   • LoadFullMapAsync(collection)    — dynamic path (DynamicService):
//       downloads the complete server map so any field can be decoded.

using System.Collections.Concurrent;
using BLite.Proto.V1;
using Grpc.Core;

namespace BLite.Client.Internal;

/// <summary>
/// Thread-safe, lazily-populated replica of the server's C-BSON key dictionary.
/// Shared across all collections inside one <see cref="BLiteClient"/> instance
/// (the dictionary is global on the server).
/// </summary>
internal sealed class ClientKeyMap
{
    private readonly MetadataService.MetadataServiceClient _stub;
    private readonly Metadata _headers;

    // Forward: normalised field name → ushort id  (used by BsonSpanWriter)
    private readonly ConcurrentDictionary<string, ushort> _forward =
        new(StringComparer.OrdinalIgnoreCase);

    // Reverse: ushort id → field name  (used by BsonSpanReader + BsonDocument ctor)
    private readonly ConcurrentDictionary<ushort, string> _reverse = new();

    private readonly SemaphoreSlim _fullMapLock = new(1, 1);
    private bool _fullMapLoaded;

    internal ClientKeyMap(
        MetadataService.MetadataServiceClient stub, Metadata headers)
    {
        _stub    = stub;
        _headers = headers;
    }

    /// <summary>
    /// Ensures that <paramref name="keys"/> are registered on the server and
    /// caches their IDs locally.  Idempotent — already-cached keys are skipped.
    /// Requires <c>Insert</c> permission on <paramref name="collection"/>.
    /// </summary>
    internal async Task RegisterAsync(
        string collection,
        IEnumerable<string> keys,
        CancellationToken ct = default)
    {
        // Filter to only the keys we haven't cached yet
        var missing = keys
            .Select(k => k.ToLowerInvariant())
            .Where(k => !_forward.ContainsKey(k))
            .Distinct()
            .ToList();

        if (missing.Count == 0) return;

        var request = new RegisterKeysRequest { Collection = collection };
        request.Keys.AddRange(missing);

        var response = await _stub.RegisterKeysAsync(
            request, _headers, cancellationToken: ct);

        if (!string.IsNullOrEmpty(response.Error))
            throw new InvalidOperationException(
                $"RegisterKeys failed: {response.Error}");

        MergeEntries(response.Entries);
    }

    /// <summary>
    /// Downloads the full global key map from the server (used by
    /// <see cref="Collections.RemoteDynamicCollection"/> so that any field
    /// name can be resolved when decoding returned documents).
    /// Requires <c>Query</c> permission on <paramref name="collection"/>.
    /// </summary>
    internal async Task LoadFullMapAsync(
        string collection,
        CancellationToken ct = default)
    {
        if (_fullMapLoaded) return;

        await _fullMapLock.WaitAsync(ct);
        try
        {
            if (_fullMapLoaded) return; // double-checked

            var response = await _stub.GetKeyMapAsync(
                new KeyMapRequest { Collection = collection },
                _headers,
                cancellationToken: ct);

            if (!string.IsNullOrEmpty(response.Error))
                throw new InvalidOperationException(
                    $"GetKeyMap failed: {response.Error}");

            MergeEntries(response.Entries);
            _fullMapLoaded = true;
        }
        finally
        {
            _fullMapLock.Release();
        }
    }

    /// <summary>
    /// Forces a full reload from the server (e.g. after reconnect or when new
    /// fields have been added by other clients).
    /// </summary>
    internal async Task RefreshAsync(string collection, CancellationToken ct = default)
    {
        await _fullMapLock.WaitAsync(ct);
        try
        {
            _fullMapLoaded = false;
        }
        finally
        {
            _fullMapLock.Release();
        }

        await LoadFullMapAsync(collection, ct);
    }

    /// <summary>Forward map (field name → ushort id) — pass to <c>BsonSpanWriter</c>.</summary>
    internal ConcurrentDictionary<string, ushort> Forward => _forward;

    /// <summary>Reverse map (ushort id → field name) — pass to <c>BsonSpanReader</c> / <c>BsonDocument</c> ctor.</summary>
    internal ConcurrentDictionary<ushort, string> Reverse => _reverse;

    // ── Helpers ───────────────────────────────────────────────────────────────

    private void MergeEntries(IDictionary<string, uint> entries)
    {
        foreach (var (name, raw) in entries)
        {
            var id = (ushort)raw;
            _forward.TryAdd(name, id);
            _reverse.TryAdd(id, name);
        }
    }
}
