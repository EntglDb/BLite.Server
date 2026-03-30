// BLite.Server — persists a stable GUID instance ID in the database
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

using BLite.Bson;
using BLite.Core;

namespace BLite.Server.License;

// Generates or loads a stable GUID that uniquely identifies this BLite.Server
// instance across restarts.  The ID is stored as a key-value entry in the
// system engine's _meta collection so it survives process restarts and is
// never user-visible.
public sealed class InstanceIdProvider
{
    private const string CollectionName = "_meta";
    private const string KeyField       = "_key";
    private const string ValueField     = "_value";
    private const string InstanceIdKey  = "instance_id";

    private string? _instanceId;

    public InstanceIdProvider(EngineRegistry engineRegistry)
    {
        _instanceId = Load(engineRegistry.SystemEngine).GetAwaiter().GetResult();
        if (_instanceId is null)
        {
            _instanceId = Guid.NewGuid().ToString("N");
            Persist(engineRegistry.SystemEngine, _instanceId);
        }
    }

    public string InstanceId => _instanceId!;

    // ── persistence ───────────────────────────────────────────────────────────

    private static async Task<string?> Load(BLiteEngine engine)
    {
        var collection = engine.GetOrCreateCollection(CollectionName);
        await foreach (var doc in collection.FindAllAsync())
        {
            if (doc.TryGetString(KeyField, out var key) && key == InstanceIdKey
                && doc.TryGetString(ValueField, out var value))
                return value;
        }
        return null;
    }

    private static void Persist(BLiteEngine engine, string id)
    {
        var col = engine.GetOrCreateCollection(CollectionName);
        var doc = col.CreateDocument(
            [KeyField, ValueField],
            b => b
                .AddString(KeyField,   InstanceIdKey)
                .AddString(ValueField, id));

        col.InsertAsync(doc, CancellationToken.None).GetAwaiter().GetResult();
    }
}
