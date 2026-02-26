// BLite.Server — EmbeddingTask
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

using BLite.Bson;

namespace BLite.Server.Embedding;

/// <summary>
/// A task in the embedding queue.
/// <see cref="Status"/> is computed based on <see cref="RawStatus"/> and elapsed time since <see cref="ChangedAt"/>.
/// </summary>
public sealed record EmbeddingTask(
    BsonId Id,
    string Key,              // "{dbId}:{collection}:{docId}" — used for dedup
    string? Database,        // null = system, otherwise tenant ID
    string Collection,
    BsonId DocumentId,
    DateTime EnqueuedAt,
    DateTime ChangedAt,
    string RawStatus)        // "todo", "in_progress", "done"
{
    /// <summary>The computed status based on RawStatus and elapsed time.</summary>
    public EmbeddingTaskStatus Status => GetStatus(EmbeddingWorkerOptions.DefaultStaleTimeoutMinutes);

    /// <summary>Computes status using a runtime-provided stale threshold.</summary>
    public EmbeddingTaskStatus GetStatus(int staleTimeoutMinutes) => RawStatus switch
    {
        "todo"        => EmbeddingTaskStatus.Todo,
        "done"        => EmbeddingTaskStatus.Done,
        "in_progress" => (DateTime.UtcNow - ChangedAt).TotalMinutes > staleTimeoutMinutes
                            ? EmbeddingTaskStatus.Stale
                            : EmbeddingTaskStatus.InProgress,
        _             => EmbeddingTaskStatus.Todo
    };

    /// <summary>Creates an EmbeddingTask from a BSON document pulled from _emb_queue.</summary>
    public static EmbeddingTask FromDocument(BsonDocument doc)
    {
        var id = doc.TryGetId(out var taskId) ? taskId : BsonId.NewId();
        var key = doc.TryGetValue("key", out var kv) ? kv.AsString : "";
        var db = doc.TryGetValue("db", out var dv) ? (dv.AsString == "" ? null : dv.AsString) : null;
        var col = doc.TryGetValue("col", out var cv) ? cv.AsString : "";
        var docIdVal = doc.TryGetValue("doc", out var doval) ? BsonValueToId(doval) : BsonId.NewId();
        var enqueuedAt = doc.TryGetValue("enqueuedAt", out var ev) ? ev.AsDateTime : DateTime.UtcNow;
        var changedAt = doc.TryGetValue("changedAt", out var cav) ? cav.AsDateTime : DateTime.UtcNow;
        var status = doc.TryGetValue("rawStatus", out var sv) ? sv.AsString : "todo";

        return new EmbeddingTask(id, key, db, col, docIdVal, enqueuedAt, changedAt, status);
    }

    private static BsonId BsonValueToId(BsonValue v) => v.Type switch
    {
        BsonType.ObjectId => new BsonId(v.AsObjectId),
        BsonType.Int32    => new BsonId(v.AsInt32),
        BsonType.Int64    => new BsonId(v.AsInt64),
        BsonType.String   => new BsonId(v.AsString),
        _                 => new BsonId(v.AsString)
    };
}
