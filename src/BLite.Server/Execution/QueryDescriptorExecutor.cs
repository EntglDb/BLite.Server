// BLite.Server — QueryDescriptorExecutor
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Translates a QueryDescriptor into a BLiteEngine query and streams BsonDocuments back.
// This is the server-side counterpart of BTreeQueryProvider — it speaks QueryDescriptor
// instead of Expression<T> and delegates to DynamicCollection.Scan / FindAll.

using BLite.Bson;
using BLite.Core;
using BLite.Proto;

namespace BLite.Server.Execution;

public static class QueryDescriptorExecutor
{
    /// <summary>
    /// Executes the query described by <paramref name="descriptor"/> against
    /// <paramref name="engine"/> and returns a streaming async sequence of matching documents.
    /// </summary>
    public static async IAsyncEnumerable<BsonDocument> ExecuteAsync(
        BLiteEngine engine,
        QueryDescriptor descriptor,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var collection = engine.GetOrCreateCollection(descriptor.Collection);

        // Build an optional predicate from the WHERE clause
        Func<BsonDocument, bool>? predicate = descriptor.Where is not null
            ? FilterNodeCompiler.Compile(descriptor.Where)
            : null;

        // Determine the raw source sequence
        IAsyncEnumerable<BsonDocument> source = predicate is not null
            ? collection.FindAsync(predicate, ct)
            : collection.FindAllAsync(ct);

        // Apply ORDER BY (in-memory post-scan, same as BTreeQueryProvider standard path)
        IEnumerable<BsonDocument>? sorted = null;
        foreach (var sort in descriptor.OrderBy)
        {
            if (sorted is null)
            {
                var materialized = new List<BsonDocument>();
                await foreach (var d in source.WithCancellation(ct))
                    materialized.Add(d);

                sorted = sort.Descending
                    ? materialized.OrderByDescending(d => GetField(d, sort.Field))
                    : materialized.OrderBy(d => GetField(d, sort.Field));
            }
            else
            {
                sorted = sort.Descending
                    ? ((IOrderedEnumerable<BsonDocument>)sorted).ThenByDescending(d => GetField(d, sort.Field))
                    : ((IOrderedEnumerable<BsonDocument>)sorted).ThenBy(d => GetField(d, sort.Field));
            }
        }

        // Yield results — either from sorted in-memory or streamed directly
        if (sorted is not null)
        {
            var seq = sorted;
            if (descriptor.Skip.HasValue) seq = seq.Skip(descriptor.Skip.Value);
            if (descriptor.Take.HasValue) seq = seq.Take(descriptor.Take.Value);

            foreach (var doc in seq)
            {
                ct.ThrowIfCancellationRequested();
                yield return doc;
            }
        }
        else
        {
            int skipped = 0;
            int taken   = 0;

            await foreach (var doc in source.WithCancellation(ct))
            {
                if (descriptor.Skip.HasValue && skipped < descriptor.Skip.Value)
                {
                    skipped++;
                    continue;
                }
                if (descriptor.Take.HasValue && taken >= descriptor.Take.Value)
                    yield break;

                taken++;
                yield return doc;
            }
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static object? GetField(BsonDocument doc, string fieldPath)
    {
        // Simple dot-path traversal: "address.city" → doc["address"]["city"]
        var parts = fieldPath.Split('.');
        object? current = doc;
        foreach (var part in parts)
        {
            if (current is BsonDocument d)
                current = d.TryGet(part, out var v) ? v : null;
            else
                return null;
        }
        return current;
    }
}
