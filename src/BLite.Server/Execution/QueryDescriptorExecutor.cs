// BLite.Server — QueryDescriptorExecutor
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Translates a QueryDescriptor into a BLiteEngine query and streams BsonDocuments back.
// This is the server-side counterpart of BTreeQueryProvider — it speaks QueryDescriptor
// instead of Expression<T> and delegates to DynamicCollection.Scan / FindAll.

using BLite.Bson;
using BLite.Core;
using BLite.Core.Indexing;
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

        // Build predicate and source — prefer index seek for simple equality filters.
        Func<BsonDocument, bool>? predicate = null;
        IAsyncEnumerable<BsonDocument> source;

        if (descriptor.Where is BinaryFilter { Op: FilterOp.Eq } eq && !eq.Field.Contains('.'))
        {
            // Look for a BTree index on this field; if one exists, use a point-lookup
            // instead of a full-collection scan.
            var btreeIdx = engine.GetIndexDescriptors(descriptor.Collection)
                .FirstOrDefault(d => d.Type == IndexType.BTree && d.FieldPath == eq.Field);

            if (btreeIdx is not null)
            {
                var key = ScalarToObject(eq.Value);
                source = SyncToAsync(collection.QueryIndexAsync(btreeIdx.Name, key, key), ct);
                // predicate stays null — index equality scan is already exact
            }
            else
            {
                predicate = FilterNodeCompiler.Compile(descriptor.Where);
                source = collection.FindAsync(predicate, ct);
            }
        }
        else
        {
            predicate = descriptor.Where is not null
                ? FilterNodeCompiler.Compile(descriptor.Where)
                : null;
            source = predicate is not null
                ? collection.FindAsync(predicate, ct)
                : collection.FindAllAsync(ct);
        }

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
                current = d.TryGetValue(part, out var bv) ? BsonValueToObject(bv) : null;
            else
                return null;
        }
        return current;
    }

    /// <summary>Boxes a <see cref="BsonValue"/> into a CLR object for LINQ comparisons.</summary>
    private static object? BsonValueToObject(BsonValue v) => v.Type switch
    {
        BsonType.Null       => null,
        BsonType.Boolean    => (object)v.AsBoolean,
        BsonType.Int32      => (object)v.AsInt32,
        BsonType.Int64      => (object)v.AsInt64,
        BsonType.Double     => (object)v.AsDouble,
        BsonType.Decimal128 => (object)v.AsDecimal,
        BsonType.String     => v.AsString,
        BsonType.DateTime   => v.AsDateTime,
        BsonType.Document   => v.AsDocument,   // nested: traversal continues
        _                   => null
    };

    /// <summary>Converts a <see cref="ScalarValue"/> to a CLR object suitable as a BTree index key.</summary>
    private static object? ScalarToObject(ScalarValue v) => v.Kind switch
    {
        ScalarKind.Null     => null,
        ScalarKind.Bool     => v.BoolVal,
        ScalarKind.Int32    => v.Int32Val,
        ScalarKind.Int64    => v.Int64Val,
        ScalarKind.Double   => v.DoubleVal,
        ScalarKind.Decimal  => v.DecimalVal,
        ScalarKind.String   => v.StringVal,
        ScalarKind.DateTime => v.DateTimeVal,
        ScalarKind.Guid     => v.GuidVal,
        _                   => null
    };

    private static async IAsyncEnumerable<BsonDocument> SyncToAsync(
        IAsyncEnumerable<BsonDocument> source,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var doc in source)
        {
            ct.ThrowIfCancellationRequested();
            yield return doc;
        }
    }
}
