// BLite.Server — BsonPayloadSerializer
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Thin helpers to convert BsonDocument ↔ raw byte[] for transport.
// BLite.Bson already exposes a span-based writer; we use a pooled buffer here.

using BLite.Bson;
using System.Buffers;

namespace BLite.Server.Execution;

public static class BsonPayloadSerializer
{
    /// <summary>Serializes a <see cref="BsonDocument"/> to a new byte array.</summary>
    public static byte[] Serialize(BsonDocument doc)
    {
        // Estimate: 256 bytes initial capacity, doubled on demand.
        var buffer = new ArrayBufferWriter<byte>(256);
        doc.WriteTo(buffer);
        return buffer.WrittenSpan.ToArray();
    }

    /// <summary>Deserializes raw BSON bytes into a <see cref="BsonDocument"/>.</summary>
    public static BsonDocument Deserialize(byte[] bytes)
    {
        var reader = new BsonSpanReader(bytes);
        return BsonDocument.ReadFrom(ref reader);
    }

    /// <summary>Deserializes raw BSON bytes into a <see cref="BsonDocument"/>.</summary>
    public static BsonDocument Deserialize(ReadOnlySpan<byte> bytes)
    {
        var reader = new BsonSpanReader(bytes);
        return BsonDocument.ReadFrom(ref reader);
    }
}
