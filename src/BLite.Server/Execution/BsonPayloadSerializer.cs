// BLite.Server — BsonPayloadSerializer
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Thin helpers to convert BsonDocument ↔ raw byte[] for transport.
// BsonDocument stores raw BSON data internally (accessible via RawData).
// Construction from bytes is handled directly by the BsonDocument constructor.

using BLite.Bson;

namespace BLite.Server.Execution;

public static class BsonPayloadSerializer
{
    /// <summary>
    /// Serializes a <see cref="BsonDocument"/> to a byte array.
    /// BsonDocument internally stores the raw BSON bytes; this simply copies the span.
    /// </summary>
    public static byte[] Serialize(BsonDocument doc) =>
        doc.RawData.ToArray();

    /// <summary>Deserializes raw BSON bytes into a <see cref="BsonDocument"/>.</summary>
    public static BsonDocument Deserialize(byte[] bytes) =>
        new BsonDocument(bytes);

    /// <summary>Deserializes raw BSON bytes into a <see cref="BsonDocument"/>.</summary>
    public static BsonDocument Deserialize(ReadOnlySpan<byte> bytes) =>
        new BsonDocument(bytes.ToArray());
}
