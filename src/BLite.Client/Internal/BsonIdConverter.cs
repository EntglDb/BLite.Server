// BLite.Client — BsonIdConverter
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Maps between BLite.Bson.BsonId and the proto BsonIdBytes message.
// Mirrors BLite.Server.Execution.BsonIdSerializer on the server side.

using BLite.Bson;
using BLite.Proto.V1;
using Google.Protobuf;

namespace BLite.Client.Internal;

internal static class BsonIdConverter
{
    internal static BsonIdBytes ToProto(BsonId id) => new()
    {
        Value  = ByteString.CopyFrom(id.ToBytes()),
        IdType = (int)id.Type
    };

    internal static BsonId FromProto(BsonIdBytes proto) =>
        BsonId.FromBytes(proto.Value.ToByteArray(), (BsonIdType)proto.IdType);
}
