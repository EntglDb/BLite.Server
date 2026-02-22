// BLite.Server — BsonIdSerializer
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Maps between BLite.Bson.BsonId and the proto BsonIdBytes message.

using BLite.Bson;
using BLite.Proto.V1;
using Google.Protobuf;

namespace BLite.Server.Execution;

public static class BsonIdSerializer
{
    public static BsonIdBytes ToProto(BsonId id)
    {
        return new BsonIdBytes
        {
            Value   = ByteString.CopyFrom(id.ToBytes()),
            IdType  = (int)id.IdType
        };
    }

    public static BsonId FromProto(BsonIdBytes proto)
    {
        var idType = (BsonIdType)proto.IdType;
        return BsonId.FromBytes(proto.Value.ToByteArray(), idType);
    }
}
