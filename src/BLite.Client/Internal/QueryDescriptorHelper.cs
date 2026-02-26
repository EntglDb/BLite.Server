// BLite.Client — QueryDescriptorHelper
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Serializes/deserializes QueryDescriptor with the same options as the server
// (MessagePack + Lz4Block compression) so bytes are wire-compatible.

using BLite.Proto;
using Google.Protobuf;
using MessagePack;

namespace BLite.Client.Internal;

internal static class QueryDescriptorHelper
{
    private static readonly MessagePackSerializerOptions Options =
        MessagePackSerializerOptions.Standard
            .WithCompression(MessagePackCompression.Lz4Block);

    internal static ByteString Serialize(QueryDescriptor descriptor) =>
        ByteString.CopyFrom(MessagePackSerializer.Serialize(descriptor, Options));

    internal static QueryDescriptor Deserialize(byte[] bytes) =>
        MessagePackSerializer.Deserialize<QueryDescriptor>(bytes, Options);
}
