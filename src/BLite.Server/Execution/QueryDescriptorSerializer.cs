// BLite.Server — QueryDescriptorSerializer
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

using BLite.Proto;
using MessagePack;

namespace BLite.Server.Execution;

/// <summary>
/// Serializes/deserializes <see cref="QueryDescriptor"/> using MessagePack.
/// The client packs the descriptor into <c>QueryRequest.query_descriptor</c> bytes;
/// the server unpacks it here before handing it to <see cref="QueryDescriptorExecutor"/>.
/// </summary>
public static class QueryDescriptorSerializer
{
    private static readonly MessagePackSerializerOptions Options =
        MessagePackSerializerOptions.Standard.WithCompression(MessagePackCompression.Lz4Block);

    public static byte[] Serialize(QueryDescriptor descriptor) =>
        MessagePackSerializer.Serialize(descriptor, Options);

    public static QueryDescriptor Deserialize(byte[] bytes) =>
        MessagePackSerializer.Deserialize<QueryDescriptor>(bytes, Options);

    public static QueryDescriptor Deserialize(ReadOnlyMemory<byte> bytes) =>
        MessagePackSerializer.Deserialize<QueryDescriptor>(bytes, Options);
}
