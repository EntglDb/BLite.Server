// BLite.Client.Java — QuerySerializer
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client.query;

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

/**
 * Serializes a {@link QueryDescriptor} into the MessagePack wire format consumed
 * by the BLite server's {@code QueryDescriptorSerializer.Deserialize}.
 *
 * <p>Wire layout — 6-element array:
 * {@code [collection, where, select, orderBy, take, skip]}
 *
 * <p>FilterNode encoding — 2-element array {@code [tag, args]}:
 * <ul>
 *   <li>BinaryFilter (tag=0): {@code [0, [field, op, scalar]]}
 *   <li>LogicalFilter (tag=1): {@code [1, [[child0], [child1], ...]]}
 *   <li>UnaryFilter   (tag=2): {@code [2, [[operand]]]}
 * </ul>
 *
 * <p>ScalarValue encoding — 11-element array indexed by {@link ScalarKind}:
 * {@code [kind, boolVal, int32Val, int64Val, doubleVal, decimalVal, stringVal,
 *          dateTimeVal, guidVal, objectIdVal, arrayVal]}
 * Null slots are written as msgpack nil.
 *
 * <p>DateTime is packed as msgpack Timestamp extension type (ext type -1 / 0xFF),
 * 8-byte big-endian: {@code (nanos << 34) | epochSecond}.
 */
public final class QuerySerializer {

    private QuerySerializer() {}

    public static byte[] serialize(QueryDescriptor qd) {
        try (MessageBufferPacker p = MessagePack.newDefaultBufferPacker()) {
            // 6-element top-level array
            p.packArrayHeader(6);
            p.packString(qd.collection());
            packFilterNode(p, qd.where());
            p.packNil();                               // select — not used by Java client
            packOrderBy(p, qd.orderBy());
            packNullableInt(p, qd.take());
            packNullableInt(p, qd.skip());
            return p.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("QueryDescriptor serialization failed", e);
        }
    }

    // ── FilterNode ───────────────────────────────────────────────────────────

    private static void packFilterNode(MessageBufferPacker p, FilterNode node) throws IOException {
        if (node == null) {
            p.packNil();
            return;
        }
        switch (node) {
            case BinaryFilter bf -> {
                p.packArrayHeader(2);
                p.packInt(0);
                p.packArrayHeader(3);
                p.packString(bf.field());
                p.packInt(bf.op().code);
                packScalarValue(p, bf.value());
            }
            case LogicalFilter lf -> {
                p.packArrayHeader(2);
                p.packInt(1);
                // inner array: [op, [children]]
                p.packArrayHeader(2);
                p.packInt(lf.op().code);
                p.packArrayHeader(lf.children().size());
                for (FilterNode child : lf.children()) packFilterNode(p, child);
            }
            case UnaryFilter uf -> {
                p.packArrayHeader(2);
                p.packInt(2);
                p.packArrayHeader(1);
                packFilterNode(p, uf.operand());
            }
        }
    }

    // ── ScalarValue ──────────────────────────────────────────────────────────

    private static void packScalarValue(MessageBufferPacker p, ScalarValue sv) throws IOException {
        // 11-element array: [kind, bool, int32, int64, double, decimal, string,
        //                    dateTime, guid, objectId, array]
        // decimal slot (index 5) is always nil — no BigDecimal support yet
        p.packArrayHeader(11);
        p.packInt(sv.kind.ordinal());

        packOrNil(p, sv.boolVal,     () -> p.packBoolean(sv.boolVal));
        packOrNil(p, sv.int32Val,    () -> p.packInt(sv.int32Val));
        packOrNil(p, sv.int64Val,    () -> p.packLong(sv.int64Val));
        packOrNil(p, sv.doubleVal,   () -> p.packDouble(sv.doubleVal));
        p.packNil(); // decimal slot

        packOrNil(p, sv.stringVal,   () -> p.packString(sv.stringVal));
        packDateTime(p, sv.dateTimeVal);
        packOrNil(p, sv.guidVal,     () -> p.packString(sv.guidVal));
        packOrNil(p, sv.objectIdVal, () -> p.packBinaryHeader(sv.objectIdVal.length));
        if (sv.objectIdVal != null) p.addPayload(sv.objectIdVal);

        packOrNil(p, sv.arrayVal, () -> {
            p.packArrayHeader(sv.arrayVal.size());
            for (ScalarValue item : sv.arrayVal) packScalarValue(p, item);
        });
    }

    /** Packs msgpack Timestamp ext (-1 / 0xFF), 8-byte big-endian. */
    private static void packDateTime(MessageBufferPacker p, Instant instant) throws IOException {
        if (instant == null) {
            p.packNil();
            return;
        }
        long epochSecond = instant.getEpochSecond();
        int  nanos       = instant.getNano();
        long tsValue     = ((long) nanos << 34) | epochSecond;
        byte[] bytes = new byte[8];
        for (int i = 7; i >= 0; i--) {
            bytes[i] = (byte) (tsValue & 0xFF);
            tsValue >>= 8;
        }
        p.packExtensionTypeHeader((byte) -1, 8);
        p.addPayload(bytes);
    }

    // ── OrderBy ──────────────────────────────────────────────────────────────

    private static void packOrderBy(MessageBufferPacker p, List<SortSpec> orderBy) throws IOException {
        if (orderBy == null || orderBy.isEmpty()) {
            p.packArrayHeader(0); // always an array, never nil — C# foreach throws on null
            return;
        }
        p.packArrayHeader(orderBy.size());
        for (SortSpec s : orderBy) {
            p.packArrayHeader(2);
            p.packString(s.field());
            p.packBoolean(s.descending());
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    @FunctionalInterface
    private interface IoRunnable { void run() throws IOException; }

    private static void packOrNil(MessageBufferPacker p, Object value, IoRunnable packer) throws IOException {
        if (value == null) p.packNil();
        else               packer.run();
    }

    private static void packNullableInt(MessageBufferPacker p, Integer value) throws IOException {
        if (value == null) p.packNil();
        else               p.packInt(value);
    }
}
