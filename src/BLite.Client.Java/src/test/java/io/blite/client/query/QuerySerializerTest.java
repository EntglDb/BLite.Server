// BLite.Client.Java — QuerySerializerTest
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client.query;

import org.junit.jupiter.api.Test;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.io.IOException;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class QuerySerializerTest {

    @Test
    void simple_eq_serializes_correctly() throws IOException {
        var qd    = new QueryDescriptor("products",
                FilterNode.eq("name", "Widget"),
                null, null, null);
        var bytes = QuerySerializer.serialize(qd);

        try (MessageUnpacker u = MessagePack.newDefaultUnpacker(bytes)) {
            assertEquals(6,          u.unpackArrayHeader()); // top-level 6-array
            assertEquals("products", u.unpackString());      // collection

            // where = [0, ["name", 0 (EQ), scalar]]
            assertEquals(2, u.unpackArrayHeader());
            assertEquals(0, u.unpackInt()); // tag=0 (binary)
            assertEquals(3, u.unpackArrayHeader());
            assertEquals("name", u.unpackString());
            assertEquals(FilterOp.EQ.code, u.unpackInt());

            // ScalarValue: 11-element array
            assertEquals(11, u.unpackArrayHeader());
            assertEquals(ScalarKind.STRING.ordinal(), u.unpackInt()); // kind
            u.unpackNil();  // bool
            u.unpackNil();  // int32
            u.unpackNil();  // int64
            u.unpackNil();  // double
            u.unpackNil();  // decimal
            assertEquals("Widget", u.unpackString()); // stringVal
        }
    }

    @Test
    void null_filter_serializes_as_nil() throws IOException {
        var qd    = new QueryDescriptor("col", null, null, null, null);
        var bytes = QuerySerializer.serialize(qd);
        try (MessageUnpacker u = MessagePack.newDefaultUnpacker(bytes)) {
            assertEquals(6,     u.unpackArrayHeader());
            assertEquals("col", u.unpackString());
            assertTrue(u.tryUnpackNil()); // where=nil
        }
    }

    @Test
    void datetime_serialized_as_ext_minus1() throws IOException {
        var now   = Instant.parse("2024-06-01T12:00:00Z");
        var qd    = new QueryDescriptor("col",
                FilterNode.eq("created", now),
                null, null, null);
        var bytes = QuerySerializer.serialize(qd);

        try (MessageUnpacker u = MessagePack.newDefaultUnpacker(bytes)) {
            u.unpackArrayHeader(); // 6
            u.unpackString();      // col
            u.unpackArrayHeader(); // 2
            u.unpackInt();         // tag=0
            u.unpackArrayHeader(); // 3
            u.unpackString();      // field
            u.unpackInt();         // EQ

            // ScalarValue
            u.unpackArrayHeader(); // 11
            u.unpackInt();         // kind = DATE_TIME
            u.unpackNil(); u.unpackNil(); u.unpackNil(); u.unpackNil(); u.unpackNil(); u.unpackNil();
            // dateTimeVal — ext type -1
            var ext = u.unpackExtensionTypeHeader();
            assertEquals((byte) -1, ext.getType());
            assertEquals(8,          ext.getLength());
            var payload = u.readPayload(8);
            assertNotNull(payload);
            assertEquals(8, payload.length);
        }
    }

    @Test
    void orderby_serialized() throws IOException {
        var qd = new QueryDescriptor("col", null,
                java.util.List.of(SortSpec.desc("price")), 10, 5);
        var bytes = QuerySerializer.serialize(qd);
        try (MessageUnpacker u = MessagePack.newDefaultUnpacker(bytes)) {
            u.unpackArrayHeader(); // 6
            u.unpackString();      // col
            u.tryUnpackNil();      // where=nil
            u.tryUnpackNil();      // select=nil
            assertEquals(1,       u.unpackArrayHeader());
            assertEquals(2,       u.unpackArrayHeader()); // [field, desc]
            assertEquals("price", u.unpackString());
            assertTrue(u.unpackBoolean()); // descending=true
            assertEquals(10, u.unpackInt()); // take
            assertEquals(5,  u.unpackInt()); // skip
        }
    }
}
