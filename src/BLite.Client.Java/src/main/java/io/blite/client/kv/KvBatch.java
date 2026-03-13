// BLite.Client.Java — KvBatch
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client.kv;

import com.google.protobuf.ByteString;
import io.blite.proto.v1.KvBatchOp;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Builder for a KV batch operation.
 *
 * <pre>{@code
 * client.kv.batch(new KvBatch()
 *     .set("key1", "value1".getBytes(), 0)
 *     .delete("old-key"));
 * }</pre>
 */
public final class KvBatch {

    private final List<KvBatchOp> ops = new ArrayList<>();

    public KvBatch set(String key, byte[] value, long ttlMs) {
        ops.add(KvBatchOp.newBuilder()
                .setKey(key)
                .setValue(ByteString.copyFrom(value))
                .setTtlMs(ttlMs)
                .setIsDelete(false)
                .build());
        return this;
    }

    public KvBatch set(String key, String value, long ttlMs) {
        return set(key, value.getBytes(StandardCharsets.UTF_8), ttlMs);
    }

    public KvBatch delete(String key) {
        ops.add(KvBatchOp.newBuilder()
                .setKey(key)
                .setIsDelete(true)
                .build());
        return this;
    }

    List<KvBatchOp> ops() { return ops; }
}
