// BLite.Client.Java — BLiteKvStore
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client.kv;

import com.google.protobuf.ByteString;
import io.blite.client.BLiteError;
import io.blite.proto.v1.*;
import io.grpc.ManagedChannel;
import io.grpc.stub.MetadataUtils;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * Persistent key-value store backed by the same database file.
 * Keys are UTF-8 strings (≤ 255 bytes); values are arbitrary byte arrays.
 */
public final class BLiteKvStore {

    private static final io.grpc.Metadata.Key<String> API_KEY_HEADER =
            io.grpc.Metadata.Key.of("x-api-key", io.grpc.Metadata.ASCII_STRING_MARSHALLER);

    private final KvServiceGrpc.KvServiceBlockingStub stub;

    public BLiteKvStore(ManagedChannel channel, String apiKey) {
        var headers = new io.grpc.Metadata();
        headers.put(API_KEY_HEADER, apiKey);
        stub = KvServiceGrpc.newBlockingStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    }

    // ── Read ──────────────────────────────────────────────────────────────────

    public Optional<byte[]> get(String key) {
        var resp = stub.get(KvGetRequest.newBuilder().setKey(key).build());
        BLiteError.check(resp.getError());
        if (!resp.getFound() || resp.getValue().isEmpty()) return Optional.empty();
        return Optional.of(resp.getValue().toByteArray());
    }

    public Optional<String> getString(String key) {
        return get(key).map(b -> new String(b, StandardCharsets.UTF_8));
    }

    public boolean exists(String key) {
        var resp = stub.exists(KvKeyRequest.newBuilder().setKey(key).build());
        BLiteError.check(resp.getError());
        return resp.getExists();
    }

    public List<String> scanKeys(String prefix) {
        var resp = stub.scanKeys(KvScanRequest.newBuilder().setPrefix(prefix).build());
        BLiteError.check(resp.getError());
        return resp.getKeysList();
    }

    // ── Write ─────────────────────────────────────────────────────────────────

    public void set(String key, byte[] value) {
        set(key, value, 0);
    }

    public void set(String key, String value) {
        set(key, value.getBytes(StandardCharsets.UTF_8), 0);
    }

    public void set(String key, byte[] value, long ttlMs) {
        var resp = stub.set(KvSetRequest.newBuilder()
                .setKey(key)
                .setValue(ByteString.copyFrom(value))
                .setTtlMs(ttlMs)
                .build());
        BLiteError.check(resp.getError());
    }

    public void set(String key, String value, long ttlMs) {
        set(key, value.getBytes(StandardCharsets.UTF_8), ttlMs);
    }

    public void delete(String key) {
        var resp = stub.delete(KvDeleteRequest.newBuilder().setKey(key).build());
        BLiteError.check(resp.getError());
    }

    public void refresh(String key, long ttlMs) {
        var resp = stub.refresh(KvRefreshRequest.newBuilder()
                .setKey(key).setTtlMs(ttlMs).build());
        BLiteError.check(resp.getError());
    }

    // ── Batch ─────────────────────────────────────────────────────────────────

    public int batch(KvBatch batch) {
        var resp = stub.batch(KvBatchRequest.newBuilder()
                .addAllOperations(batch.ops())
                .build());
        BLiteError.check(resp.getError());
        return resp.getAffectedCount();
    }

    // ── Maintenance ───────────────────────────────────────────────────────────

    public int purgeExpired() {
        var resp = stub.purgeExpired(KvDbRequest.newBuilder().build());
        BLiteError.check(resp.getError());
        return resp.getPurgedCount();
    }
}
