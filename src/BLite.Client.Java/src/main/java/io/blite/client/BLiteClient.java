// BLite.Client.Java — BLiteClient
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client;

import io.blite.client.admin.BLiteAdminClient;
import io.blite.client.kv.BLiteKvStore;
import io.blite.proto.v1.*;
import io.grpc.*;
import io.grpc.stub.MetadataUtils;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Entry point for the BLite Java client.
 *
 * <p>A single {@code BLiteClient} maintains one gRPC channel with a shared
 * {@link ClientKeyMap}. Reuse the same instance across the application lifetime.
 * Close with try-with-resources or call {@link #close()} explicitly.
 *
 * <pre>{@code
 * try (var client = new BLiteClient(BLiteClientOptions.local("my-api-key"))) {
 *     var col = client.getCollection("products");
 *     var id  = col.insert(Map.of("name", "Widget", "price", 9.99));
 * }
 * }</pre>
 */
public final class BLiteClient implements AutoCloseable {

    private static final Metadata.Key<String> API_KEY_HEADER =
            Metadata.Key.of("x-api-key", Metadata.ASCII_STRING_MARSHALLER);

    private final ManagedChannel                                         channel;
    private final ClientKeyMap                                           keyMap;
    private final DynamicServiceGrpc.DynamicServiceBlockingStub         dynamic;
    private final MetadataServiceGrpc.MetadataServiceBlockingStub       meta;
    private final TransactionServiceGrpc.TransactionServiceBlockingStub txn;

    public final BLiteAdminClient admin;
    public final BLiteKvStore     kv;

    public BLiteClient(BLiteClientOptions opts) {
        var apiKey = opts.getApiKey();
        if (apiKey == null || apiKey.isBlank())
            throw new IllegalArgumentException("apiKey must not be blank");

        channel = buildChannel(opts);
        keyMap  = new ClientKeyMap();

        var interceptor = MetadataUtils.newAttachHeadersInterceptor(apiKeyHeaders(apiKey));
        dynamic = DynamicServiceGrpc.newBlockingStub(channel).withInterceptors(interceptor);
        meta    = MetadataServiceGrpc.newBlockingStub(channel).withInterceptors(interceptor);
        txn     = TransactionServiceGrpc.newBlockingStub(channel).withInterceptors(interceptor);

        admin = new BLiteAdminClient(channel, apiKey);
        kv    = new BLiteKvStore(channel, apiKey);
    }

    // ── Collection access ─────────────────────────────────────────────────────

    public BLiteCollection getCollection(String name) {
        return new BLiteCollection(name, dynamic, meta, keyMap);
    }

    // ── Transactions ──────────────────────────────────────────────────────────

    public BLiteTransaction beginTransaction() {
        var resp = txn.begin(BeginTransactionRequest.newBuilder().build());
        BLiteError.check(resp.getError());
        return new BLiteTransaction(resp.getTransactionId(), txn);
    }

    // ── Collection management ─────────────────────────────────────────────────

    public List<String> listCollections() {
        return dynamic.listCollections(Empty.newBuilder().build()).getNamesList();
    }

    public void dropCollection(String name) {
        var resp = dynamic.dropCollection(DropCollectionRequest.newBuilder()
                .setCollection(name).build());
        BLiteError.check(resp.getError());
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    @Override
    public void close() {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            channel.shutdownNow();
        }
    }

    // ── Channel construction ──────────────────────────────────────────────────

    private static ManagedChannel buildChannel(BLiteClientOptions opts) {
        var target = opts.getHost() + ":" + opts.getPort();
        if (opts.isUseTls()) {
            return ManagedChannelBuilder.forTarget(target)
                    .useTransportSecurity()
                    .build();
        } else {
            return ManagedChannelBuilder.forTarget(target)
                    .usePlaintext()
                    .build();
        }
    }

    private static Metadata apiKeyHeaders(String apiKey) {
        var headers = new Metadata();
        headers.put(API_KEY_HEADER, apiKey);
        return headers;
    }

}
