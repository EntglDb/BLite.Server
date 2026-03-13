// BLite.Client.Java — BLiteCollection
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client;

import com.google.protobuf.ByteString;
import io.blite.client.bson.BsonId;
import io.blite.client.bson.CbsonReader;
import io.blite.client.bson.CbsonWriter;
import io.blite.client.query.QueryBuilder;
import io.blite.client.query.QueryDescriptor;
import io.blite.client.query.QuerySerializer;
import io.blite.proto.v1.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Provides CRUD and query operations for a single named collection.
 * All write paths call {@link #ensureKeys} before encoding to guarantee the
 * client's key-ID map matches the server's global dictionary.
 */
public final class BLiteCollection {

    private final String collection;
    private final DynamicServiceGrpc.DynamicServiceBlockingStub dynamic;
    private final MetadataServiceGrpc.MetadataServiceBlockingStub meta;
    private final ClientKeyMap keyMap;

    BLiteCollection(String collection,
                    DynamicServiceGrpc.DynamicServiceBlockingStub dynamic,
                    MetadataServiceGrpc.MetadataServiceBlockingStub meta,
                    ClientKeyMap keyMap) {
        this.collection = collection;
        this.dynamic    = dynamic;
        this.meta       = meta;
        this.keyMap     = keyMap;
    }

    // ── Single-document writes ────────────────────────────────────────────────

    public BsonId insert(Map<String, Object> doc) {
        return insert(doc, null);
    }

    public BsonId insert(Map<String, Object> doc, BLiteTransaction tx) {
        ensureKeys(doc);
        var resp = dynamic.insert(InsertRequest.newBuilder()
                .setCollection(collection)
                .setBsonPayload(ByteString.copyFrom(CbsonWriter.encode(doc, keyMap.forward())))
                .setTransactionId(txId(tx))
                .build());
        BLiteError.check(resp.getError());
        return fromProto(resp.getId());
    }

    public boolean update(BsonId id, Map<String, Object> doc) {
        return update(id, doc, null);
    }

    public boolean update(BsonId id, Map<String, Object> doc, BLiteTransaction tx) {
        ensureKeys(doc);
        var resp = dynamic.update(UpdateRequest.newBuilder()
                .setCollection(collection)
                .setId(toProto(id))
                .setBsonPayload(ByteString.copyFrom(CbsonWriter.encode(doc, keyMap.forward())))
                .setTransactionId(txId(tx))
                .build());
        BLiteError.check(resp.getError());
        return resp.getSuccess();
    }

    public boolean delete(BsonId id) {
        return delete(id, null);
    }

    public boolean delete(BsonId id, BLiteTransaction tx) {
        var resp = dynamic.delete(DeleteRequest.newBuilder()
                .setCollection(collection)
                .setId(toProto(id))
                .setTransactionId(txId(tx))
                .build());
        BLiteError.check(resp.getError());
        return resp.getSuccess();
    }

    // ── Single-document reads ─────────────────────────────────────────────────

    public Optional<Map<String, Object>> findById(BsonId id) {
        var resp = dynamic.findById(FindByIdRequest.newBuilder()
                .setCollection(collection)
                .setId(toProto(id))
                .build());
        BLiteError.check(resp.getError());
        if (!resp.getFound() || resp.getBsonPayload().isEmpty()) return Optional.empty();
        return Optional.of(CbsonReader.decode(resp.getBsonPayload().toByteArray(), keyMap.reverse()));
    }

    // ── Bulk operations ───────────────────────────────────────────────────────

    public List<BsonId> insertBulk(List<Map<String, Object>> docs) {
        return insertBulk(docs, null);
    }

    public List<BsonId> insertBulk(List<Map<String, Object>> docs, BLiteTransaction tx) {
        if (docs.isEmpty()) return List.of();
        ensureKeys(docs.get(0)); // register keys from first doc; assume consistent shape
        var builder = BulkInsertRequest.newBuilder()
                .setCollection(collection)
                .setTransactionId(txId(tx));
        for (var doc : docs)
            builder.addPayloads(ByteString.copyFrom(CbsonWriter.encode(doc, keyMap.forward())));
        var resp = dynamic.insertBulk(builder.build());
        BLiteError.check(resp.getError());
        var ids = new ArrayList<BsonId>(resp.getIdsCount());
        for (var pbId : resp.getIdsList()) ids.add(fromProto(pbId));
        return ids;
    }

    public int updateBulk(List<UpdatePair> updates) {
        return updateBulk(updates, null);
    }

    public int updateBulk(List<UpdatePair> updates, BLiteTransaction tx) {
        if (updates.isEmpty()) return 0;
        ensureKeys(updates.get(0).document());
        var builder = BulkUpdateRequest.newBuilder()
                .setCollection(collection)
                .setTransactionId(txId(tx));
        for (var pair : updates)
            builder.addItems(BulkUpdateItem.newBuilder()
                    .setId(toProto(pair.id()))
                    .setBsonPayload(ByteString.copyFrom(CbsonWriter.encode(pair.document(), keyMap.forward())))
                    .build());
        var resp = dynamic.updateBulk(builder.build());
        BLiteError.check(resp.getError());
        return resp.getAffectedCount();
    }

    public int deleteBulk(List<BsonId> ids) {
        return deleteBulk(ids, null);
    }

    public int deleteBulk(List<BsonId> ids, BLiteTransaction tx) {
        if (ids.isEmpty()) return 0;
        var builder = BulkDeleteRequest.newBuilder()
                .setCollection(collection)
                .setTransactionId(txId(tx));
        for (var id : ids) builder.addIds(toProto(id));
        var resp = dynamic.deleteBulk(builder.build());
        BLiteError.check(resp.getError());
        return resp.getAffectedCount();
    }

    // ── Query ─────────────────────────────────────────────────────────────────

    public Iterable<Map<String, Object>> findAll() {
        return query().execute();
    }

    public QueryBuilder<Map<String, Object>> query() {
        return new QueryBuilder<>(collection, this::executeQuery);
    }

    private Iterable<Map<String, Object>> executeQuery(QueryDescriptor qd) {
        var bytes   = QuerySerializer.serialize(qd);
        var request = QueryRequest.newBuilder()
                .setQueryDescriptor(ByteString.copyFrom(bytes))
                .build();
        var stream = dynamic.query(request);
        var results = new ArrayList<Map<String, Object>>();
        while (stream.hasNext()) {
            var resp = stream.next();
            if (!resp.getBsonPayload().isEmpty())
                results.add(CbsonReader.decode(resp.getBsonPayload().toByteArray(), keyMap.reverse()));
        }
        return results;
    }

    // ── Key registration ───────────────────────────────────────────────────────

    private void ensureKeys(Map<String, Object> doc) {
        var missing = keyMap.missing(doc.keySet());
        if (missing.isEmpty()) return;
        var resp = meta.registerKeys(RegisterKeysRequest.newBuilder()
                .setCollection(collection)
                .addAllKeys(missing)
                .build());
        BLiteError.check(resp.getError());
        keyMap.merge(resp.getEntriesMap());
    }

    // ── Proto helpers ──────────────────────────────────────────────────────────

    private static BsonIdBytes toProto(BsonId id) {
        return BsonIdBytes.newBuilder()
                .setValue(id.toProtoValue())
                .setIdType(id.getType().code)
                .build();
    }

    private static BsonId fromProto(BsonIdBytes pb) {
        return BsonId.fromProto(pb.getValue(), pb.getIdType());
    }

    private static String txId(BLiteTransaction tx) {
        return tx != null && tx.isActive() ? tx.getTransactionId() : "";
    }
}
