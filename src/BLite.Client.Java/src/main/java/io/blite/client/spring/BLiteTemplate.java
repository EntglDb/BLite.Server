// BLite.Client.Java — BLiteTemplate
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client.spring;

import io.blite.client.BLiteClient;
import io.blite.client.BLiteCollection;
import io.blite.client.bson.BsonId;
import io.blite.client.query.QueryBuilder;
import io.blite.client.spring.annotation.BLiteDocument;
import io.blite.client.spring.annotation.BLiteId;

import java.lang.reflect.Field;
import java.util.*;

/**
 * Spring-style facade over {@link BLiteClient}.
 * Provides typed CRUD operations using reflection to map Java objects to/from
 * {@code Map<String, Object>} (the BLite dynamic-document model).
 *
 * <p>Entities must be annotated with {@link BLiteDocument} and have exactly one
 * field annotated with {@link BLiteId} whose type is {@link BsonId}.
 */
public class BLiteTemplate {

    private final BLiteClient client;

    public BLiteTemplate(BLiteClient client) {
        this.client = client;
    }

    // ── Write operations ──────────────────────────────────────────────────────

    public <T> BsonId insert(T entity) {
        var meta       = getMeta(entity.getClass());
        var collection = client.getCollection(meta.collectionName);
        var doc        = toMap(entity, meta);
        var id         = collection.insert(doc);
        setId(entity, meta.idField, id);
        return id;
    }

    public <T> BsonId insert(String collectionName, T entity) {
        var meta = getMeta(entity.getClass());
        var doc  = toMap(entity, meta);
        var id   = client.getCollection(collectionName).insert(doc);
        setId(entity, meta.idField, id);
        return id;
    }

    public <T> boolean update(T entity) {
        var meta       = getMeta(entity.getClass());
        var collection = client.getCollection(meta.collectionName);
        var id         = getId(entity, meta.idField);
        var doc        = toMap(entity, meta);
        return collection.update(id, doc);
    }

    public <T> boolean update(String collectionName, BsonId id, T entity) {
        var meta = getMeta(entity.getClass());
        return client.getCollection(collectionName).update(id, toMap(entity, meta));
    }

    public boolean delete(String collectionName, BsonId id) {
        return client.getCollection(collectionName).delete(id);
    }

    // ── Read operations ───────────────────────────────────────────────────────

    public <T> Optional<T> findById(BsonId id, Class<T> type) {
        var meta = getMeta(type);
        return findById(meta.collectionName, id, type);
    }

    public <T> Optional<T> findById(String collectionName, BsonId id, Class<T> type) {
        var meta = getMeta(type);
        return client.getCollection(collectionName)
                .findById(id)
                .map(doc -> fromMap(doc, type, meta, id));
    }

    public <T> List<T> findAll(Class<T> type) {
        var meta = getMeta(type);
        return findAll(meta.collectionName, type);
    }

    public <T> List<T> findAll(String collectionName, Class<T> type) {
        var meta    = getMeta(type);
        var results = new ArrayList<T>();
        for (var doc : client.getCollection(collectionName).findAll())
            results.add(fromMap(doc, type, meta, null));
        return results;
    }

    public <T> QueryBuilder<Map<String, Object>> query(Class<T> type) {
        return client.getCollection(getMeta(type).collectionName).query();
    }

    public BLiteCollection getCollection(String name) {
        return client.getCollection(name);
    }

    // ── Reflection helpers ────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private static <T> Map<String, Object> toMap(T entity, EntityMeta meta) {
        var map = new LinkedHashMap<String, Object>();
        for (var field : entity.getClass().getDeclaredFields()) {
            if (field.equals(meta.idField)) continue; // id is assigned by server
            field.setAccessible(true);
            try {
                var value = field.get(entity);
                if (value != null) map.put(field.getName().toLowerCase(), value);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Cannot read field " + field.getName(), e);
            }
        }
        return map;
    }

    private static <T> T fromMap(Map<String, Object> doc, Class<T> type,
                                 EntityMeta meta, BsonId knownId) {
        try {
            var instance = type.getDeclaredConstructor().newInstance();
            for (var field : type.getDeclaredFields()) {
                field.setAccessible(true);
                if (field.equals(meta.idField)) {
                    // id comes from the document's _id field or from knownId
                    var idVal = doc.get("_id");
                    if (idVal instanceof BsonId bid) field.set(instance, bid);
                    else if (knownId != null)        field.set(instance, knownId);
                    continue;
                }
                var value = doc.get(field.getName().toLowerCase());
                if (value != null) {
                    field.set(instance, coerce(value, field.getType()));
                }
            }
            return instance;
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Cannot deserialize " + type.getSimpleName(), e);
        }
    }

    private static Object coerce(Object value, Class<?> targetType) {
        if (targetType.isInstance(value)) return value;
        if (targetType == int.class || targetType == Integer.class)
            return value instanceof Number n ? n.intValue()   : Integer.parseInt(value.toString());
        if (targetType == long.class || targetType == Long.class)
            return value instanceof Number n ? n.longValue()  : Long.parseLong(value.toString());
        if (targetType == double.class || targetType == Double.class)
            return value instanceof Number n ? n.doubleValue(): Double.parseDouble(value.toString());
        if (targetType == float.class || targetType == Float.class)
            return value instanceof Number n ? n.floatValue() : Float.parseFloat(value.toString());
        if (targetType == boolean.class || targetType == Boolean.class)
            return value instanceof Boolean b ? b : Boolean.parseBoolean(value.toString());
        if (targetType == String.class) return value.toString();
        return value;
    }

    private static BsonId getId(Object entity, Field idField) {
        idField.setAccessible(true);
        try {
            var v = idField.get(entity);
            if (v instanceof BsonId bid) return bid;
            throw new IllegalStateException("@BLiteId field must hold a BsonId instance");
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static void setId(Object entity, Field idField, BsonId id) {
        idField.setAccessible(true);
        try { idField.set(entity, id); }
        catch (IllegalAccessException e) { throw new RuntimeException(e); }
    }

    // ── Metadata cache ────────────────────────────────────────────────────────

    private static final Map<Class<?>, EntityMeta> META_CACHE = new WeakHashMap<>();

    private static EntityMeta getMeta(Class<?> type) {
        return META_CACHE.computeIfAbsent(type, t -> {
            var ann = t.getAnnotation(BLiteDocument.class);
            if (ann == null)
                throw new IllegalArgumentException(
                        t.getSimpleName() + " must be annotated with @BLiteDocument");
            Field idField = null;
            for (var field : t.getDeclaredFields()) {
                if (field.isAnnotationPresent(BLiteId.class)) {
                    idField = field;
                    break;
                }
            }
            if (idField == null)
                throw new IllegalArgumentException(
                        t.getSimpleName() + " must have exactly one @BLiteId field");
            return new EntityMeta(ann.value(), idField);
        });
    }

    private record EntityMeta(String collectionName, Field idField) {}
}
