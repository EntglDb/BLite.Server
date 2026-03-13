// BLite.Client.Java — BLiteRepositoryImpl
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client.spring.repository;

import io.blite.client.bson.BsonId;
import io.blite.client.spring.BLiteTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Default implementation of {@link BLiteRepository} backed by {@link BLiteTemplate}.
 *
 * @param <T>  entity type annotated with {@code @BLiteDocument}
 */
public class BLiteRepositoryImpl<T> implements BLiteRepository<T, BsonId> {

    private final BLiteTemplate template;
    private final Class<T>      entityType;

    public BLiteRepositoryImpl(BLiteTemplate template, Class<T> entityType) {
        this.template   = template;
        this.entityType = entityType;
    }

    @Override
    public <S extends T> S save(S entity) {
        template.insert(entity);
        return entity;
    }

    @Override
    public <S extends T> Iterable<S> saveAll(Iterable<S> entities) {
        var saved = new ArrayList<S>();
        for (S e : entities) { save(e); saved.add(e); }
        return saved;
    }

    @Override
    public Optional<T> findById(BsonId id) {
        return template.findById(id, entityType);
    }

    @Override
    public boolean existsById(BsonId id) {
        return findById(id).isPresent();
    }

    @Override
    public Iterable<T> findAll() {
        return template.findAll(entityType);
    }

    @Override
    public Iterable<T> findAllById(Iterable<BsonId> ids) {
        var results = new ArrayList<T>();
        for (var id : ids) findById(id).ifPresent(results::add);
        return results;
    }

    @Override
    public long count() {
        return template.findAll(entityType).size();
    }

    @Override
    public void deleteById(BsonId id) {
        var meta = getMeta();
        template.delete(meta, id);
    }

    @Override
    public void delete(T entity) {
        try {
            for (var field : entityType.getDeclaredFields()) {
                if (field.isAnnotationPresent(io.blite.client.spring.annotation.BLiteId.class)) {
                    field.setAccessible(true);
                    deleteById((BsonId) field.get(entity));
                    return;
                }
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        throw new IllegalStateException(entityType.getSimpleName() + " has no @BLiteId field");
    }

    @Override
    public void deleteAllById(Iterable<? extends BsonId> ids) {
        var meta = getMeta();
        for (var id : ids) template.delete(meta, id);
    }

    @Override
    public void deleteAll(Iterable<? extends T> entities) {
        for (var e : entities) {
            // Resolve id via reflection
            try {
                for (var field : entityType.getDeclaredFields()) {
                    if (field.isAnnotationPresent(io.blite.client.spring.annotation.BLiteId.class)) {
                        field.setAccessible(true);
                        var id = (BsonId) field.get(e);
                        deleteById(id);
                        break;
                    }
                }
            } catch (IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @Override
    public void deleteAll() {
        // Fetch all ids and delete in bulk
        List<BsonId> ids = new ArrayList<>();
        for (var entity : findAll()) {
            try {
                for (var field : entityType.getDeclaredFields()) {
                    if (field.isAnnotationPresent(io.blite.client.spring.annotation.BLiteId.class)) {
                        field.setAccessible(true);
                        ids.add((BsonId) field.get(entity));
                        break;
                    }
                }
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        var meta = getMeta();
        for (var id : ids) template.delete(meta, id);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private String getMeta() {
        var ann = entityType.getAnnotation(io.blite.client.spring.annotation.BLiteDocument.class);
        if (ann == null) throw new IllegalStateException(
                entityType.getSimpleName() + " missing @BLiteDocument");
        return ann.value();
    }
}
