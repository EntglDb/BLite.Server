// BLite.Client.Java — QueryBuilder
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Fluent query builder tied to a specific collection.
 *
 * <p>The {@code executor} functional interface is provided by {@link io.blite.client.BLiteCollection}
 * and handles the actual gRPC call.
 *
 * @param <T> result type (typically {@code Map<String,Object>} for dynamic collections,
 *            or a domain class for typed collections)
 */
public final class QueryBuilder<T> {

    private final String collection;
    private final Function<QueryDescriptor, Iterable<T>> executor;

    private FilterNode        where;
    private final List<SortSpec> orderBy = new ArrayList<>();
    private Integer           skip;
    private Integer           take;

    public QueryBuilder(String collection, Function<QueryDescriptor, Iterable<T>> executor) {
        this.collection = collection;
        this.executor   = executor;
    }

    // ── Where predicates ─────────────────────────────────────────────────────

    public QueryBuilder<T> whereEq(String field, Object value)          { return appendFilter(FilterNode.eq(field, value)); }
    public QueryBuilder<T> whereNeq(String field, Object value)         { return appendFilter(FilterNode.neq(field, value)); }
    public QueryBuilder<T> whereGt(String field, Object value)          { return appendFilter(FilterNode.gt(field, value)); }
    public QueryBuilder<T> whereGte(String field, Object value)         { return appendFilter(FilterNode.gte(field, value)); }
    public QueryBuilder<T> whereLt(String field, Object value)          { return appendFilter(FilterNode.lt(field, value)); }
    public QueryBuilder<T> whereLte(String field, Object value)         { return appendFilter(FilterNode.lte(field, value)); }
    public QueryBuilder<T> whereStartsWith(String field, String value)  { return appendFilter(FilterNode.startsWith(field, value)); }
    public QueryBuilder<T> whereContains(String field, String value)    { return appendFilter(FilterNode.contains(field, value)); }
    public QueryBuilder<T> whereIn(String field, List<?> values)        { return appendFilter(FilterNode.in(field, values)); }

    /** Applies a pre-built {@link FilterNode} directly (useful for complex AND/OR trees). */
    public QueryBuilder<T> where(FilterNode node) {
        this.where = node;
        return this;
    }

    // ── Sorting ──────────────────────────────────────────────────────────────

    public QueryBuilder<T> orderBy(String field)           { orderBy.add(SortSpec.asc(field));  return this; }
    public QueryBuilder<T> orderByDescending(String field) { orderBy.add(SortSpec.desc(field)); return this; }

    // ── Paging ───────────────────────────────────────────────────────────────

    public QueryBuilder<T> skip(int n)  { this.skip = n; return this; }
    public QueryBuilder<T> take(int n)  { this.take = n; return this; }
    public QueryBuilder<T> limit(int n) { return take(n); }

    // ── Terminal operations ───────────────────────────────────────────────────

    public Iterable<T> execute()     { return executor.apply(build()); }

    public List<T> toList() {
        List<T> list = new ArrayList<>();
        for (T item : execute()) list.add(item);
        return list;
    }

    public Optional<T> first() {
        var saved = take;
        take = 1;
        try {
            var it = execute().iterator();
            return it.hasNext() ? Optional.of(it.next()) : Optional.empty();
        } finally {
            take = saved;
        }
    }

    public long count() {
        return toList().size();
    }

    // ── Build ─────────────────────────────────────────────────────────────────

    public QueryDescriptor build() {
        return new QueryDescriptor(collection, where, orderBy.isEmpty() ? null : orderBy, take, skip);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private QueryBuilder<T> appendFilter(FilterNode node) {
        if (where == null) {
            where = node;
        } else {
            // Multiple chained predicates are implicitly ANDed
            where = FilterNode.and(where, node);
        }
        return this;
    }
}
