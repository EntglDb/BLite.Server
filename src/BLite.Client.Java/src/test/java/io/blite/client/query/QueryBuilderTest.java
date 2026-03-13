// BLite.Client.Java — QueryBuilderTest
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client.query;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class QueryBuilderTest {

    private static final String COL = "products";

    @Test
    void empty_builder_produces_minimal_descriptor() {
        var qd = new QueryBuilder<>(COL, ignored -> List.of()).build();
        assertEquals(COL,  qd.collection());
        assertNull(qd.where());
        assertNull(qd.orderBy());
        assertNull(qd.take());
        assertNull(qd.skip());
    }

    @Test
    void single_predicate() {
        var qd = new QueryBuilder<>(COL, ignored -> List.of())
                .whereEq("name", "Widget")
                .build();
        var bf = assertInstanceOf(BinaryFilter.class, qd.where());
        assertEquals("name",       bf.field());
        assertEquals(FilterOp.EQ,  bf.op());
        assertEquals("Widget",     bf.value().stringVal);
    }

    @Test
    void multiple_predicates_are_anded() {
        var qd = new QueryBuilder<>(COL, ignored -> List.of())
                .whereGte("price", 5.0)
                .whereLte("price", 20.0)
                .build();
        var lf = assertInstanceOf(LogicalFilter.class, qd.where());
        assertEquals(LogicalOp.AND, lf.op());
        assertEquals(2, lf.children().size());
    }

    @Test
    void orderby_and_paging() {
        var qd = new QueryBuilder<>(COL, ignored -> List.of())
                .orderByDescending("price")
                .skip(10)
                .take(5)
                .build();
        assertNotNull(qd.orderBy());
        assertEquals(1,       qd.orderBy().size());
        assertTrue(qd.orderBy().get(0).descending());
        assertEquals(10,      qd.skip());
        assertEquals(5,       qd.take());
    }

    @Test
    void in_predicate() {
        var qd = new QueryBuilder<>(COL, ignored -> List.of())
                .whereIn("status", List.of("active", "pending"))
                .build();
        var bf = assertInstanceOf(BinaryFilter.class, qd.where());
        assertEquals(FilterOp.IN,          bf.op());
        assertEquals(ScalarKind.ARRAY,     bf.value().kind);
        assertEquals(2,                    bf.value().arrayVal.size());
    }

    @Test
    void explicit_filter_node() {
        var node = FilterNode.or(FilterNode.eq("a", 1), FilterNode.eq("b", 2));
        var qd   = new QueryBuilder<>(COL, ignored -> List.of())
                .where(node)
                .build();
        var lf = assertInstanceOf(LogicalFilter.class, qd.where());
        assertEquals(LogicalOp.OR, lf.op());
    }
}
