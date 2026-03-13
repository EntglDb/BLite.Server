// BLite.Client.Java — FilterNode
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client.query;

import java.util.Arrays;
import java.util.List;

/** Sealed hierarchy for query filter predicates. */
public sealed interface FilterNode permits BinaryFilter, LogicalFilter, UnaryFilter {

    static BinaryFilter eq(String field, Object value)          { return new BinaryFilter(field, FilterOp.EQ,         ScalarValue.from(value)); }
    static BinaryFilter neq(String field, Object value)         { return new BinaryFilter(field, FilterOp.NOT_EQ,     ScalarValue.from(value)); }
    static BinaryFilter gt(String field, Object value)          { return new BinaryFilter(field, FilterOp.GT,         ScalarValue.from(value)); }
    static BinaryFilter gte(String field, Object value)         { return new BinaryFilter(field, FilterOp.GT_EQ,      ScalarValue.from(value)); }
    static BinaryFilter lt(String field, Object value)          { return new BinaryFilter(field, FilterOp.LT,         ScalarValue.from(value)); }
    static BinaryFilter lte(String field, Object value)         { return new BinaryFilter(field, FilterOp.LT_EQ,      ScalarValue.from(value)); }
    static BinaryFilter startsWith(String field, String value)  { return new BinaryFilter(field, FilterOp.STARTS_WITH,ScalarValue.of(value)); }
    static BinaryFilter contains(String field, String value)    { return new BinaryFilter(field, FilterOp.CONTAINS,   ScalarValue.of(value)); }

    static BinaryFilter in(String field, List<?> values) {
        var scalars = values.stream().map(ScalarValue::from).toList();
        return new BinaryFilter(field, FilterOp.IN, ScalarValue.ofArray(scalars));
    }

    static LogicalFilter and(FilterNode... children) { return new LogicalFilter(LogicalOp.AND, Arrays.asList(children)); }
    static LogicalFilter or(FilterNode... children)  { return new LogicalFilter(LogicalOp.OR,  Arrays.asList(children)); }
    static UnaryFilter   not(FilterNode operand)     { return new UnaryFilter(operand); }
}
