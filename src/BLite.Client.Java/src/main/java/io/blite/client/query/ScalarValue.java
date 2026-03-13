// BLite.Client.Java — ScalarValue
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

package io.blite.client.query;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/** Typed scalar value used in filter predicates. Mirrors the C# ScalarValue. */
public final class ScalarValue {

    public final ScalarKind        kind;
    public final Boolean           boolVal;
    public final Integer           int32Val;
    public final Long              int64Val;
    public final Double            doubleVal;
    public final String            stringVal;
    public final Instant           dateTimeVal;
    public final String            guidVal;
    public final byte[]            objectIdVal;
    public final List<ScalarValue> arrayVal;

    private ScalarValue(
            ScalarKind kind, Boolean boolVal, Integer int32Val, Long int64Val,
            Double doubleVal, String stringVal, Instant dateTimeVal, String guidVal,
            byte[] objectIdVal, List<ScalarValue> arrayVal) {
        this.kind        = kind;
        this.boolVal     = boolVal;
        this.int32Val    = int32Val;
        this.int64Val    = int64Val;
        this.doubleVal   = doubleVal;
        this.stringVal   = stringVal;
        this.dateTimeVal = dateTimeVal;
        this.guidVal     = guidVal;
        this.objectIdVal = objectIdVal;
        this.arrayVal    = arrayVal;
    }

    // ── Factory methods ──────────────────────────────────────────────────────

    public static ScalarValue ofNull()                    { return new ScalarValue(ScalarKind.NULL,      null, null, null, null, null, null, null, null, null); }
    public static ScalarValue of(boolean v)               { return new ScalarValue(ScalarKind.BOOL,      v,    null, null, null, null, null, null, null, null); }
    public static ScalarValue of(int v)                   { return new ScalarValue(ScalarKind.INT32,     null, v,    null, null, null, null, null, null, null); }
    public static ScalarValue of(long v)                  { return new ScalarValue(ScalarKind.INT64,     null, null, v,    null, null, null, null, null, null); }
    public static ScalarValue of(double v)                { return new ScalarValue(ScalarKind.DOUBLE,    null, null, null, v,    null, null, null, null, null); }
    public static ScalarValue of(String v)                { return new ScalarValue(ScalarKind.STRING,    null, null, null, null, v,    null, null, null, null); }
    public static ScalarValue of(Instant v)               { return new ScalarValue(ScalarKind.DATE_TIME, null, null, null, null, null, v,    null, null, null); }
    public static ScalarValue ofGuid(String v)            { return new ScalarValue(ScalarKind.GUID,      null, null, null, null, null, null, v,    null, null); }
    public static ScalarValue ofObjectId(byte[] v)        { return new ScalarValue(ScalarKind.OBJECT_ID, null, null, null, null, null, null, null, v,    null); }
    public static ScalarValue ofArray(List<ScalarValue> v){ return new ScalarValue(ScalarKind.ARRAY,     null, null, null, null, null, null, null, null, v   ); }

    /** Infers the best ScalarValue type from a plain Java value. */
    public static ScalarValue from(Object v) {
        if (v == null)             return ofNull();
        if (v instanceof Boolean b) return of(b);
        if (v instanceof Integer i) return of(i);
        if (v instanceof Long l)    return of(l);
        if (v instanceof Double d)  return of(d);
        if (v instanceof Float f)   return of((double) f);
        if (v instanceof String s)  return of(s);
        if (v instanceof Instant i) return of(i);
        if (v instanceof Date d)    return of(d.toInstant());
        if (v instanceof List<?> list)
            return ofArray(list.stream().map(ScalarValue::from).collect(Collectors.toList()));
        throw new IllegalArgumentException("Cannot convert " + v.getClass().getSimpleName() + " to ScalarValue");
    }
}
