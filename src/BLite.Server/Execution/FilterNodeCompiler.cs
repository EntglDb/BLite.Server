// BLite.Server — FilterNodeCompiler
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Translates a FilterNode tree (from QueryDescriptor.Where) into a
// Func<BsonDocument, bool> predicate that DynamicCollection.FindAsync can consume.

using BLite.Bson;
using BLite.Proto;

namespace BLite.Server.Execution;

public static class FilterNodeCompiler
{
    public static Func<BsonDocument, bool> Compile(FilterNode node) =>
        node switch
        {
            BinaryFilter  b => CompileBinary(b),
            LogicalFilter l => CompileLogical(l),
            UnaryFilter   u => CompileUnary(u),
            _ => throw new NotSupportedException($"Unknown FilterNode type: {node.GetType().Name}")
        };

    // ── Binary ────────────────────────────────────────────────────────────────

    private static Func<BsonDocument, bool> CompileBinary(BinaryFilter f)
    {
        var field = f.Field;
        var value = f.Value;
        var op    = f.Op;

        return doc =>
        {
            var actual = GetField(doc, field);
            return op switch
            {
                FilterOp.Eq         => IsEqual(actual, value),
                FilterOp.NotEq      => !IsEqual(actual, value),
                FilterOp.Gt         => Compare(actual, value) > 0,
                FilterOp.GtEq       => Compare(actual, value) >= 0,
                FilterOp.Lt         => Compare(actual, value) < 0,
                FilterOp.LtEq       => Compare(actual, value) <= 0,
                FilterOp.StartsWith => actual is string s && value.StringVal is string sv
                                       && s.StartsWith(sv, StringComparison.Ordinal),
                FilterOp.Contains   => actual is string sc && value.StringVal is string vc
                                       && sc.Contains(vc, StringComparison.Ordinal),
                FilterOp.In         => value.ArrayVal is { } arr && arr.Any(v => IsEqual(actual, v)),
                _ => throw new NotSupportedException($"Unknown FilterOp: {op}")
            };
        };
    }

    // ── Logical ───────────────────────────────────────────────────────────────

    private static Func<BsonDocument, bool> CompileLogical(LogicalFilter f)
    {
        var compiled = f.Children.Select(Compile).ToList();
        return f.Op switch
        {
            LogicalOp.And => doc => compiled.All(p => p(doc)),
            LogicalOp.Or  => doc => compiled.Any(p => p(doc)),
            _ => throw new NotSupportedException($"Unknown LogicalOp: {f.Op}")
        };
    }

    // ── Unary (NOT) ────────────────────────────────────────────────────────────

    private static Func<BsonDocument, bool> CompileUnary(UnaryFilter f)
    {
        var inner = Compile(f.Operand);
        return doc => !inner(doc);
    }

    // ── Equality & comparison helpers ─────────────────────────────────────────

    private static bool IsEqual(object? actual, ScalarValue expected) =>
        expected.Kind switch
        {
            ScalarKind.Null     => actual is null,
            ScalarKind.Bool     => actual is bool b    && b == expected.BoolVal,
            ScalarKind.Int32    => actual is int i      && i == expected.Int32Val,
            ScalarKind.Int64    => actual is long l     && l == expected.Int64Val,
            ScalarKind.Double   => actual is double d   && d == expected.DoubleVal,
            ScalarKind.Decimal  => actual is decimal m  && m == expected.DecimalVal,
            ScalarKind.String   => actual is string s   && s == expected.StringVal,
            ScalarKind.DateTime => actual is DateTime dt && dt == expected.DateTimeVal,
            ScalarKind.Guid     => actual is Guid g     && g == expected.GuidVal,
            _ => Equals(actual, BoxedValue(expected))
        };

    private static int Compare(object? actual, ScalarValue expected)
    {
        if (actual is null) return -1;
        return actual switch
        {
            int    i => i.CompareTo(expected.Int32Val ?? 0),
            long   l => l.CompareTo(expected.Int64Val ?? 0L),
            double d => d.CompareTo(expected.DoubleVal ?? 0d),
            decimal m => m.CompareTo(expected.DecimalVal ?? 0m),
            DateTime dt => dt.CompareTo(expected.DateTimeVal ?? DateTime.MinValue),
            string s => string.Compare(s, expected.StringVal, StringComparison.Ordinal),
            _ => 0
        };
    }

    private static object? BoxedValue(ScalarValue v) => v.Kind switch
    {
        ScalarKind.Null     => null,
        ScalarKind.Bool     => v.BoolVal,
        ScalarKind.Int32    => v.Int32Val,
        ScalarKind.Int64    => v.Int64Val,
        ScalarKind.Double   => v.DoubleVal,
        ScalarKind.Decimal  => v.DecimalVal,
        ScalarKind.String   => v.StringVal,
        ScalarKind.DateTime => v.DateTimeVal,
        ScalarKind.Guid     => v.GuidVal,
        _ => null
    };

    private static object? GetField(BsonDocument doc, string fieldPath)
    {
        var parts = fieldPath.Split('.');
        object? current = doc;
        foreach (var part in parts)
        {
            if (current is BsonDocument d)
                current = d.TryGet(part, out var v) ? v : null;
            else
                return null;
        }
        return current;
    }
}
