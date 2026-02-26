// BLite.Client — QueryModelToDescriptorConverter
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Converts a QueryModel (produced by BTreeExpressionVisitor from LINQ expression
// trees) into a QueryDescriptor (MessagePack-serializable DTO sent over gRPC).
//
// LambdaExpression cannot be serialized — this converter walks the expression
// body and produces the equivalent FilterNode / ProjectionSpec / SortSpec tree.

using System.Linq.Expressions;
using System.Reflection;
using BLite.Core.Query;
using BLite.Proto;

namespace BLite.Client.Internal;

internal static class QueryModelToDescriptorConverter
{
    /// <summary>
    /// Converts a <see cref="QueryModel"/> into a wire-ready <see cref="QueryDescriptor"/>.
    /// </summary>
    internal static QueryDescriptor Convert(QueryModel model, string collection)
    {
        var descriptor = new QueryDescriptor
        {
            Collection = collection,
            Take       = model.Take,
            Skip       = model.Skip
        };

        if (model.WhereClause is not null)
            descriptor.Where = ConvertFilter(model.WhereClause.Body);

        if (model.SelectClause is not null)
            descriptor.Select = ConvertProjection(model.SelectClause);

        if (model.OrderByClause is not null)
        {
            var fieldName = ExtractMemberName(model.OrderByClause.Body);
            if (fieldName is not null)
            {
                descriptor.OrderBy.Add(new SortSpec
                {
                    Field      = fieldName,
                    Descending = model.OrderDescending
                });
            }
        }

        return descriptor;
    }

    // ── Filter conversion ─────────────────────────────────────────────────────

    private static FilterNode ConvertFilter(Expression expr)
    {
        return expr switch
        {
            BinaryExpression binary => ConvertBinary(binary),
            UnaryExpression { NodeType: ExpressionType.Not } unary => ConvertNot(unary),
            MethodCallExpression call => ConvertMethodCall(call),
            // Handle invocations wrapping lambdas (from chained Where)
            InvocationExpression inv => ConvertFilter(inv.Expression is LambdaExpression l ? l.Body : inv),
            _ => throw new NotSupportedException(
                     $"Expression type '{expr.NodeType}' ({expr.GetType().Name}) is not supported for remote query push-down.")
        };
    }

    private static FilterNode ConvertBinary(BinaryExpression binary)
    {
        // Logical AND / OR
        if (binary.NodeType is ExpressionType.AndAlso or ExpressionType.OrElse)
        {
            return new LogicalFilter
            {
                Op       = binary.NodeType == ExpressionType.AndAlso ? LogicalOp.And : LogicalOp.Or,
                Children = [ConvertFilter(binary.Left), ConvertFilter(binary.Right)]
            };
        }

        // Comparison operators: field op value
        var (field, value, flipped) = ExtractFieldAndValue(binary);
        if (field is null || value is null)
            throw new NotSupportedException(
                $"Cannot extract field/value from binary expression: {binary}");

        var op = (binary.NodeType, flipped) switch
        {
            (ExpressionType.Equal, _)              => FilterOp.Eq,
            (ExpressionType.NotEqual, _)           => FilterOp.NotEq,
            (ExpressionType.LessThan, false)       => FilterOp.Lt,
            (ExpressionType.LessThan, true)        => FilterOp.Gt,
            (ExpressionType.LessThanOrEqual, false) => FilterOp.LtEq,
            (ExpressionType.LessThanOrEqual, true)  => FilterOp.GtEq,
            (ExpressionType.GreaterThan, false)     => FilterOp.Gt,
            (ExpressionType.GreaterThan, true)      => FilterOp.Lt,
            (ExpressionType.GreaterThanOrEqual, false) => FilterOp.GtEq,
            (ExpressionType.GreaterThanOrEqual, true)  => FilterOp.LtEq,
            _ => throw new NotSupportedException($"Binary operator '{binary.NodeType}' is not supported.")
        };

        return new BinaryFilter
        {
            Field = field,
            Op    = op,
            Value = value
        };
    }

    private static FilterNode ConvertNot(UnaryExpression unary)
    {
        return new UnaryFilter { Operand = ConvertFilter(unary.Operand) };
    }

    private static FilterNode ConvertMethodCall(MethodCallExpression call)
    {
        var methodName = call.Method.Name;

        // string.StartsWith / string.Contains on a member: x.Name.StartsWith("A")
        if (call.Object is MemberExpression member && call.Arguments.Count >= 1)
        {
            var field = ExtractMemberName(member);
            if (field is not null)
            {
                var argValue = EvaluateExpression(call.Arguments[0]);
                if (argValue is string s)
                {
                    var op = methodName switch
                    {
                        "StartsWith" => FilterOp.StartsWith,
                        "Contains"   => FilterOp.Contains,
                        _ => throw new NotSupportedException(
                                 $"Method '{methodName}' is not supported for remote query push-down.")
                    };
                    return new BinaryFilter
                    {
                        Field = field,
                        Op    = op,
                        Value = ScalarValue.From(s)
                    };
                }
            }
        }

        // Enumerable.Contains(collection, x.Field) — used for .In() patterns
        if (methodName == "Contains" && call.Method.DeclaringType == typeof(Enumerable)
            && call.Arguments.Count == 2)
        {
            var collection = EvaluateExpression(call.Arguments[0]);
            var fieldExpr  = call.Arguments[1];
            var field      = ExtractMemberName(fieldExpr);

            if (field is not null && collection is System.Collections.IEnumerable items)
            {
                var scalars = new List<ScalarValue>();
                foreach (var item in items)
                    scalars.Add(ToScalarValue(item));

                return new BinaryFilter
                {
                    Field = field,
                    Op    = FilterOp.In,
                    Value = ScalarValue.FromArray(scalars)
                };
            }
        }

        // List<T>.Contains(x.Field) — instance method version of In
        if (methodName == "Contains" && call.Object is not null && call.Arguments.Count == 1)
        {
            var fieldExpr = call.Arguments[0];
            var field     = ExtractMemberName(fieldExpr);

            if (field is not null)
            {
                var collection = EvaluateExpression(call.Object);
                if (collection is System.Collections.IEnumerable items)
                {
                    var scalars = new List<ScalarValue>();
                    foreach (var item in items)
                        scalars.Add(ToScalarValue(item));

                    return new BinaryFilter
                    {
                        Field = field,
                        Op    = FilterOp.In,
                        Value = ScalarValue.FromArray(scalars)
                    };
                }
            }
        }

        throw new NotSupportedException(
            $"Method '{call.Method.DeclaringType?.Name}.{methodName}' is not supported for remote query push-down.");
    }

    // ── Projection conversion ─────────────────────────────────────────────────

    private static ProjectionSpec ConvertProjection(LambdaExpression selectLambda)
    {
        var analysis = ProjectionAnalyzer.Analyze(selectLambda);
        var spec     = new ProjectionSpec();

        if (analysis.IsSimple)
        {
            spec.Fields.AddRange(analysis.Fields.Select(f => f.BsonName));
            spec.ResultTypeName = selectLambda.ReturnType.FullName ?? selectLambda.ReturnType.Name;
        }

        return spec;
    }

    // ── Member extraction ─────────────────────────────────────────────────────

    /// <summary>
    /// Extracts the BSON field name from a member access expression (e.g. x.Age → "age").
    /// </summary>
    private static string? ExtractMemberName(Expression expr)
    {
        if (expr is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            expr = convert.Operand;

        if (expr is MemberExpression { Expression: ParameterExpression } member)
        {
            return ProjectionAnalyzer.ToBsonName(member.Member.Name);
        }

        return null;
    }

    /// <summary>
    /// Given a binary expression like (x.Age > 25) or (25 < x.Age),
    /// returns (fieldName, scalarValue, flipped) where flipped=true when
    /// the value was on the left side.
    /// </summary>
    private static (string? Field, ScalarValue? Value, bool Flipped) ExtractFieldAndValue(
        BinaryExpression binary)
    {
        // Try left = field, right = value
        var leftField = ExtractMemberName(binary.Left);
        if (leftField is not null)
        {
            var rightValue = EvaluateExpression(binary.Right);
            return (leftField, ToScalarValue(rightValue), false);
        }

        // Try right = field, left = value (flipped)
        var rightField = ExtractMemberName(binary.Right);
        if (rightField is not null)
        {
            var leftValue = EvaluateExpression(binary.Left);
            return (rightField, ToScalarValue(leftValue), true);
        }

        return (null, null, false);
    }

    // ── Value evaluation ──────────────────────────────────────────────────────

    /// <summary>
    /// Evaluates a constant or captured-variable expression to its runtime value.
    /// </summary>
    private static object? EvaluateExpression(Expression expr)
    {
        return expr switch
        {
            ConstantExpression c => c.Value,
            // Captured variables: x => x.Age > threshold   →  MemberAccess on a closure field
            MemberExpression m when m.Expression is ConstantExpression ce
                => m.Member switch
                {
                    FieldInfo fi    => fi.GetValue(ce.Value),
                    PropertyInfo pi => pi.GetValue(ce.Value),
                    _ => CompileAndInvoke(expr)
                },
            UnaryExpression { NodeType: ExpressionType.Convert } u => EvaluateExpression(u.Operand),
            _ => CompileAndInvoke(expr)
        };
    }

    private static object? CompileAndInvoke(Expression expr)
    {
        var lambda   = Expression.Lambda<Func<object?>>(Expression.Convert(expr, typeof(object)));
        var compiled = lambda.Compile();
        return compiled();
    }

    // ── Scalar value boxing ───────────────────────────────────────────────────

    private static ScalarValue ToScalarValue(object? value) => value switch
    {
        null       => ScalarValue.Null(),
        bool b     => ScalarValue.From(b),
        int i      => ScalarValue.From(i),
        long l     => ScalarValue.From(l),
        double d   => ScalarValue.From(d),
        decimal m  => ScalarValue.From(m),
        float f    => ScalarValue.From((double)f),
        string s   => ScalarValue.From(s),
        DateTime dt => ScalarValue.From(dt),
        Guid g     => ScalarValue.From(g),
        Enum e     => ScalarValue.From(System.Convert.ToInt32(e)),
        _ when value.GetType().FullName == "BLite.Bson.ObjectId"
            => ScalarValue.FromObjectId(GetObjectIdBytes(value)),
        _ => throw new NotSupportedException(
                 $"Cannot convert value of type '{value.GetType().Name}' to ScalarValue.")
    };

    private static byte[] GetObjectIdBytes(object objectId)
    {
        var method = objectId.GetType().GetMethod("ToByteArray")
            ?? throw new InvalidOperationException("ObjectId does not have a ToByteArray method.");
        return (byte[])method.Invoke(objectId, null)!;
    }
}
