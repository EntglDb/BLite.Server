// BLite.Proto — QueryDescriptor
// Serializable representation of a LINQ query that can be sent over the wire
// (MessagePack-encoded inside a QueryRequest.query_descriptor bytes field).
// Maps 1:1 to BLite.Core.Query.QueryModel so the server can execute it without
// needing to reconstruct a full .NET Expression tree.

using MessagePack;

namespace BLite.Proto;

// ─── Top-level descriptor ─────────────────────────────────────────────────────

[MessagePackObject]
public sealed class QueryDescriptor
{
    [Key(0)] public string Collection { get; set; } = string.Empty;
    [Key(1)] public FilterNode? Where  { get; set; }
    [Key(2)] public ProjectionSpec? Select { get; set; }
    [Key(3)] public List<SortSpec> OrderBy { get; set; } = [];
    [Key(4)] public int? Take { get; set; }
    [Key(5)] public int? Skip { get; set; }
}

// ─── Projection ───────────────────────────────────────────────────────────────

/// <summary>
/// Describes the SELECT clause: which scalar BSON fields to read,
/// and how to construct the result on the client side.
/// </summary>
[MessagePackObject]
public sealed class ProjectionSpec
{
    /// <summary>BSON field names (lowercase) to extract from each document.</summary>
    [Key(0)] public List<string> Fields { get; set; } = [];

    /// <summary>
    /// Fully-qualified CLR type name of the projection result.
    /// The server uses this as a hint when returning <see cref="TypedDocumentResponse"/>.
    /// </summary>
    [Key(1)] public string? ResultTypeName { get; set; }
}

// ─── Sorting ──────────────────────────────────────────────────────────────────

[MessagePackObject]
public sealed class SortSpec
{
    [Key(0)] public string Field { get; set; } = string.Empty;
    [Key(1)] public bool Descending { get; set; }
}

// ─── Filter tree ──────────────────────────────────────────────────────────────

/// <summary>
/// Root of the predicate tree.  Subclasses: <see cref="BinaryFilter"/>,
/// <see cref="LogicalFilter"/>, <see cref="UnaryFilter"/>.
/// MessagePack union discriminator is the filter kind byte.
/// </summary>
[Union(0, typeof(BinaryFilter))]
[Union(1, typeof(LogicalFilter))]
[Union(2, typeof(UnaryFilter))]
[MessagePackObject]
public abstract class FilterNode { }

// ─── Binary (field op value) ──────────────────────────────────────────────────

[MessagePackObject]
public sealed class BinaryFilter : FilterNode
{
    /// <summary>BSON field name (lowercase, dot-path supported: "address.city").</summary>
    [Key(0)] public string Field { get; set; } = string.Empty;
    [Key(1)] public FilterOp Op  { get; set; }
    [Key(2)] public ScalarValue Value { get; set; } = default!;
}

public enum FilterOp : byte
{
    Eq        = 0,
    NotEq     = 1,
    Lt        = 2,
    LtEq      = 3,
    Gt        = 4,
    GtEq      = 5,
    StartsWith = 6,
    Contains   = 7,
    In         = 8,   // value is an array ScalarValue
}

// ─── Logical (AND / OR / NOT) ─────────────────────────────────────────────────

[MessagePackObject]
public sealed class LogicalFilter : FilterNode
{
    [Key(0)] public LogicalOp    Op       { get; set; }
    [Key(1)] public List<FilterNode> Children { get; set; } = [];
}

public enum LogicalOp : byte
{
    And = 0,
    Or  = 1,
}

// ─── Unary (NOT) ─────────────────────────────────────────────────────────────

[MessagePackObject]
public sealed class UnaryFilter : FilterNode
{
    [Key(0)] public FilterNode Operand { get; set; } = default!;
}

// ─── Scalar value (discriminated union of primitive types) ───────────────────

/// <summary>
/// Holds any scalar BSON value that can appear as the RHS of a predicate.
/// Only one of the value fields will be non-null / non-default.
/// </summary>
[MessagePackObject]
public sealed class ScalarValue
{
    [Key(0)] public ScalarKind Kind { get; set; }

    [Key(1)]  public bool?     BoolVal     { get; set; }
    [Key(2)]  public int?      Int32Val    { get; set; }
    [Key(3)]  public long?     Int64Val    { get; set; }
    [Key(4)]  public double?   DoubleVal   { get; set; }
    [Key(5)]  public decimal?  DecimalVal  { get; set; }
    [Key(6)]  public string?   StringVal   { get; set; }
    [Key(7)]  public DateTime? DateTimeVal { get; set; }
    [Key(8)]  public Guid?     GuidVal     { get; set; }
    [Key(9)]  public byte[]?   ObjectIdVal { get; set; }  // 12-byte ObjectId
    [Key(10)] public List<ScalarValue>? ArrayVal { get; set; } // for FilterOp.In

    // ── Factory helpers ───────────────────────────────────────────────────────

    public static ScalarValue Null()              => new() { Kind = ScalarKind.Null };
    public static ScalarValue From(bool v)        => new() { Kind = ScalarKind.Bool,     BoolVal     = v };
    public static ScalarValue From(int v)         => new() { Kind = ScalarKind.Int32,    Int32Val    = v };
    public static ScalarValue From(long v)        => new() { Kind = ScalarKind.Int64,    Int64Val    = v };
    public static ScalarValue From(double v)      => new() { Kind = ScalarKind.Double,   DoubleVal   = v };
    public static ScalarValue From(decimal v)     => new() { Kind = ScalarKind.Decimal,  DecimalVal  = v };
    public static ScalarValue From(string v)      => new() { Kind = ScalarKind.String,   StringVal   = v };
    public static ScalarValue From(DateTime v)    => new() { Kind = ScalarKind.DateTime, DateTimeVal = v };
    public static ScalarValue From(Guid v)        => new() { Kind = ScalarKind.Guid,     GuidVal     = v };
    public static ScalarValue FromObjectId(byte[] v) => new() { Kind = ScalarKind.ObjectId, ObjectIdVal = v };
    public static ScalarValue FromArray(IEnumerable<ScalarValue> items)
        => new() { Kind = ScalarKind.Array, ArrayVal = [.. items] };
}

public enum ScalarKind : byte
{
    Null     = 0,
    Bool     = 1,
    Int32    = 2,
    Int64    = 3,
    Double   = 4,
    Decimal  = 5,
    String   = 6,
    DateTime = 7,
    Guid     = 8,
    ObjectId = 9,
    Array    = 10,
}
