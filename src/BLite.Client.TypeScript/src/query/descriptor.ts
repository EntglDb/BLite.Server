// blite-client — QueryDescriptor type definitions
//
// These mirror the C# MessagePack-serialized QueryDescriptor from BLite.Proto.
// The wire format uses array-indexed MessagePack (matching [Key(n)] attributes).

// ─── Enums ────────────────────────────────────────────────────────────────────

export const enum FilterOp {
  Eq         = 0,
  NotEq      = 1,
  Lt         = 2,
  LtEq       = 3,
  Gt         = 4,
  GtEq       = 5,
  StartsWith = 6,
  Contains   = 7,
  In         = 8,
}

export const enum LogicalOp {
  And = 0,
  Or  = 1,
}

export const enum ScalarKind {
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

// ─── ScalarValue ──────────────────────────────────────────────────────────────

/** Represents a typed scalar value used in filter predicates. */
export interface ScalarValue {
  kind: ScalarKind;
  boolVal?: boolean;
  int32Val?: number;
  int64Val?: bigint | number;
  doubleVal?: number;
  stringVal?: string;
  dateTimeVal?: Date;
  guidVal?: string;
  objectIdVal?: Uint8Array;
  arrayVal?: ScalarValue[];
}

export const ScalarValue = {
  null(): ScalarValue                         { return { kind: ScalarKind.Null }; },
  bool(v: boolean): ScalarValue               { return { kind: ScalarKind.Bool, boolVal: v }; },
  int32(v: number): ScalarValue               { return { kind: ScalarKind.Int32, int32Val: v }; },
  int64(v: bigint | number): ScalarValue      { return { kind: ScalarKind.Int64, int64Val: v }; },
  double(v: number): ScalarValue              { return { kind: ScalarKind.Double, doubleVal: v }; },
  string(v: string): ScalarValue              { return { kind: ScalarKind.String, stringVal: v }; },
  dateTime(v: Date): ScalarValue              { return { kind: ScalarKind.DateTime, dateTimeVal: v }; },
  guid(v: string): ScalarValue                { return { kind: ScalarKind.Guid, guidVal: v }; },
  objectId(v: Uint8Array): ScalarValue        { return { kind: ScalarKind.ObjectId, objectIdVal: v }; },
  array(items: ScalarValue[]): ScalarValue    { return { kind: ScalarKind.Array, arrayVal: items }; },

  /** Infers the best ScalarValue type from a plain JS value. */
  from(v: unknown): ScalarValue {
    if (v === null || v === undefined) return ScalarValue.null();
    if (typeof v === 'boolean')  return ScalarValue.bool(v);
    if (typeof v === 'bigint')   return ScalarValue.int64(v);
    if (typeof v === 'number') {
      if (Number.isInteger(v) && v >= -2_147_483_648 && v <= 2_147_483_647) return ScalarValue.int32(v);
      return ScalarValue.double(v);
    }
    if (typeof v === 'string')   return ScalarValue.string(v);
    if (v instanceof Date)       return ScalarValue.dateTime(v);
    if (Array.isArray(v))        return ScalarValue.array(v.map(ScalarValue.from));
    throw new TypeError(`Cannot convert ${typeof v} to ScalarValue`);
  },
};

// ─── Filter nodes ─────────────────────────────────────────────────────────────

export interface BinaryFilter {
  kind: 'binary';
  field: string;
  op: FilterOp;
  value: ScalarValue;
}

export interface LogicalFilter {
  kind: 'logical';
  op: LogicalOp;
  children: FilterNode[];
}

export interface UnaryFilter {
  kind: 'unary';
  operand: FilterNode;
}

export type FilterNode = BinaryFilter | LogicalFilter | UnaryFilter;

export function and(...children: FilterNode[]): LogicalFilter {
  return { kind: 'logical', op: LogicalOp.And, children };
}

export function or(...children: FilterNode[]): LogicalFilter {
  return { kind: 'logical', op: LogicalOp.Or, children };
}

export function not(operand: FilterNode): UnaryFilter {
  return { kind: 'unary', operand };
}

export function eq(field: string, value: unknown): BinaryFilter {
  return { kind: 'binary', field, op: FilterOp.Eq, value: ScalarValue.from(value) };
}

export function neq(field: string, value: unknown): BinaryFilter {
  return { kind: 'binary', field, op: FilterOp.NotEq, value: ScalarValue.from(value) };
}

export function gt(field: string, value: unknown): BinaryFilter {
  return { kind: 'binary', field, op: FilterOp.Gt, value: ScalarValue.from(value) };
}

export function gte(field: string, value: unknown): BinaryFilter {
  return { kind: 'binary', field, op: FilterOp.GtEq, value: ScalarValue.from(value) };
}

export function lt(field: string, value: unknown): BinaryFilter {
  return { kind: 'binary', field, op: FilterOp.Lt, value: ScalarValue.from(value) };
}

export function lte(field: string, value: unknown): BinaryFilter {
  return { kind: 'binary', field, op: FilterOp.LtEq, value: ScalarValue.from(value) };
}

export function startsWith(field: string, value: string): BinaryFilter {
  return { kind: 'binary', field, op: FilterOp.StartsWith, value: ScalarValue.string(value) };
}

export function contains(field: string, value: string): BinaryFilter {
  return { kind: 'binary', field, op: FilterOp.Contains, value: ScalarValue.string(value) };
}

export function inList(field: string, values: unknown[]): BinaryFilter {
  return { kind: 'binary', field, op: FilterOp.In, value: ScalarValue.array(values.map(ScalarValue.from)) };
}

// ─── QueryDescriptor ──────────────────────────────────────────────────────────

export interface ProjectionSpec {
  fields: string[];
  resultTypeName?: string;
}

export interface SortSpec {
  field: string;
  descending: boolean;
}

export interface QueryDescriptor {
  collection: string;
  where?: FilterNode;
  select?: ProjectionSpec;
  orderBy?: SortSpec[];
  take?: number;
  skip?: number;
}
