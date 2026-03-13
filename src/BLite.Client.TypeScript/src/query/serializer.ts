// blite-client — QueryDescriptor MessagePack serializer
//
// Encodes a QueryDescriptor as the byte array expected by the server's
// QueryDescriptorSerializer.Deserialize (MessagePack-CSharp, array-indexed
// format, Lz4Block-transparent).
//
// The server uses WithCompression(Lz4Block) which is transparent for reads:
// it accepts both compressed and plain MessagePack, so we send plain msgpack.

import { encode } from '@msgpack/msgpack';
import {
  QueryDescriptor,
  FilterNode,
  ScalarValue,
  ScalarKind,
  BinaryFilter,
  LogicalFilter,
  UnaryFilter,
  SortSpec,
  ProjectionSpec,
} from './descriptor';

// ─── Public API ───────────────────────────────────────────────────────────────

export function serializeDescriptor(d: QueryDescriptor): Uint8Array {
  return encode(encodeDescriptor(d));
}

// ─── Encoding helpers ─────────────────────────────────────────────────────────

// QueryDescriptor → 6-element array [collection, where, select, orderBy, take, skip]
function encodeDescriptor(d: QueryDescriptor): unknown[] {
  return [
    d.collection,
    d.where != null ? encodeFilter(d.where) : null,
    d.select != null ? encodeProjection(d.select) : null,
    (d.orderBy ?? []).map(encodeSortSpec),
    d.take ?? null,
    d.skip ?? null,
  ];
}

// ProjectionSpec → [fields, resultTypeName]
function encodeProjection(p: ProjectionSpec): unknown[] {
  return [p.fields, p.resultTypeName ?? null];
}

// SortSpec → [field, descending]
function encodeSortSpec(s: SortSpec): unknown[] {
  return [s.field, s.descending];
}

// FilterNode Union → [tag, valueArray]
// BinaryFilter  tag 0 → [0, [field, op, scalarValue]]
// LogicalFilter tag 1 → [1, [op, [children...]]]
// UnaryFilter   tag 2 → [2, [operand]]
function encodeFilter(f: FilterNode): unknown[] {
  switch (f.kind) {
    case 'binary': {
      const b = f as BinaryFilter;
      return [0, [b.field, b.op, encodeScalar(b.value)]];
    }
    case 'logical': {
      const l = f as LogicalFilter;
      return [1, [l.op, l.children.map(encodeFilter)]];
    }
    case 'unary': {
      const u = f as UnaryFilter;
      return [2, [encodeFilter(u.operand)]];
    }
  }
}

// ScalarValue → 11-element array, indexed by Key(0)..Key(10)
// [kind, boolVal, int32Val, int64Val, doubleVal, decimalVal, stringVal, dateTimeVal, guidVal, objectIdVal, arrayVal]
function encodeScalar(s: ScalarValue): unknown[] {
  const arr: unknown[] = new Array(11).fill(null);
  arr[0] = s.kind;

  switch (s.kind) {
    case ScalarKind.Bool:
      arr[1] = s.boolVal ?? null;
      break;
    case ScalarKind.Int32:
      arr[2] = s.int32Val ?? null;
      break;
    case ScalarKind.Int64:
      // Encode as number if safe, otherwise the msgpack library will handle bigint
      arr[3] = s.int64Val ?? null;
      break;
    case ScalarKind.Double:
      arr[4] = s.doubleVal ?? null;
      break;
    case ScalarKind.String:
      arr[6] = s.stringVal ?? null;
      break;
    case ScalarKind.DateTime:
      // @msgpack/msgpack encodes Date objects as Timestamp ext type (-1),
      // which matches MessagePack-CSharp's DateTime serialization.
      arr[7] = s.dateTimeVal!;
      break;
    case ScalarKind.Guid:
      arr[8] = s.guidVal ?? null;
      break;
    case ScalarKind.ObjectId:
      arr[9] = s.objectIdVal ?? null;
      break;
    case ScalarKind.Array:
      arr[10] = (s.arrayVal ?? []).map(encodeScalar);
      break;
    // ScalarKind.Null and ScalarKind.Decimal leave all fields as null
  }

  return arr;
}


