// blite-client — C-BSON writer
// Encodes a JavaScript object to the C-BSON binary format used by BLite.
//
// C-BSON differs from standard BSON only in how field names are stored: instead
// of a null-terminated string, each field is identified by a 2-byte little-
// endian ushort that maps to a name through the server's global key dictionary.
// Array elements use positional ushort indices (same 2-byte slot).

import { BsonType, BsonDocument, BsonValue, BsonObjectId } from './types';

// ─── Public API ───────────────────────────────────────────────────────────────

/**
 * Encodes a JS object as C-BSON bytes.
 * @param doc     Source document.
 * @param keyMap  Map of field name → ushort ID (from the server key-map).
 */
export function encodeCbson(doc: BsonDocument, keyMap: Map<string, number>): Buffer {
  return writeDocument(doc, keyMap);
}

// ─── Internal helpers ─────────────────────────────────────────────────────────

function writeDocument(doc: BsonDocument, keyMap: Map<string, number>): Buffer {
  const parts: Buffer[] = [];

  for (const [name, value] of Object.entries(doc)) {
    if (value === undefined) continue;
    parts.push(writeElement(name, value, keyMap));
  }

  parts.push(BUF_END);
  const body = Buffer.concat(parts);
  const size = 4 + body.length;
  const header = Buffer.allocUnsafe(4);
  header.writeInt32LE(size, 0);
  return Buffer.concat([header, body]);
}

function writeArray(arr: BsonValue[], keyMap: Map<string, number>): Buffer {
  const parts: Buffer[] = [];

  for (let i = 0; i < arr.length; i++) {
    parts.push(writeArrayElement(i, arr[i], keyMap));
  }

  parts.push(BUF_END);
  const body = Buffer.concat(parts);
  const size = 4 + body.length;
  const header = Buffer.allocUnsafe(4);
  header.writeInt32LE(size, 0);
  return Buffer.concat([header, body]);
}

function writeElement(name: string, value: BsonValue, keyMap: Map<string, number>): Buffer {
  const id = keyMap.get(name.toLowerCase());
  if (id === undefined) {
    throw new Error(
      `C-BSON key '${name}' not registered. Call RegisterKeys before writing documents.`,
    );
  }

  const idBuf = Buffer.allocUnsafe(2);
  idBuf.writeUInt16LE(id, 0);
  const valueParts = encodeValue(value, keyMap);
  return Buffer.concat([Buffer.from([resolveType(value)]), idBuf, ...valueParts]);
}

function writeArrayElement(index: number, value: BsonValue, keyMap: Map<string, number>): Buffer {
  const idxBuf = Buffer.allocUnsafe(2);
  idxBuf.writeUInt16LE(index, 0);
  const valueParts = encodeValue(value, keyMap);
  return Buffer.concat([Buffer.from([resolveType(value)]), idxBuf, ...valueParts]);
}

function resolveType(value: BsonValue): BsonType {
  if (value === null || value === undefined) return BsonType.Null;
  if (typeof value === 'boolean') return BsonType.Boolean;
  if (typeof value === 'bigint') return BsonType.Int64;
  if (typeof value === 'number') {
    if (Number.isInteger(value) && value >= -2_147_483_648 && value <= 2_147_483_647) return BsonType.Int32;
    return BsonType.Double;
  }
  if (typeof value === 'string') return BsonType.String;
  if (value instanceof Date) return BsonType.DateTime;
  if (Buffer.isBuffer(value)) return BsonType.Binary;
  if (Array.isArray(value)) return BsonType.Array;
  if (isObjectId(value)) return BsonType.ObjectId;
  return BsonType.Document;
}

function encodeValue(value: BsonValue, keyMap: Map<string, number>): Buffer[] {
  if (value === null || value === undefined) return [];
  if (typeof value === 'boolean') {
    return [Buffer.from([value ? 1 : 0])];
  }
  if (typeof value === 'bigint') {
    const b = Buffer.allocUnsafe(8);
    b.writeBigInt64LE(value, 0);
    return [b];
  }
  if (typeof value === 'number') {
    if (Number.isInteger(value) && value >= -2_147_483_648 && value <= 2_147_483_647) {
      const b = Buffer.allocUnsafe(4);
      b.writeInt32LE(value, 0);
      return [b];
    }
    const b = Buffer.allocUnsafe(8);
    b.writeDoubleBE(value, 0); // Not LE! We need LE.
    // Fix: use writeDoubleLE
    b.writeDoubleLE(value, 0);
    return [b];
  }
  if (typeof value === 'string') {
    const strBytes = Buffer.from(value, 'utf8');
    const lenBuf = Buffer.allocUnsafe(4);
    lenBuf.writeInt32LE(strBytes.length + 1, 0);
    return [lenBuf, strBytes, BUF_NULL];
  }
  if (value instanceof Date) {
    const b = Buffer.allocUnsafe(8);
    b.writeBigInt64LE(BigInt(value.getTime()), 0);
    return [b];
  }
  if (Buffer.isBuffer(value)) {
    const lenBuf = Buffer.allocUnsafe(4);
    lenBuf.writeInt32LE(value.length, 0);
    return [lenBuf, BUF_ZERO, value];
  }
  if (Array.isArray(value)) {
    return [writeArray(value as BsonValue[], keyMap)];
  }
  if (isObjectId(value)) {
    return [Buffer.from((value as BsonObjectId).$oid, 'hex')];
  }
  // Nested document
  return [writeDocument(value as BsonDocument, keyMap)];
}

function isObjectId(v: BsonValue): v is BsonObjectId {
  return (
    typeof v === 'object' &&
    v !== null &&
    !Array.isArray(v) &&
    !Buffer.isBuffer(v) &&
    !(v instanceof Date) &&
    '$oid' in v
  );
}

const BUF_END  = Buffer.from([0x00]);
const BUF_NULL = Buffer.from([0x00]);
const BUF_ZERO = Buffer.from([0x00]);
