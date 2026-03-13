// blite-client — C-BSON reader
// Decodes C-BSON bytes to a JavaScript object.
//
// C-BSON uses 2-byte little-endian ushort IDs for field names (looked up in
// the server key-map) and positional ushort indices for array elements.

import { BsonType, BsonDocument, BsonValue } from './types';

// ─── Public API ───────────────────────────────────────────────────────────────

/**
 * Decodes C-BSON bytes into a plain JavaScript object.
 * @param buf        Raw C-BSON bytes.
 * @param reverseMap Map of ushort ID → field name.  Unknown IDs fall back to
 *                   the string representation of the numeric ID.
 */
export function decodeCbson(buf: Buffer, reverseMap: Map<number, string>): BsonDocument {
  const r = new Reader(buf, 0);
  return readDocument(r, reverseMap);
}

// ─── Internal reader ──────────────────────────────────────────────────────────

class Reader {
  constructor(
    private readonly buf: Buffer,
    public pos: number,
  ) {}

  readInt32(): number {
    const v = this.buf.readInt32LE(this.pos);
    this.pos += 4;
    return v;
  }

  readUInt16(): number {
    const v = this.buf.readUInt16LE(this.pos);
    this.pos += 2;
    return v;
  }

  readUInt8(): number {
    return this.buf[this.pos++];
  }

  readInt64(): bigint {
    const v = this.buf.readBigInt64LE(this.pos);
    this.pos += 8;
    return v;
  }

  readDouble(): number {
    const v = this.buf.readDoubleLE(this.pos);
    this.pos += 8;
    return v;
  }

  readString(len: number): string {
    const s = this.buf.toString('utf8', this.pos, this.pos + len);
    this.pos += len;
    return s;
  }

  readBytes(len: number): Buffer {
    const b = this.buf.subarray(this.pos, this.pos + len) as Buffer;
    this.pos += len;
    return b;
  }

  skip(n: number): void {
    this.pos += n;
  }

  get(offset: number): number {
    return this.buf[offset];
  }
}

// ─── Document / array decoding ────────────────────────────────────────────────

function readDocument(r: Reader, reverseMap: Map<number, string>): BsonDocument {
  const size = r.readInt32();
  const docEnd = r.pos + size - 4; // r.pos advanced past the size; total size includes itself
  const doc: BsonDocument = {};

  while (r.pos < docEnd) {
    const type = r.readUInt8() as BsonType;
    if (type === BsonType.EndOfDocument) break;

    const id = r.readUInt16();
    const name = reverseMap.get(id) ?? String(id);
    doc[name] = readValue(type, r, reverseMap);
  }

  return doc;
}

function readArray(r: Reader, reverseMap: Map<number, string>): BsonValue[] {
  const size = r.readInt32();
  const arrEnd = r.pos + size - 4; // r.pos advanced past the size; total size includes itself
  const arr: BsonValue[] = [];

  while (r.pos < arrEnd) {
    const type = r.readUInt8() as BsonType;
    if (type === BsonType.EndOfDocument) break;
    r.skip(2); // skip positional ushort index
    arr.push(readValue(type, r, reverseMap));
  }

  return arr;
}

function readValue(type: BsonType, r: Reader, reverseMap: Map<number, string>): BsonValue {
  switch (type) {
    case BsonType.Null:
      return null;

    case BsonType.Boolean:
      return r.readUInt8() !== 0;

    case BsonType.Int32:
      return r.readInt32();

    case BsonType.Int64:
    case BsonType.Timestamp: {
      const n = r.readInt64();
      // Return as number if it fits safely, otherwise bigint
      if (n >= -9_007_199_254_740_991n && n <= 9_007_199_254_740_991n) {
        return Number(n);
      }
      return n;
    }

    case BsonType.Double:
      return r.readDouble();

    case BsonType.Decimal128: {
      // 16 bytes — decode as two int64 halves, return as string for lossless round-trip
      const lo = r.readInt64();
      const hi = r.readInt64();
      return `${lo}:${hi}`;
    }

    case BsonType.String: {
      const len = r.readInt32();
      const s = r.readString(len - 1);
      r.skip(1); // null terminator
      return s;
    }

    case BsonType.DateTime: {
      const ms = r.readInt64();
      return new Date(Number(ms));
    }

    case BsonType.ObjectId: {
      const bytes = r.readBytes(12);
      return { $oid: bytes.toString('hex') };
    }

    case BsonType.Binary: {
      const len = r.readInt32();
      r.skip(1); // subtype byte
      return r.readBytes(len);
    }

    case BsonType.Document:
      return readDocument(r, reverseMap);

    case BsonType.Array:
      return readArray(r, reverseMap);

    default:
      throw new Error(`Unsupported C-BSON type 0x${type.toString(16)}`);
  }
}
