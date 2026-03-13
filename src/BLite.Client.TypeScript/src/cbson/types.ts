// blite-client — C-BSON type definitions
// BsonType codes match the BSON specification.

export enum BsonType {
  EndOfDocument = 0x00,
  Double        = 0x01,
  String        = 0x02,
  Document      = 0x03,
  Array         = 0x04,
  Binary        = 0x05,
  ObjectId      = 0x07,
  Boolean       = 0x08,
  DateTime      = 0x09,
  Null          = 0x0a,
  Int32         = 0x10,
  Timestamp     = 0x11,
  Int64         = 0x12,
  Decimal128    = 0x13,
}

export enum BsonIdType {
  ObjectId = 0,
  String   = 1,
  Int32    = 2,
  Int64    = 3,
  Guid     = 4,
}

// ─── BsonId ───────────────────────────────────────────────────────────────────

export class BsonId {
  constructor(
    public readonly value: Buffer,
    public readonly type: BsonIdType,
  ) {}

  static fromObjectId(hex: string): BsonId {
    return new BsonId(Buffer.from(hex, 'hex'), BsonIdType.ObjectId);
  }

  static fromObjectIdBytes(bytes: Uint8Array): BsonId {
    if (bytes.length !== 12) throw new Error('ObjectId must be 12 bytes');
    return new BsonId(Buffer.from(bytes), BsonIdType.ObjectId);
  }

  static fromString(s: string): BsonId {
    return new BsonId(Buffer.from(s, 'utf8'), BsonIdType.String);
  }

  static fromInt32(n: number): BsonId {
    const buf = Buffer.allocUnsafe(4);
    buf.writeInt32BE(n, 0);
    return new BsonId(buf, BsonIdType.Int32);
  }

  static fromInt64(n: bigint): BsonId {
    const buf = Buffer.allocUnsafe(8);
    buf.writeBigInt64BE(n, 0);
    return new BsonId(buf, BsonIdType.Int64);
  }

  static fromGuid(s: string): BsonId {
    return new BsonId(Buffer.from(s, 'utf8'), BsonIdType.Guid);
  }

  toString(): string {
    switch (this.type) {
      case BsonIdType.ObjectId:
        return this.value.toString('hex');
      case BsonIdType.String:
      case BsonIdType.Guid:
        return this.value.toString('utf8');
      case BsonIdType.Int32:
        return String(this.value.readInt32BE(0));
      case BsonIdType.Int64:
        return String(this.value.readBigInt64BE(0));
    }
  }

  toProto(): { value: Buffer; id_type: number } {
    return { value: this.value, id_type: this.type };
  }

  static fromProto(proto: { value: Buffer | Uint8Array; id_type: number }): BsonId {
    return new BsonId(Buffer.from(proto.value), proto.id_type as BsonIdType);
  }
}

// ─── Document value types ─────────────────────────────────────────────────────

/** Represents a raw BSON ObjectId in a decoded document. */
export interface BsonObjectId {
  readonly $oid: string;
}

/** Raw decoded document — any JS value the C-BSON decoder may produce. */
export type BsonValue =
  | null
  | boolean
  | number
  | bigint
  | string
  | Date
  | Buffer
  | BsonObjectId
  | BsonValue[]
  | BsonDocument;

export type BsonDocument = { [key: string]: BsonValue };
