// blite-client — KvStore
//
// Persistent key-value store backed by the same database file.
// Keys are UTF-8 strings; values are arbitrary Buffers.

import { callUnary } from './grpc/loader';
import { BLiteError } from './errors';

// ─── Batch builder ────────────────────────────────────────────────────────────

export interface KvBatchOp {
  key: string;
  value?: Buffer;
  ttlMs?: number;
  isDelete?: boolean;
}

export class KvBatch {
  private readonly _ops: KvBatchOp[] = [];

  set(key: string, value: Buffer, ttlMs?: number): this {
    this._ops.push({ key, value, ttlMs: ttlMs ?? 0 });
    return this;
  }

  delete(key: string): this {
    this._ops.push({ key, isDelete: true });
    return this;
  }

  ops(): KvBatchOp[] { return this._ops; }
}

// ─── KvStore ─────────────────────────────────────────────────────────────────

export class KvStore {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  constructor(private readonly _stub: any) {}

  async get(key: string): Promise<Buffer | null> {
    const res = await callUnary<unknown, { value: Buffer; found: boolean; error: string }>(
      this._stub,
      'Get',
      { key },
    );
    BLiteError.check(res.error);
    return res.found ? Buffer.from(res.value) : null;
  }

  async exists(key: string): Promise<boolean> {
    const res = await callUnary<unknown, { exists: boolean; error: string }>(
      this._stub,
      'Exists',
      { key },
    );
    BLiteError.check(res.error);
    return res.exists;
  }

  async scanKeys(prefix = ''): Promise<string[]> {
    const res = await callUnary<unknown, { keys: string[]; error: string }>(
      this._stub,
      'ScanKeys',
      { prefix },
    );
    BLiteError.check(res.error);
    return res.keys ?? [];
  }

  async set(key: string, value: Buffer, ttlMs?: number): Promise<void> {
    const res = await callUnary<unknown, { success: boolean; error: string }>(
      this._stub,
      'Set',
      { key, value, ttl_ms: ttlMs ?? 0 },
    );
    BLiteError.check(res.error);
  }

  async delete(key: string): Promise<boolean> {
    const res = await callUnary<unknown, { success: boolean; error: string }>(
      this._stub,
      'Delete',
      { key },
    );
    BLiteError.check(res.error);
    return res.success;
  }

  async refresh(key: string, ttlMs: number): Promise<boolean> {
    const res = await callUnary<unknown, { success: boolean; error: string }>(
      this._stub,
      'Refresh',
      { key, ttl_ms: ttlMs },
    );
    BLiteError.check(res.error);
    return res.success;
  }

  async batch(configure: (b: KvBatch) => void): Promise<number> {
    const b = new KvBatch();
    configure(b);
    const ops = b.ops();
    if (ops.length === 0) return 0;

    const res = await callUnary<unknown, { affected_count: number; error: string }>(
      this._stub,
      'Batch',
      {
        operations: ops.map((op) => ({
          key: op.key,
          value: op.value ?? Buffer.alloc(0),
          ttl_ms: op.ttlMs ?? 0,
          is_delete: op.isDelete ?? false,
        })),
      },
    );
    BLiteError.check(res.error);
    return res.affected_count;
  }

  async purgeExpired(): Promise<number> {
    const res = await callUnary<unknown, { purged_count: number; error: string }>(
      this._stub,
      'PurgeExpired',
      {},
    );
    BLiteError.check(res.error);
    return res.purged_count;
  }
}
