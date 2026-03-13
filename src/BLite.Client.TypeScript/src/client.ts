// blite-client — BLiteClient
//
// Main entry point.  Creates gRPC stubs, manages the global key map, and
// exposes methods to obtain collection handles, transactions and admin access.

import { makeStubs, callUnary } from './grpc/loader';
import { ClientKeyMap } from './key-map';
import { DynamicCollection } from './dynamic-collection';
import { Collection, CollectionMapper } from './collection';
import { BLiteTransaction } from './transaction';
import { AdminClient } from './admin-client';
import { KvStore } from './kv-store';
import { BLiteError } from './errors';

// ─── Options ─────────────────────────────────────────────────────────────────

export interface BLiteClientOptions {
  /** Server hostname or IP. Default: 'localhost'. */
  host?: string;
  /** gRPC server port. Default: 2626. */
  port?: number;
  /** API key sent as 'x-api-key' metadata on every call. */
  apiKey: string;
  /** Use TLS. Default: true. Disable only for local dev. */
  useTls?: boolean;
  /**
   * Fully-qualified address override (e.g. 'https://db.example.com:2626').
   * When supplied, host/port/useTls are ignored.
   */
  address?: string;
}

// ─── BLiteClient ─────────────────────────────────────────────────────────────

export class BLiteClient {
  private readonly _stubs: ReturnType<typeof makeStubs>;
  private readonly _keyMap: ClientKeyMap;

  readonly admin: AdminClient;
  readonly kv: KvStore;

  constructor(options: BLiteClientOptions) {
    const host   = options.host ?? 'localhost';
    const port   = options.port ?? 2626;
    const useTls = options.useTls ?? true;

    const address = options.address ?? `${host}:${port}`;

    this._stubs  = makeStubs(address, options.apiKey, useTls);
    this._keyMap = new ClientKeyMap();

    this.admin = new AdminClient(this._stubs.admin);
    this.kv    = new KvStore(this._stubs.kv);
  }

  // ── Collection access ──────────────────────────────────────────────────────

  /** Returns a schema-less collection handle. */
  getCollection(name: string): DynamicCollection {
    return new DynamicCollection(name, this._stubs.dynamic, this._stubs.metadata, this._keyMap);
  }

  /**
   * Returns a typed collection handle backed by the given mapper.
   * The mapper defines serialization between T and BsonDocument.
   */
  getTypedCollection<T extends object>(mapper: CollectionMapper<T>): Collection<T> {
    const inner = this.getCollection(mapper.collectionName);
    return new Collection<T>(inner, mapper);
  }

  // ── Collection management ──────────────────────────────────────────────────

  async listCollections(): Promise<string[]> {
    const res = await callUnary<unknown, { names: string[] }>(
      this._stubs.dynamic,
      'ListCollections',
      {},
    );
    return res.names ?? [];
  }

  async dropCollection(name: string): Promise<boolean> {
    const res = await callUnary<unknown, { success: boolean; error: string }>(
      this._stubs.dynamic,
      'DropCollection',
      { collection: name },
    );
    BLiteError.check(res.error);
    return res.success;
  }

  // ── Transactions ───────────────────────────────────────────────────────────

  async beginTransaction(): Promise<BLiteTransaction> {
    const res = await callUnary<unknown, { transaction_id: string; error: string }>(
      this._stubs.transaction,
      'Begin',
      {},
    );
    BLiteError.check(res.error);
    return new BLiteTransaction(res.transaction_id, this._stubs.transaction);
  }

  // ── Key-map utilities ──────────────────────────────────────────────────────

  /**
   * Pre-populates the global key map from the server.
   * Call once after connecting if you expect to read documents written by
   * other clients or sessions that registered their own field names.
   * @param anchorCollection  Any collection the caller has Query access to.
   */
  async refreshKeyMap(anchorCollection: string): Promise<void> {
    await this._keyMap.refresh(this._stubs.metadata, anchorCollection);
  }
}
