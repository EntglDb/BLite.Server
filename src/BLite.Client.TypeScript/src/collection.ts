// blite-client — Collection<T>
//
// A TypeScript-generic wrapper over DynamicCollection that maps between a
// user-defined type T and BsonDocument.  Both read and write paths go through
// the DynamicService (same C-BSON path); the type parameter is a compile-time
// convenience, not a server-side concept.
//
// A Mapper<T> tells the collection how to serialize T → BsonDocument and how
// to deserialize BsonDocument → T.

import { BsonId, BsonDocument } from './cbson/types';
import { DynamicCollection, IndexInfo, ChangeEvent } from './dynamic-collection';
import { BLiteTransaction } from './transaction';
import { QueryBuilder } from './query/builder';
import { QueryDescriptor } from './query/descriptor';

// ─── Mapper interface ─────────────────────────────────────────────────────────

export interface CollectionMapper<T extends object> {
  /** Logical collection name on the server. */
  readonly collectionName: string;
  /** Extracts the document ID from a typed entity. */
  getId(entity: T): BsonId;
  /** Serializes T to a BsonDocument for writing. */
  toDocument(entity: T): BsonDocument;
  /** Deserializes a BsonDocument to T. */
  fromDocument(doc: BsonDocument): T;
}

// ─── Collection<T> ────────────────────────────────────────────────────────────

export class Collection<T extends object> {
  private readonly _inner: DynamicCollection;
  private readonly _mapper: CollectionMapper<T>;

  constructor(inner: DynamicCollection, mapper: CollectionMapper<T>) {
    this._inner = inner;
    this._mapper = mapper;
  }

  get name(): string { return this._mapper.collectionName; }

  // ── CRUD ────────────────────────────────────────────────────────────────────

  async insert(entity: T, tx?: BLiteTransaction): Promise<BsonId> {
    return this._inner.insert(this._mapper.toDocument(entity), tx);
  }

  async insertBulk(entities: T[], tx?: BLiteTransaction): Promise<BsonId[]> {
    return this._inner.insertBulk(entities.map((e) => this._mapper.toDocument(e)), tx);
  }

  async findById(id: BsonId): Promise<T | null> {
    const doc = await this._inner.findById(id);
    return doc ? this._mapper.fromDocument(doc) : null;
  }

  async* findAll(): AsyncGenerator<T> {
    for await (const doc of this._inner.findAll()) yield this._mapper.fromDocument(doc);
  }

  async update(entity: T, tx?: BLiteTransaction): Promise<boolean> {
    return this._inner.update(this._mapper.getId(entity), this._mapper.toDocument(entity), tx);
  }

  async updateBulk(entities: T[], tx?: BLiteTransaction): Promise<number> {
    return this._inner.updateBulk(
      entities.map((e) => ({ id: this._mapper.getId(e), doc: this._mapper.toDocument(e) })),
      tx,
    );
  }

  async delete(id: BsonId, tx?: BLiteTransaction): Promise<boolean> {
    return this._inner.delete(id, tx);
  }

  async deleteBulk(ids: BsonId[], tx?: BLiteTransaction): Promise<number> {
    return this._inner.deleteBulk(ids, tx);
  }

  // ── Querying ────────────────────────────────────────────────────────────────

  /** Fluent QueryBuilder that yields typed T results. */
  query(): QueryBuilder<T> {
    return new QueryBuilder<T>(this.name, (d) => this._executeTyped(d));
  }

  async* executeDescriptor(descriptor: QueryDescriptor): AsyncGenerator<T> {
    yield* this._executeTyped(descriptor);
  }

  // ── Index management ────────────────────────────────────────────────────────

  createIndex(field: string, opts?: { name?: string; unique?: boolean }): Promise<void> {
    return this._inner.createIndex(field, opts);
  }

  createVectorIndex(
    field: string,
    dimensions: number,
    opts?: { metric?: 'Cosine' | 'L2' | 'DotProduct'; name?: string },
  ): Promise<void> {
    return this._inner.createVectorIndex(field, dimensions, opts);
  }

  dropIndex(indexName: string): Promise<boolean> {
    return this._inner.dropIndex(indexName);
  }

  listIndexes(): Promise<IndexInfo[]> {
    return this._inner.listIndexes();
  }

  // ── Vector search ─────────────────────────────────────────────────────────────

  async* vectorSearch(
    queryVector: number[],
    opts?: { k?: number; indexName?: string; efSearch?: number },
  ): AsyncGenerator<T> {
    for await (const doc of this._inner.vectorSearch(queryVector, opts)) {
      yield this._mapper.fromDocument(doc);
    }
  }

  // ── CDC ───────────────────────────────────────────────────────────────────────

  watch(capturePayload?: boolean): AsyncGenerator<ChangeEvent> {
    return this._inner.watch(capturePayload);
  }

  // ── Internal ─────────────────────────────────────────────────────────────────

  private async* _executeTyped(descriptor: QueryDescriptor): AsyncGenerator<T> {
    for await (const doc of this._inner.executeDescriptor(descriptor)) {
      yield this._mapper.fromDocument(doc);
    }
  }
}
