// blite-client — DynamicCollection
//
// Schema-less collection that stores and retrieves plain JavaScript objects.
// Uses DynamicService (raw C-BSON path) and MetadataService for key management.

import { BsonId, BsonDocument } from './cbson/types';
import { encodeCbson } from './cbson/writer';
import { decodeCbson } from './cbson/reader';
import { ClientKeyMap } from './key-map';
import { BLiteTransaction } from './transaction';
import { QueryBuilder } from './query/builder';
import { QueryDescriptor } from './query/descriptor';
import { serializeDescriptor } from './query/serializer';
import { callUnary, callServerStream } from './grpc/loader';
import { BLiteError } from './errors';

// ─── Index info ───────────────────────────────────────────────────────────────

export interface IndexInfo {
  name: string;
  field: string;
  type: 'BTree' | 'Vector' | 'Spatial';
  unique: boolean;
  dimensions: number;
  metric: string;
}

export interface VectorSourceInfo {
  separator: string;
  fields: Array<{ path: string; prefix?: string; suffix?: string }>;
}

export interface TimeSeriesInfo {
  isTimeSeries: boolean;
  ttlFieldName?: string;
  retentionMs: number;
}

export interface SchemaFieldInfo {
  name: string;
  typeCode: number;
  nullable: boolean;
}

export interface SchemaInfo {
  hasSchema: boolean;
  title?: string;
  version?: number;
  versionCount: number;
  fields: SchemaFieldInfo[];
}

// ─── DynamicCollection ────────────────────────────────────────────────────────

export class DynamicCollection {
  constructor(
    public readonly name: string,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    private readonly _dynStub: any,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    private readonly _metaStub: any,
    private readonly _keyMap: ClientKeyMap,
  ) {}

  // ── CRUD ────────────────────────────────────────────────────────────────────

  async insert(doc: BsonDocument, tx?: BLiteTransaction): Promise<BsonId> {
    await this._ensureKeys(doc);
    const payload = encodeCbson(doc, this._keyMap.forward);
    const res = await callUnary<unknown, { id: { value: Buffer; id_type: number }; error: string }>(
      this._dynStub,
      'Insert',
      {
        collection: this.name,
        bson_payload: payload,
        transaction_id: tx?.transactionId ?? '',
      },
    );
    BLiteError.check(res.error);
    return BsonId.fromProto(res.id);
  }

  async insertBulk(docs: BsonDocument[], tx?: BLiteTransaction): Promise<BsonId[]> {
    if (docs.length === 0) return [];
    for (const doc of docs) await this._ensureKeys(doc);
    const payloads = docs.map((d) => encodeCbson(d, this._keyMap.forward));
    const res = await callUnary<unknown, { ids: Array<{ value: Buffer; id_type: number }>; error: string }>(
      this._dynStub,
      'InsertBulk',
      {
        collection: this.name,
        payloads,
        transaction_id: tx?.transactionId ?? '',
      },
    );
    BLiteError.check(res.error);
    return res.ids.map(BsonId.fromProto);
  }

  async findById(id: BsonId): Promise<BsonDocument | null> {
    const res = await callUnary<unknown, { bson_payload: Buffer; found: boolean; error: string }>(
      this._dynStub,
      'FindById',
      { collection: this.name, id: id.toProto() },
    );
    BLiteError.check(res.error);
    if (!res.found || !res.bson_payload?.length) return null;
    return decodeCbson(Buffer.from(res.bson_payload), this._keyMap.reverse);
  }

  async* findAll(): AsyncGenerator<BsonDocument> {
    yield* this._executeQuery({ collection: this.name });
  }

  async update(id: BsonId, doc: BsonDocument, tx?: BLiteTransaction): Promise<boolean> {
    await this._ensureKeys(doc);
    const payload = encodeCbson(doc, this._keyMap.forward);
    const res = await callUnary<unknown, { success: boolean; error: string }>(
      this._dynStub,
      'Update',
      {
        collection: this.name,
        id: id.toProto(),
        bson_payload: payload,
        transaction_id: tx?.transactionId ?? '',
      },
    );
    BLiteError.check(res.error);
    return res.success;
  }

  async updateBulk(
    updates: Array<{ id: BsonId; doc: BsonDocument }>,
    tx?: BLiteTransaction,
  ): Promise<number> {
    if (updates.length === 0) return 0;
    for (const { doc } of updates) await this._ensureKeys(doc);
    const items = updates.map(({ id, doc }) => ({
      id: id.toProto(),
      bson_payload: encodeCbson(doc, this._keyMap.forward),
    }));
    const res = await callUnary<unknown, { affected_count: number; error: string }>(
      this._dynStub,
      'UpdateBulk',
      { collection: this.name, items, transaction_id: tx?.transactionId ?? '' },
    );
    BLiteError.check(res.error);
    return res.affected_count;
  }

  async delete(id: BsonId, tx?: BLiteTransaction): Promise<boolean> {
    const res = await callUnary<unknown, { success: boolean; error: string }>(
      this._dynStub,
      'Delete',
      {
        collection: this.name,
        id: id.toProto(),
        transaction_id: tx?.transactionId ?? '',
      },
    );
    BLiteError.check(res.error);
    return res.success;
  }

  async deleteBulk(ids: BsonId[], tx?: BLiteTransaction): Promise<number> {
    if (ids.length === 0) return 0;
    const res = await callUnary<unknown, { affected_count: number; error: string }>(
      this._dynStub,
      'DeleteBulk',
      {
        collection: this.name,
        ids: ids.map((id) => id.toProto()),
        transaction_id: tx?.transactionId ?? '',
      },
    );
    BLiteError.check(res.error);
    return res.affected_count;
  }

  // ── Querying ────────────────────────────────────────────────────────────────

  /** Returns a fluent QueryBuilder scoped to this collection. */
  query(): QueryBuilder<BsonDocument> {
    return new QueryBuilder<BsonDocument>(this.name, (d) => this._executeQuery(d));
  }

  async* executeDescriptor(descriptor: QueryDescriptor): AsyncGenerator<BsonDocument> {
    yield* this._executeQuery(descriptor);
  }

  // ── Index management ────────────────────────────────────────────────────────

  async createIndex(field: string, opts?: { name?: string; unique?: boolean }): Promise<void> {
    const res = await callUnary<unknown, { success: boolean; error: string }>(
      this._dynStub,
      'CreateIndex',
      {
        collection: this.name,
        field,
        name: opts?.name ?? '',
        unique: opts?.unique ?? false,
      },
    );
    BLiteError.check(res.error);
  }

  async createVectorIndex(
    field: string,
    dimensions: number,
    opts?: { metric?: 'Cosine' | 'L2' | 'DotProduct'; name?: string },
  ): Promise<void> {
    const res = await callUnary<unknown, { success: boolean; error: string }>(
      this._dynStub,
      'CreateIndex',
      {
        collection: this.name,
        field,
        name: opts?.name ?? '',
        is_vector: true,
        dimensions,
        metric: opts?.metric ?? 'Cosine',
      },
    );
    BLiteError.check(res.error);
  }

  async createSpatialIndex(field: string, name?: string): Promise<void> {
    const res = await callUnary<unknown, { success: boolean; error: string }>(
      this._dynStub,
      'CreateIndex',
      { collection: this.name, field, name: name ?? '', is_spatial: true },
    );
    BLiteError.check(res.error);
  }

  async dropIndex(indexName: string): Promise<boolean> {
    const res = await callUnary<unknown, { success: boolean; error: string }>(
      this._dynStub,
      'DropIndex',
      { collection: this.name, name: indexName },
    );
    BLiteError.check(res.error);
    return res.success;
  }

  async listIndexes(): Promise<IndexInfo[]> {
    const res = await callUnary<unknown, { indexes: IndexInfo[]; error: string }>(
      this._dynStub,
      'ListIndexes',
      { collection: this.name },
    );
    BLiteError.check(res.error);
    return res.indexes ?? [];
  }

  // ── Vector search ────────────────────────────────────────────────────────────

  async* vectorSearch(
    queryVector: number[],
    opts?: { k?: number; indexName?: string; efSearch?: number },
  ): AsyncGenerator<BsonDocument> {
    for await (const item of callServerStream<unknown, { bson_payload: Buffer; found: boolean; error: string }>(
      this._dynStub,
      'VectorSearch',
      {
        collection: this.name,
        index_name: opts?.indexName ?? '',
        query_vector: queryVector,
        k: opts?.k ?? 10,
        ef_search: opts?.efSearch ?? 100,
      },
    )) {
      BLiteError.check(item.error);
      if (item.bson_payload?.length) {
        yield decodeCbson(Buffer.from(item.bson_payload), this._keyMap.reverse);
      }
    }
  }

  // ── VectorSource ─────────────────────────────────────────────────────────────

  async setVectorSource(
    fields: Array<{ path: string; prefix?: string; suffix?: string }>,
    separator = ' ',
  ): Promise<void> {
    const res = await callUnary<unknown, { success: boolean; error: string }>(
      this._dynStub,
      'SetVectorSource',
      {
        collection: this.name,
        separator,
        fields: fields.map((f) => ({ path: f.path, prefix: f.prefix ?? '', suffix: f.suffix ?? '' })),
      },
    );
    BLiteError.check(res.error);
  }

  async getVectorSource(): Promise<VectorSourceInfo | null> {
    const res = await callUnary<unknown, {
      configured: boolean;
      separator: string;
      fields: Array<{ path: string; prefix: string; suffix: string }>;
      error: string;
    }>(this._dynStub, 'GetVectorSource', { collection: this.name });
    BLiteError.check(res.error);
    if (!res.configured) return null;
    return {
      separator: res.separator,
      fields: res.fields.map((f) => ({
        path: f.path,
        prefix: f.prefix || undefined,
        suffix: f.suffix || undefined,
      })),
    };
  }

  // ── TimeSeries ───────────────────────────────────────────────────────────────

  async configureTimeSeries(ttlFieldName: string, retentionMs: number): Promise<void> {
    const res = await callUnary<unknown, { success: boolean; error: string }>(
      this._dynStub,
      'ConfigureTimeSeries',
      { collection: this.name, ttl_field_name: ttlFieldName, retention_ms: retentionMs },
    );
    BLiteError.check(res.error);
  }

  async getTimeSeriesInfo(): Promise<TimeSeriesInfo> {
    const res = await callUnary<unknown, {
      is_time_series: boolean;
      ttl_field_name: string;
      retention_ms: string | number;
      error: string;
    }>(this._dynStub, 'GetTimeSeriesInfo', { collection: this.name });
    BLiteError.check(res.error);
    return {
      isTimeSeries: res.is_time_series,
      ttlFieldName: res.ttl_field_name || undefined,
      retentionMs: Number(res.retention_ms),
    };
  }

  async forcePrune(): Promise<void> {
    const res = await callUnary<unknown, { success: boolean; error: string }>(
      this._dynStub,
      'ForcePrune',
      { collection: this.name },
    );
    BLiteError.check(res.error);
  }

  // ── Schema ───────────────────────────────────────────────────────────────────

  async getSchema(): Promise<SchemaInfo> {
    const res = await callUnary<unknown, {
      has_schema: boolean;
      title: string;
      version: number;
      version_count: number;
      fields: Array<{ name: string; type: number; nullable: boolean }>;
      error: string;
    }>(this._dynStub, 'GetSchema', { collection: this.name });
    BLiteError.check(res.error);
    return {
      hasSchema: res.has_schema,
      title: res.title || undefined,
      version: res.version || undefined,
      versionCount: res.version_count,
      fields: (res.fields ?? []).map((f) => ({ name: f.name, typeCode: f.type, nullable: f.nullable })),
    };
  }

  async setSchema(
    fields: Array<{ name: string; typeCode: number; nullable?: boolean }>,
    title?: string,
  ): Promise<void> {
    const res = await callUnary<unknown, { success: boolean; error: string }>(
      this._dynStub,
      'SetSchema',
      {
        collection: this.name,
        title: title ?? '',
        fields: fields.map((f) => ({ name: f.name, type: f.typeCode, nullable: f.nullable ?? false })),
      },
    );
    BLiteError.check(res.error);
  }

  // ── Change Data Capture ───────────────────────────────────────────────────────

  async* watch(capturePayload = false): AsyncGenerator<ChangeEvent> {
    for await (const item of callServerStream<unknown, {
      timestamp: string;
      transaction_id: string;
      collection: string;
      operation: number;
      document_id: { value: Buffer; id_type: number };
      bson_payload: Buffer;
      error: string;
    }>(this._dynStub, 'Watch', { collection: this.name, capture_payload: capturePayload })) {
      BLiteError.check(item.error);
      yield {
        timestamp: Number(item.timestamp),
        transactionId: item.transaction_id,
        collection: item.collection,
        operation: item.operation as OperationType,
        documentId: BsonId.fromProto(item.document_id),
        payload: item.bson_payload?.length
          ? decodeCbson(Buffer.from(item.bson_payload), this._keyMap.reverse)
          : undefined,
      };
    }
  }

  // ── Key-map helpers ───────────────────────────────────────────────────────────

  async refreshKeyMap(): Promise<void> {
    await this._keyMap.refresh(this._metaStub, this.name);
  }

  // ── Internal helpers ──────────────────────────────────────────────────────────

  private async _ensureKeys(doc: BsonDocument): Promise<void> {
    const keys = ClientKeyMap.collectKeys(doc);
    await this._keyMap.ensureRegistered(this._metaStub, this.name, keys);
  }

  private async* _executeQuery(descriptor: QueryDescriptor): AsyncGenerator<BsonDocument> {
    const bytes = serializeDescriptor({ ...descriptor, collection: this.name });
    for await (const item of callServerStream<unknown, { bson_payload: Buffer; found: boolean; error: string }>(
      this._dynStub,
      'Query',
      { query_descriptor: Buffer.from(bytes) },
    )) {
      BLiteError.check(item.error);
      if (item.bson_payload?.length) {
        yield decodeCbson(Buffer.from(item.bson_payload), this._keyMap.reverse);
      }
    }
  }
}

// ─── CDC types ────────────────────────────────────────────────────────────────

export const enum OperationType {
  Insert = 0,
  Update = 1,
  Delete = 2,
}

export interface ChangeEvent {
  timestamp: number;
  transactionId: string;
  collection: string;
  operation: OperationType;
  documentId: BsonId;
  payload?: BsonDocument;
}
