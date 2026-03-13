// blite-client — public API
export { BLiteClient, BLiteClientOptions } from './client';
export { BLiteTransaction } from './transaction';
export { BLiteError } from './errors';

// Types
export { BsonId, BsonIdType, BsonDocument, BsonValue, BsonObjectId } from './cbson/types';

// Collections
export { DynamicCollection, IndexInfo, VectorSourceInfo, TimeSeriesInfo, SchemaInfo, SchemaFieldInfo, ChangeEvent, OperationType } from './dynamic-collection';
export { Collection, CollectionMapper } from './collection';

// Admin
export { AdminClient, BLiteOperation, UserPermission, UserInfo, TenantInfo } from './admin-client';

// KV
export { KvStore, KvBatch, KvBatchOp } from './kv-store';

// Query
export { FilterOp, LogicalOp, ScalarKind, ScalarValue, FilterNode, BinaryFilter, LogicalFilter, UnaryFilter, QueryDescriptor, ProjectionSpec, SortSpec, and, or, not, eq, neq, gt, gte, lt, lte, startsWith, contains, inList } from './query/descriptor';
export { QueryBuilder } from './query/builder';
export { serializeDescriptor } from './query/serializer';

// C-BSON (advanced usage)
export { encodeCbson } from './cbson/writer';
export { decodeCbson } from './cbson/reader';
