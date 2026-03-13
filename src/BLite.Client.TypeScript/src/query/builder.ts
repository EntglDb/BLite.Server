// blite-client — fluent QueryBuilder
//
// Provides a chainable API to build a QueryDescriptor without manually
// constructing the filter tree.  Equivalent to the LINQ API on the C# client.
//
// Usage:
//   const results = collection.query()
//     .where('age', FilterOp.Gt, 18)
//     .where('status', FilterOp.Eq, 'active')
//     .orderBy('name')
//     .skip(0).take(20)
//     .execute();

import {
  QueryDescriptor,
  FilterNode,
  SortSpec,
  FilterOp,
  and,
  BinaryFilter,
  ScalarValue,
  eq,
  neq,
  gt,
  gte,
  lt,
  lte,
  startsWith,
  contains,
  inList,
} from './descriptor';

export type ExecuteStream<T> = AsyncGenerator<T>;

export class QueryBuilder<T> {
  private readonly _collection: string;
  private readonly _execute: (d: QueryDescriptor) => AsyncGenerator<T>;
  private _filters: FilterNode[] = [];
  private _sorts: SortSpec[] = [];
  private _skip?: number;
  private _take?: number;

  constructor(collection: string, execute: (d: QueryDescriptor) => AsyncGenerator<T>) {
    this._collection = collection;
    this._execute = execute;
  }

  // ── Filter methods ──────────────────────────────────────────────────────────

  /** Adds a binary predicate (AND-ed with any existing filters). */
  where(field: string, op: FilterOp, value: unknown): this {
    this._filters.push({ kind: 'binary', field, op, value: ScalarValue.from(value) });
    return this;
  }

  whereEq(field: string, value: unknown):       this { return this.addFilter(eq(field, value)); }
  whereNeq(field: string, value: unknown):      this { return this.addFilter(neq(field, value)); }
  whereGt(field: string, value: unknown):       this { return this.addFilter(gt(field, value)); }
  whereGte(field: string, value: unknown):      this { return this.addFilter(gte(field, value)); }
  whereLt(field: string, value: unknown):       this { return this.addFilter(lt(field, value)); }
  whereLte(field: string, value: unknown):      this { return this.addFilter(lte(field, value)); }
  whereStartsWith(field: string, v: string):    this { return this.addFilter(startsWith(field, v)); }
  whereContains(field: string, v: string):      this { return this.addFilter(contains(field, v)); }
  whereIn(field: string, values: unknown[]):    this { return this.addFilter(inList(field, values)); }

  /** Adds a pre-built FilterNode (AND-ed with any existing filters). */
  addFilter(f: FilterNode): this {
    this._filters.push(f);
    return this;
  }

  // ── Sort methods ────────────────────────────────────────────────────────────

  orderBy(field: string, descending = false): this {
    this._sorts.push({ field, descending });
    return this;
  }

  orderByDescending(field: string): this {
    return this.orderBy(field, true);
  }

  // ── Pagination ──────────────────────────────────────────────────────────────

  skip(n: number): this {
    this._skip = n;
    return this;
  }

  take(n: number): this {
    this._take = n;
    return this;
  }

  // ── Terminal operators ──────────────────────────────────────────────────────

  /** Streams results from the server. Backpressure-aware AsyncGenerator. */
  execute(): AsyncGenerator<T> {
    return this._execute(this.build());
  }

  /** Collects all results into an array. */
  async toArray(): Promise<T[]> {
    const results: T[] = [];
    for await (const item of this.execute()) results.push(item);
    return results;
  }

  /** Returns the first result, or undefined if none. */
  async first(): Promise<T | undefined> {
    for await (const item of this.take(1).execute()) return item;
    return undefined;
  }

  /** Counts results by collecting them all (no server-side COUNT support yet). */
  async count(): Promise<number> {
    let n = 0;
    for await (const _ of this.execute()) n++;
    return n;
  }

  // ── Descriptor builder ──────────────────────────────────────────────────────

  build(): QueryDescriptor {
    const where = this._filters.length === 0
      ? undefined
      : this._filters.length === 1
      ? this._filters[0]
      : and(...this._filters);

    return {
      collection: this._collection,
      where,
      orderBy: this._sorts.length > 0 ? this._sorts : undefined,
      take: this._take,
      skip: this._skip,
    };
  }
}
