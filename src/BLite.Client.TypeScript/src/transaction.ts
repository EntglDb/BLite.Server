// blite-client — BLiteTransaction
//
// Wraps a server-side transaction.  Pass the transaction to collection write
// methods; call commit() when done.  Dispose (or call rollback()) on failure.
// Implements AsyncDisposable so it can be used with "await using".

import { callUnary } from './grpc/loader';
import { BLiteError } from './errors';

export class BLiteTransaction implements AsyncDisposable {
  private _committed = false;
  private _rolledBack = false;

  constructor(
    public readonly transactionId: string,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    private readonly _txnStub: any,
  ) {}

  get committed(): boolean { return this._committed; }
  get rolledBack(): boolean { return this._rolledBack; }
  get active(): boolean { return !this._committed && !this._rolledBack; }

  async commit(): Promise<void> {
    if (!this.active) throw new BLiteError('Transaction is no longer active');
    const res = await callUnary<unknown, { success: boolean; error: string }>(
      this._txnStub,
      'Commit',
      { transaction_id: this.transactionId },
    );
    BLiteError.check(res.error);
    this._committed = true;
  }

  async rollback(): Promise<void> {
    if (!this.active) return;
    const res = await callUnary<unknown, { success: boolean; error: string }>(
      this._txnStub,
      'Rollback',
      { transaction_id: this.transactionId },
    );
    BLiteError.check(res.error);
    this._rolledBack = true;
  }

  async [Symbol.asyncDispose](): Promise<void> {
    if (this.active) await this.rollback();
  }
}
