// tests/transaction.test.ts — BLiteTransaction state machine

import { BLiteTransaction } from '../src/transaction';
import { BLiteError } from '../src/errors';

function makeStub() {
  const stub = {
    Commit: jest.fn((_req: unknown, cb: (e: null, r: { success: boolean; error: string }) => void) =>
      cb(null, { success: true, error: '' })),
    Rollback: jest.fn((_req: unknown, cb: (e: null, r: { success: boolean; error: string }) => void) =>
      cb(null, { success: true, error: '' })),
  };
  return stub;
}

describe('BLiteTransaction', () => {
  test('initial state: active=true, committed=false, rolledBack=false', () => {
    const stub = makeStub();
    const txn = new BLiteTransaction('txn-001', stub as never);
    expect(txn.transactionId).toBe('txn-001');
    expect(txn.active).toBe(true);
    expect(txn.committed).toBe(false);
    expect(txn.rolledBack).toBe(false);
  });

  test('commit() transitions to committed', async () => {
    const stub = makeStub();
    const txn = new BLiteTransaction('txn-001', stub as never);
    await txn.commit();
    expect(txn.committed).toBe(true);
    expect(txn.active).toBe(false);
    expect(txn.rolledBack).toBe(false);
  });

  test('commit() calls Commit RPC', async () => {
    const stub = makeStub();
    const txn = new BLiteTransaction('txn-001', stub as never);
    await txn.commit();
    expect(stub.Commit).toHaveBeenCalledWith(
      expect.objectContaining({ transaction_id: 'txn-001' }),
      expect.any(Function),
    );
  });

  test('rollback() transitions to rolledBack', async () => {
    const stub = makeStub();
    const txn = new BLiteTransaction('txn-001', stub as never);
    await txn.rollback();
    expect(txn.rolledBack).toBe(true);
    expect(txn.active).toBe(false);
    expect(txn.committed).toBe(false);
  });

  test('rollback() calls Rollback RPC', async () => {
    const stub = makeStub();
    const txn = new BLiteTransaction('txn-001', stub as never);
    await txn.rollback();
    expect(stub.Rollback).toHaveBeenCalledWith(
      expect.objectContaining({ transaction_id: 'txn-001' }),
      expect.any(Function),
    );
  });

  test('double commit() throws BLiteError', async () => {
    const stub = makeStub();
    const txn = new BLiteTransaction('txn-001', stub as never);
    await txn.commit();
    await expect(txn.commit()).rejects.toBeInstanceOf(BLiteError);
  });

  test('double rollback() is a no-op (does not throw)', async () => {
    const stub = makeStub();
    const txn = new BLiteTransaction('txn-001', stub as never);
    await txn.rollback();
    await expect(txn.rollback()).resolves.toBeUndefined();
    // Second call should not hit the RPC again
    expect(stub.Rollback).toHaveBeenCalledTimes(1);
  });

  test('commit after rollback throws BLiteError', async () => {
    const stub = makeStub();
    const txn = new BLiteTransaction('txn-001', stub as never);
    await txn.rollback();
    await expect(txn.commit()).rejects.toBeInstanceOf(BLiteError);
  });

  test('rollback after commit is a no-op (does not throw)', async () => {
    const stub = makeStub();
    const txn = new BLiteTransaction('txn-001', stub as never);
    await txn.commit();
    await expect(txn.rollback()).resolves.toBeUndefined();
    expect(stub.Rollback).not.toHaveBeenCalled();
  });

  test('[Symbol.asyncDispose] rolls back if not committed', async () => {
    const stub = makeStub();
    const txn = new BLiteTransaction('txn-001', stub as never);
    await txn[Symbol.asyncDispose]();
    expect(txn.rolledBack).toBe(true);
    expect(stub.Rollback).toHaveBeenCalledTimes(1);
  });

  test('[Symbol.asyncDispose] is a no-op if already committed', async () => {
    const stub = makeStub();
    const txn = new BLiteTransaction('txn-001', stub as never);
    await txn.commit();
    await txn[Symbol.asyncDispose](); // should not throw
    expect(stub.Rollback).not.toHaveBeenCalled();
  });

  test('[Symbol.asyncDispose] is a no-op if already rolled back', async () => {
    const stub = makeStub();
    const txn = new BLiteTransaction('txn-001', stub as never);
    await txn.rollback();
    await txn[Symbol.asyncDispose](); // should not throw
    expect(stub.Rollback).toHaveBeenCalledTimes(1); // only the first call
  });
});
