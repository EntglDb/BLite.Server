// tests/error.test.ts — BLiteError

import { BLiteError } from '../src/errors';

describe('BLiteError', () => {
  test('is an instance of Error', () => {
    const e = new BLiteError('oops');
    expect(e).toBeInstanceOf(Error);
  });

  test('name is BLiteError', () => {
    expect(new BLiteError('msg').name).toBe('BLiteError');
  });

  test('message is preserved', () => {
    expect(new BLiteError('something went wrong').message).toBe('something went wrong');
  });

  describe('BLiteError.check()', () => {
    test('throws on non-empty error string', () => {
      expect(() => BLiteError.check('Permission denied')).toThrow(BLiteError);
    });

    test('thrown error carries the message', () => {
      let caught: unknown;
      try { BLiteError.check('not found'); } catch (e) { caught = e; }
      expect((caught as BLiteError).message).toBe('not found');
    });

    test('does not throw on empty string', () => {
      expect(() => BLiteError.check('')).not.toThrow();
    });

    test('does not throw on null', () => {
      expect(() => BLiteError.check(null as unknown as string)).not.toThrow();
    });

    test('does not throw on undefined', () => {
      expect(() => BLiteError.check(undefined as unknown as string)).not.toThrow();
    });
  });
});
