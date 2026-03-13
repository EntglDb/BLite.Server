// blite-client — BLiteError
export class BLiteError extends Error {
  constructor(message: string, public readonly code?: string) {
    super(message);
    this.name = 'BLiteError';
  }

  static check(error: string | undefined | null): void {
    if (error) throw new BLiteError(error);
  }
}
