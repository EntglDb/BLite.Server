// blite-client — ClientKeyMap
//
// Maintains the bidirectional field-name ↔ ushort-id mapping required for
// C-BSON encoding and decoding.  The mapping is globally shared across all
// collections (the server uses a single global dictionary).

import { callUnary } from './grpc/loader';

// ─── Key map ─────────────────────────────────────────────────────────────────

export class ClientKeyMap {
  /** field name → ushort id (for encoding) */
  readonly forward: Map<string, number> = new Map();
  /** ushort id → field name (for decoding) */
  readonly reverse: Map<number, string> = new Map();

  private merge(entries: Record<string, number>): void {
    for (const [name, id] of Object.entries(entries)) {
      this.forward.set(name, id);
      this.reverse.set(id, name);
    }
  }

  /**
   * Ensures all given field names are registered with the server.
   * Only calls RegisterKeys for names not already in the local cache.
   */
  async ensureRegistered(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    metadataStub: any,
    collection: string,
    names: string[],
  ): Promise<void> {
    // Server normalizes all field names to lowercase; mirror that here so the
    // local forward map always uses the same casing as the server returns.
    const normalizedNames = names.map((n) => n.toLowerCase());
    const missing = normalizedNames.filter((n) => !this.forward.has(n));
    if (missing.length === 0) return;

    const res = await callUnary<unknown, { entries: Record<string, number>; error: string }>(
      metadataStub,
      'RegisterKeys',
      { collection, keys: missing },  // already lowercase
    );

    if (res.error) throw new Error(`RegisterKeys failed: ${res.error}`);
    this.merge(res.entries);
  }

  /**
   * Fetches the full global key map from the server (called once on connect
   * so that documents written by other clients can be decoded immediately).
   */
  async refresh(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    metadataStub: any,
    anchorCollection: string,
  ): Promise<void> {
    const res = await callUnary<unknown, { entries: Record<string, number>; error: string }>(
      metadataStub,
      'GetKeyMap',
      { collection: anchorCollection },
    );

    if (res.error) throw new Error(`GetKeyMap failed: ${res.error}`);
    this.merge(res.entries);
  }

  /** Returns all field names in the given document (flat, one level), normalized to lowercase. */
  static collectKeys(doc: Record<string, unknown>): string[] {
    return Object.keys(doc).map((k) => k.toLowerCase());
  }
}
