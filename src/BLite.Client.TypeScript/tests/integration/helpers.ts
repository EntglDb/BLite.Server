// tests/integration/helpers.ts
//
// Shared setup for BLite integration tests.
//
// Environment variables:
//   BLITE_HOST     — server hostname   (default: localhost)
//   BLITE_PORT     — gRPC port         (default: 2626)
//   BLITE_API_KEY  — API key           (default: dev)
//   BLITE_TLS      — 'true' for TLS    (default: false)

import { BLiteClient } from '../../src/client';

const HOST    = process.env['BLITE_HOST']    ?? 'localhost';
const PORT    = parseInt(process.env['BLITE_PORT'] ?? '2626', 10);
const KEY     = process.env['BLITE_API_KEY'] ?? '807ab8d026ccef15583ada824c78e538bcf1a77888566b971c244e63fe492455';
const USE_TLS = process.env['BLITE_TLS']    === 'true';

/** Creates a fresh client using env-configured (or default) coordinates. */
export function createClient(): BLiteClient {
  return new BLiteClient({ host: HOST, port: PORT, apiKey: KEY, useTls: USE_TLS });
}

/**
 * Checks whether the BLite server is reachable within 3 seconds.
 * Returns false (instead of throwing) on any network / auth error.
 */
export async function checkAvailability(client: BLiteClient): Promise<boolean> {
  const timeout = new Promise<boolean>((resolve) => setTimeout(() => resolve(false), 3000));
  const probe   = client.listCollections().then(() => true).catch(() => false);
  return Promise.race([probe, timeout]);
}

/** Returns a unique collection name safe to use and drop in a test suite. */
export function uniqueCol(tag: string): string {
  return `ts_int_${tag}_${Date.now()}`;
}
