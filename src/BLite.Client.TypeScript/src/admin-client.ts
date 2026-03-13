// blite-client — AdminClient
//
// Wraps AdminService for user and tenant management.
// All methods require Admin permission on the connected user's API key.

import { callUnary } from './grpc/loader';
import { BLiteError } from './errors';

// ─── Operation flags (mirrors BLiteOperation) ─────────────────────────────────

export const enum BLiteOperation {
  None   = 0,
  Query  = 1,
  Insert = 2,
  Update = 4,
  Delete = 8,
  Drop   = 16,
  Admin  = 32,
  Write  = 14,  // Insert | Update | Delete
  All    = 63,  // Query | Write | Drop | Admin
}

// ─── DTOs ─────────────────────────────────────────────────────────────────────

export interface UserPermission {
  collection: string;
  ops: BLiteOperation;
}

export interface UserInfo {
  username: string;
  namespace: string;
  active: boolean;
  createdAt: string;
  permissions: UserPermission[];
  databaseId: string;
}

export interface TenantInfo {
  databaseId: string;
  databasePath: string;
  isActive: boolean;
}

// ─── AdminClient ─────────────────────────────────────────────────────────────

export class AdminClient {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  constructor(private readonly _stub: any) {}

  // ── User management ────────────────────────────────────────────────────────

  async createUser(
    username: string,
    opts?: {
      namespace?: string;
      permissions?: UserPermission[];
      databaseId?: string;
    },
  ): Promise<string> {
    const res = await callUnary<unknown, { api_key: string; error: string }>(
      this._stub,
      'CreateUser',
      {
        username,
        namespace: opts?.namespace ?? '',
        permissions: (opts?.permissions ?? []).map((p) => ({ collection: p.collection, ops: p.ops })),
        database_id: opts?.databaseId ?? '',
      },
    );
    BLiteError.check(res.error);
    return res.api_key;
  }

  async revokeUser(username: string): Promise<void> {
    const res = await callUnary<unknown, { success: boolean; error: string }>(
      this._stub,
      'RevokeUser',
      { username },
    );
    BLiteError.check(res.error);
  }

  async rotateKey(username: string): Promise<string> {
    const res = await callUnary<unknown, { api_key: string; error: string }>(
      this._stub,
      'RotateKey',
      { username },
    );
    BLiteError.check(res.error);
    return res.api_key;
  }

  async listUsers(): Promise<UserInfo[]> {
    const res = await callUnary<unknown, { users: Array<{
      username: string;
      namespace: string;
      active: boolean;
      created_at: string;
      permissions: Array<{ collection: string; ops: number }>;
      database_id: string;
    }>; error: string }>(this._stub, 'ListUsers', {});
    BLiteError.check(res.error);
    return (res.users ?? []).map((u) => ({
      username: u.username,
      namespace: u.namespace,
      active: u.active,
      createdAt: u.created_at,
      permissions: u.permissions.map((p) => ({ collection: p.collection, ops: p.ops as BLiteOperation })),
      databaseId: u.database_id,
    }));
  }

  async updatePermissions(username: string, permissions: UserPermission[]): Promise<void> {
    const res = await callUnary<unknown, { success: boolean; error: string }>(
      this._stub,
      'UpdatePerms',
      {
        username,
        permissions: permissions.map((p) => ({ collection: p.collection, ops: p.ops })),
      },
    );
    BLiteError.check(res.error);
  }

  // ── Tenant management ──────────────────────────────────────────────────────

  async provisionTenant(databaseId: string): Promise<void> {
    const res = await callUnary<unknown, { success: boolean; error: string }>(
      this._stub,
      'ProvisionTenant',
      { database_id: databaseId },
    );
    BLiteError.check(res.error);
  }

  async deprovisionTenant(databaseId: string, deleteFiles = false): Promise<void> {
    const res = await callUnary<unknown, { success: boolean; error: string }>(
      this._stub,
      'DeprovisionTenant',
      { database_id: databaseId, delete_files: deleteFiles },
    );
    BLiteError.check(res.error);
  }

  async listTenants(): Promise<TenantInfo[]> {
    const res = await callUnary<unknown, {
      tenants: Array<{ database_id: string; database_path: string; is_active: boolean }>;
      error: string;
    }>(this._stub, 'ListTenants', {});
    BLiteError.check(res.error);
    return (res.tenants ?? []).map((t) => ({
      databaseId: t.database_id,
      databasePath: t.database_path,
      isActive: t.is_active,
    }));
  }
}
