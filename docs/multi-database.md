# Multi-Database Support (Physical Tenant Isolation)

## Overview

BLite Server supports running **multiple independent databases** in the same server
instance, with each tenant mapped to its own physical `.db` file on the server.
This is the recommended architecture for GDPR-compliant multi-tenant deployments.

## Motivation

| Isolation model          | Default (namespace prefix)       | Physical DB per tenant       |
|--------------------------|----------------------------------|------------------------------|
| Right to erasure (GDPR)  | Requires filtered export         | Delete the `.db` file        |
| Data portability         | Filtered export                  | Copy the `.db` file          |
| Data residency           | Not enforceable                  | Configurable path per tenant |
| Breach blast radius      | All tenants exposed              | Single tenant exposed        |
| Backup granularity       | Full-server only                 | Per-tenant                   |
| Audit trail              | Shared engine                    | Fully independent            |

## Architecture

```
API Key -? AuthorizationService -? BLiteUser.DatabaseId
                                            �
                                            ?
                                    EngineRegistry
                         +--------------------------------------+
                         � null / ""  ?  BLiteEngine (default)  � --? blite.db  (system/auth)
                         � "acme"     ?  BLiteEngine (lazy)     � --? data/tenants/acme.db
                         � "globex"   ?  BLiteEngine (lazy)     � --? data/tenants/globex.db
                         +--------------------------------------+
                                            �
                                     DatabasesDirectory
                                     (hot provisioned at runtime)
```

- **System database** (`null` / empty `database_id`) � always open; hosts the `_users`
  collection and is the default for any user without an explicit `DatabaseId`.
- **Named tenant databases** � stored under `BLiteServer:DatabasesDirectory`.
  Engines are opened **lazily** on first access, and **created at runtime** via the
  `ProvisionTenant` Admin RPC (no restart required).
- **DatabaseId normalisation** � IDs are lowercased and trimmed; `"ACME"` and `"acme"`
  map to the same database.

## Configuration

`appsettings.json`:

```json
{
  "BLiteServer": {
    "DatabasePath": "blite.db",
    "MaxPageSizeBytes": 16384,
    "DatabasesDirectory": "data/tenants"
  }
}
```

| Key                       | Default        | Description                                      |
|---------------------------|----------------|--------------------------------------------------|
| `DatabasePath`            | `blite.db`     | Path to the system (default) database file.      |
| `MaxPageSizeBytes`        | `16384`        | Page size, shared by all databases.              |
| `DatabasesDirectory`      | `data/tenants` | Directory where tenant `.db` files are stored.   |

The directory is created automatically if it does not exist.  
Existing `.db` files in the directory are discovered lazily on first access.

## Provisioning a New Tenant at Runtime

Use the `AdminService.ProvisionTenant` gRPC RPC (requires `Admin` permission):

```protobuf
rpc ProvisionTenant   (ProvisionTenantRequest)   returns (ProvisionTenantResponse);
rpc DeprovisionTenant (DeprovisionTenantRequest) returns (DeprovisionTenantResponse);
rpc ListTenants       (Empty)                    returns (ListTenantsResponse);
```

No server restart is required. The new `.db` file is created and its engine is
registered in memory immediately.

Example workflow:

```
1. Admin calls ProvisionTenant { database_id: "newcorp" }
   ? creates data/tenants/newcorp.db

2. Admin calls CreateUser {
       username: "alice",
       database_id: "newcorp",
       permissions: [{ collection: "*", ops: 7 }]
   }

3. alice sends requests with her API key
   ? BLiteUser.DatabaseId = "newcorp"
   ? EngineRegistry routes all operations to data/tenants/newcorp.db
```

## GDPR Right-to-Erasure

To permanently delete all data for a tenant:

```
1. Admin calls DeprovisionTenant { database_id: "newcorp", delete_files: true }
   ? engine is closed, data/tenants/newcorp.db (and its .wal file) are deleted

2. Admin calls RevokeUser for each user belonging to that tenant
   (or simply rotate the root key to prevent any future access)
```

Since the data is in a separate physical file, erasure is immediate and complete.
No partial deletions, no index remnants.

## Transaction Isolation

Each named database has its own transaction semaphore. A transaction begun on
`"acme"` does not block writes on `"globex"` or the system database.

```
BeginTransaction(user=alice, db="acme")   ? acquires acme-semaphore
BeginTransaction(user=bob,   db="globex") ? acquires globex-semaphore (independent)
```

## Key Map Isolation

Each physical database maintains an **independent C-BSON key map** (field name ? ushort ID).
Clients connecting to different tenants will negotiate their key maps independently
via `MetadataService.RegisterKeys` / `GetKeyMap`, which are scoped to the resolved engine.

## Lifecycle of a Tenant Engine

```
Provisioned --? File created, engine opened, added to _active dict
     �
     ?
Active ------? GetEngine() returns it instantly (O(1) ConcurrentDictionary lookup)
     �
     ?
Lazy-open ---? File exists but never accessed ? engine opens on first GetEngine() call
     �
     ?
Deprovisioned ? engine.Dispose() + optional file deletion ? removed from _active dict
```

## Security Notes

- Tenant IDs are used as file names; only lowercase alphanumeric characters, hyphens
  and underscores are safe. The server **does not validate** the format currently �
  consider adding an `[A-Za-z0-9_-]+` constraint before production deployment if
  tenant IDs originate from user input.
- The system database contains user credentials; it is never accessible via a
  named `database_id`.
- Backup the `DatabasesDirectory` regularly; each `.db` file is independent and
  can be backed up or restored individually.
