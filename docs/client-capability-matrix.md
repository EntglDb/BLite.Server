# BLite Client Capability Matrix (v1)

This matrix defines the minimum parity contract across official BLite clients.

## Supported Client Set

- BLite.Client (.NET) - baseline reference
- BLite.Client.TypeScript (Node.js)
- BLite.Client.Python
- BLite.Client.Java

## Status Legend

- Required: must be implemented for GA.
- Optional: can ship later, but must be explicitly marked experimental in all clients.

## Capability Matrix

| Area | Capability | .NET Baseline | TypeScript | Python | Java | Requirement | Notes |
|---|---|---|---|---|---|---|---|
| Connection | Host/port/tls/api-key configuration | Yes | Planned | Planned | Planned | Required | Same option names where possible |
| Connection | Header auth (`x-api-key` / bearer) | Yes | Planned | Planned | Planned | Required | Consistent auth error mapping |
| Collections | Dynamic collection access | Yes | Planned | Planned | Planned | Required | CRUD + query |
| Collections | Typed collection access | Yes | Planned | Planned | Planned | Required | Mapper/serializer abstraction |
| CRUD | Insert / FindById / Update / Delete | Yes | Planned | Planned | Planned | Required | Same semantics and return types |
| Bulk | InsertBulk / UpdateBulk / DeleteBulk | Yes | Planned | Planned | Planned | Required | Partial failure model must match |
| Query | QueryDescriptor push-down | Yes | Planned | Planned | Planned | Required | Filter + sort + skip + take |
| Query | Streaming query results | Yes | Planned | Planned | Planned | Required | Async streaming API in each language |
| Query | LINQ/DSL fluent query layer | Yes | Planned | Planned | Planned | Required | Language-native API, same behavior |
| Transactions | Begin / Commit / Rollback | Yes | Planned | Planned | Planned | Required | Transaction ID behavior aligned |
| Index | Create / Drop / List indexes | Yes | Planned | Planned | Planned | Required | BTree + vector index metadata |
| Index | QueryIndex | Yes | Planned | Planned | Planned | Required | Ordering/skip/take parity |
| Vector | VectorSearch API | Yes | Planned | Planned | Planned | Required | Same metric options |
| CDC | Watch / change stream | Yes | Planned | Planned | Planned | Required | Include payload toggle |
| KV | Get / Set / Exists / Delete / Scan / Batch / Refresh / PurgeExpired | Yes | Planned | Planned | Planned | Required | Same TTL semantics |
| Admin | User management | Yes | Planned | Planned | Planned | Required | Create/revoke/rotate/list/update perms |
| Admin | Tenant management | Yes | Planned | Planned | Planned | Required | Provision/deprovision/list tenants |
| Errors | Normalized error model | Yes | Planned | Planned | Planned | Required | Status code + server message + context |
| Diagnostics | Request/response diagnostics hooks | Yes | Planned | Planned | Planned | Required | Structured logs/trace hooks |
| Packaging | Public package publishing | Yes | Planned | Planned | Planned | Required | NuGet/npm/PyPI/Maven Central |
| Compatibility | Cross-client conformance tests | Partial | Planned | Planned | Planned | Required | Same test suite against BLite.Server |

## Release Gate Policy

A client version can be marked GA only when all Required capabilities are complete and tested against the same BLite.Server version.

### Gate checks

1. API parity: all Required endpoints/RPCs implemented.
2. Behavioral parity: conformance tests green.
3. Error parity: canonical error mapping validated.
4. Performance sanity: baseline throughput and latency checks completed.
5. Documentation parity: quickstart + CRUD + query + transaction + watch + KV + admin examples.

## Versioning Policy

- Capability version: this document version (currently v1) defines required scope.
- Client package version: independent per ecosystem.
- Server compatibility: each client release must declare tested BLite.Server versions.

## Delivery Order

1. TypeScript client (complete v1 parity)
2. Python client (complete v1 parity)
3. Java client (complete v1 parity)

No next client starts GA hardening until the current client reaches full v1 parity.
