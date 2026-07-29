# Plan: @gizmodata/gizmosql-client 2.0 on the native Go ADBC driver

Status: **in progress** (started 2026-07-29)

## Goal

Rebase this client on `@apache-arrow/adbc-driver-manager` (the official
ADBC Node.js driver manager, a NAPI addon) loading
`libadbc_driver_gizmosql` — the native Go GizmoSQL ADBC driver from
[gizmodata/gizmosql-adbc](https://github.com/gizmodata/gizmosql-adbc).
The public TypeScript API (`FlightSQLClient` and friends) stays
compatible so `gizmosql-ui` (which imports only `FlightSQLClient`)
upgrades with at most trivial changes.

## Why

- **Delete ~all transport code**: the hand-rolled Flight SQL RPC layer
  (`flight-client.ts`, generated protos, grpc-js plumbing) is replaced
  by the shared Go driver used by Python/Go/C++/R.
- **Delete client-side OAuth**: `adbc.gizmosql.auth_type=external` runs
  the identical browser flow inside the Go driver.
- **Gain GizmoSQL semantics for free**: DDL/DML immediate execution
  under the lazy-execution model, `RETURNING` eager materialization,
  `gizmosql://` URIs (TLS by default), OpenTelemetry, profiles.
- One implementation, every language — the 2.0 thesis.

## Architecture

```
FlightSQLClient (public API, unchanged signatures)
  └─ AdbcDatabase({driver: resolveDriverLib(), uri, username, ...})
       └─ libadbc_driver_gizmosql.{so,dylib,dll}   (bundled/downloaded)
            └─ Go driver → upstream Flight SQL → GizmoSQL server
```

- Both the driver manager and `apache-arrow` results speak Arrow tables,
  matching the current API's return types.
- Requires Node.js **22+** (NAPI addon constraint) — document as a 2.0
  breaking change alongside the removed low-level `FlightClient` API.

## Driver library distribution (decision)

v2.0.0 ships with a **postinstall downloader**: fetch the platform's
`libadbc_driver_gizmosql-<ver>-<platform>.tar.gz` from the
gizmosql-adbc GitHub Release, verify by SHA-256, unpack into the
package's cache dir; `GIZMOSQL_DRIVER_LIB` env var overrides for
airgapped installs, and a clear error message covers download failure.
This needs zero new publishing secrets. Fast-follow (Philip-gated on an
npm token in the gizmosql-adbc repo): per-platform
`@gizmodata/gizmosql-driver-*` npm packages as `optionalDependencies`,
mirroring how `@apache-arrow/adbc-driver-manager` ships its own addon.

## API mapping (sketch)

| Current | 2.0 backing |
|---|---|
| `new FlightSQLClient(config)` / connect | `AdbcDatabase` with mapped options (`uri` from host/port/tls config, `username`, `password`, OAuth → `adbc.gizmosql.*`) |
| `execute(query): Table` | `connection.query()` (driver routes DDL/DML) |
| `prepare/executePrepared/closePrepared` | ADBC prepared statements |
| `getCatalogs/getSchemas/getTables/getTableTypes` | `connection.getObjects()` / metadata APIs |
| `getSqlInfo` | `connection.getInfo()` |
| `getPrimaryKeys/getForeignKeys` | `getObjects` depth=all projections |
| `FlightClient` (low-level) | **removed** in 2.0 (breaking; document in migration notes) |
| OAuth config fields | kept in `FlightSQLClientConfig`, forwarded to `adbc.gizmosql.*` options |

## Acceptance gates

1. Existing unit + integration test suites pass (adapted where they
   tested removed transport internals; behavior-level tests verbatim
   where possible).
2. New integration tests for GizmoSQL semantics through the client:
   DDL/DML without fetch, `RETURNING`, bulk paths used by the UI.
3. `gizmosql-ui` builds and its test suite passes against the local
   2.0 package (`npm pack` + install), before the client publishes.
4. Node 22+ CI matrix.
