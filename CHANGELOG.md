# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.0.0] - 2026-07-29

### Changed
- **FlightSQLClient is now backed by the native Go GizmoSQL ADBC driver**
  (loaded via `@apache-arrow/adbc-driver-manager`): `execute()`, prepared
  statements, and all metadata methods (`getCatalogs`/`getSchemas`/
  `getTables`/`getTableTypes`/`getSqlInfo`/`getPrimaryKeys`/
  `getForeignKeys`) run over ADBC, gaining DDL/DML immediate execution,
  `RETURNING` support, `gizmosql://` URIs (TLS by default), and
  geometry-preserving ingest from the shared driver library. The public
  API is unchanged; results are still `apache-arrow` tables.
- `discoverOAuthUrl()` now probes the server's OAuth HTTP endpoint
  (HTTPS then HTTP, `oauthPort` config option, default 31339) — the same
  discovery the Go/Python drivers perform — instead of the gRPC
  handshake header.
- Prepared statements are managed client-side over ADBC (opaque handles;
  `parameterSchema`/`resultSchema` are no longer populated).

### Verified
- `gizmosql-ui` builds and its live call patterns (query service +
  OAuth-discovery route) pass against the packed 2.0 tarball with zero
  application changes.

### Removed
- **The low-level `FlightClient` class, the generated Flight protobufs,
  and the `@grpc/grpc-js` / `google-protobuf` dependencies** — the
  transport layer is the shared Go driver now. (Breaking for 2.0; the
  `FlightSQLClient` surface consumed by gizmosql-ui is unchanged.)

### Added
- **Native driver resolver + postinstall downloader** (2.0 groundwork):
  `resolveDriverLib()` in `src/driver-lib.ts` locates the native Go
  GizmoSQL ADBC driver library — `GIZMOSQL_DRIVER_LIB` env override
  first, then the package-local download cache (`drivers/<version>/`),
  otherwise a clear error with remediation steps (re-run the download,
  set the env var, or build from source). `scripts/download-driver.cjs`
  (npm `postinstall`, Node stdlib only) fetches the platform's
  `libadbc_driver_gizmosql` tarball from the gizmodata/gizmosql-adbc
  GitHub release pinned in `driver-manifest.json`, verifies its SHA-256
  against the manifest, and installs the library atomically; download
  failures warn (with remediation) but never break `npm install`, and
  `GIZMOSQL_DRIVER_SKIP_DOWNLOAD=1` skips it entirely. Supported
  platforms: macOS arm64/x64, Linux x64/arm64, Windows x64/arm64.

### Changed
- **2.0 groundwork**: added `@apache-arrow/adbc-driver-manager` — the
  client is being rebased on the native Go GizmoSQL ADBC driver
  ([gizmodata/gizmosql-adbc](https://github.com/gizmodata/gizmosql-adbc));
  see `docs/go-driver-rewrite-plan.md`. `scripts/spike-adbc.mjs` proves
  the architecture end to end from Node (SELECT round trip, DDL/DML
  immediate execution without fetch, `INSERT ... RETURNING`
  persistence) against a live GizmoSQL server. Node.js engines floor
  raised to >=22 (NAPI driver-manager requirement — breaking for 2.0).

## [1.4.4] - 2026-07-22

### Fixed
- **Prepared statements were completely broken** ([#1](https://github.com/gizmodata/gizmosql-client-js/issues/1)). Three bugs, found via smoke-testing against GizmoSQL server v1.34.0:
  - `prepare()` and `closePrepared()` built a `FlightDescriptor` instead of a Flight `Action` (and never attached the request payload), failing client-side with `Expected argument of type arrow.flight.protocol.Action` before any bytes reached the server. They now send a proper `Action` (`CreatePreparedStatement` / `ClosePreparedStatement`) with the `Any`-packed request as the body, and the `as any` casts that hid the type error are gone.
  - `prepare()` treated the raw DoAction result body as the statement handle; per the Flight SQL spec it is a `google.protobuf.Any` wrapping an `ActionCreatePreparedStatementResult`. The result is now unpacked properly, and `parameterSchema`/`resultSchema` are populated with the schemas' IPC bytes from the server (previously always `undefined`).
  - `executePrepared()` sent the `CommandPreparedStatementQuery` without the `Any` wrapper (unlike every other command), which servers reject as an invalid request. It now uses the same `createCommandDescriptor` path as `execute()`.

### Changed
- Integration CI: the CloseSession log assertion now resolves the server container dynamically (GitHub Actions service containers have generated names), fixing the Test workflow that had been failing on main since March
- CI workflows: bumped `actions/checkout` and `actions/setup-node` to v5 (Node 24 action runtime)
- Refreshed dependencies within semver ranges (apache-arrow 21.2.0, @grpc/grpc-js 1.14.4, google-protobuf 4.0.2, jest 30.4.2, et al)

## [1.4.3] - 2026-03-11

### Fixed
- `parseErrorFromGrpc` now uses the gRPC `details` field (the server's actual error message) instead of hardcoded generic messages like "Authentication failed" or "Service unavailable"
- `FlightClient.connect()` now preserves specific error types (e.g., `AuthenticationError`) instead of wrapping them in a generic `ConnectionError` that hides the detail
- All `FlightSQLClient` methods (`getCatalogs`, `getSchemas`, `getTables`, `execute`, etc.) now include the underlying error detail in their error messages instead of just the error class name

## [1.4.2] - 2026-03-10

### Fixed
- Fix `close()` sending CloseSession to wrong session in bundled environments (e.g., `@yao-pkg/pkg`). The `doAction()` wrapper's auto-reconnect logic could create a new session instead of closing the existing one. Now calls the gRPC client directly.
- Log CloseSession RPC failures with `console.warn` instead of silently swallowing all errors.

## [1.4.1] - 2026-03-10

### Fixed
- Always send `CloseSession` RPC when closing the client connection. Previously, `close()` only closed the gRPC channel without notifying the server, leaving server-side sessions open indefinitely.

## [1.4.0] - 2026-02-11

### Added
- `getSqlInfo()` method on `FlightSQLClient` for querying Flight SQL metadata (server name, capabilities, custom GizmoSQL instrumentation info)
- `SqlInfoValue` type and `GIZMOSQL_SQL_INFO` constants for instrumentation metadata discovery (IDs 10000-10002)

## [1.3.0] - 2026-02-11

### Added
- `discoverOAuthUrl()` method on `FlightSQLClient` for OAuth/SSO URL discovery via Flight handshake protocol
- `CLAUDE.md` with project guidelines
- `CHANGELOG.md`
- OAuth/SSO documentation in README

## [1.2.10] - 2025-12-15

Initial release as `@gizmodata/gizmosql-client`.

### Features
- Full Apache Arrow Flight SQL protocol support
- TLS with certificate verification skip option
- Basic authentication (username/password)
- Bearer token authentication
- Query execution with Apache Arrow table results
- Database metadata operations (catalogs, schemas, tables)
- Prepared statements support
