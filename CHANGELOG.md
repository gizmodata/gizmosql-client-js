# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.2.0] - 2026-09-03

### Added
- `executeStream(query, params?)`: returns a `QueryStream` — an async
  iterable of Arrow `RecordBatch`es with the result `schema`, `cancel()`,
  `done`, and `toTable()`. Batches are pulled lazily; breaking out of the
  loop (or `cancel()`) releases the server-side stream and the client
  stays usable. `QueryStream` is exported.
- `adbcOptions` connection option: extra ADBC database options passed to
  the native driver after (and overriding) the derived ones — e.g. call
  headers, custom root certificates, or `adbc.gizmosql.auth_type`.
- **Query cancellation.** `execute()`, `executeStream()` and
  `executeUpdate()` accept `{ signal: AbortSignal }` (`ExecuteOptions`).
  For queries, aborting while the server is executing closes the ADBC
  statement, which the Go driver relays as a Flight SQL cancel and
  GizmoSQL >= 1.38.0 turns into a DuckDB interrupt; aborting while
  fetching releases the result stream. The call rejects with the new
  `QueryCancelledError` (carries the abort `reason`).
  `AbortSignal.timeout(ms)` gives a client-side deadline. `executeUpdate()`
  honors an already-aborted signal; an abort during a running DML/DDL
  statement cannot reach the driver through the Node.js ADBC driver
  manager (0.24 releases the statement only after the update returns), so
  the update completes — bound DML/DDL with `SET gizmosql.query_timeout`.
- `oauthPort` documented in the README connection options.
- Integration tests for cancellation during execution and fetch,
  `AbortSignal.timeout`, `SET gizmosql.query_timeout`, and the server
  interrupting statements of killed clients (asserted via server logs).

### Changed
- `execute()` / `executeUpdate()` now run on an explicitly managed ADBC
  statement (needed for cancellation) instead of the driver manager's
  `conn.query()` / `conn.execute()`; results and errors are unchanged.
- Bundled native driver bumped to `gizmosql-adbc` v2.0.12 (was v2.0.10);
  hashes refreshed in `driver-manifest.json`. 2.0.12 cancels an in-flight
  update when the statement is released.

## [2.1.0] - 2026-09-02

### Added
- **Parameter binding.** `execute()`, `getQuerySchema()` and
  `executePrepared()` accept a second `params` argument for `?` (or
  `$1`, `$2`, ...) placeholders — an array of JS values (`string`,
  `number`, `bigint`, `boolean`, `Date`, `Uint8Array`/`Buffer`, `null`)
  or a pre-built one-row Arrow `Table`. Values are sent to the server as
  typed Arrow data via ADBC prepared statements; no SQL string
  interpolation. The `parametersToTable()` helper and the
  `SqlParameterValue` / `SqlParameters` types are exported.
- `executeUpdate(query, params?)`: runs INSERT/UPDATE/DELETE/DDL and
  returns the affected-row count.
- `scripts/pin-driver.mjs <version>`: re-pins `driver-manifest.json` to a
  gizmosql-adbc release (downloads the six platform tarballs and records
  their SHA-256s).
- Integration tests can target an existing server via
  `GIZMOSQL_TEST_EXTERNAL=1` plus `GIZMOSQL_TEST_HOST` / `_PORT` /
  `_USERNAME` / `_PASSWORD` / `_PLAINTEXT=1`, instead of starting their
  own container.

### Changed
- **The package is now published as ES modules** (`"type": "module"`).
  `import { FlightSQLClient } from '@gizmodata/gizmosql-client'` is
  unchanged; CommonJS callers can `require()` it on Node >= 22.12 (the
  same floor the ESM-only `@apache-arrow/adbc-driver-manager` already
  imposes). This fixes parameter binding from the compiled CommonJS
  build: it loaded the CommonJS copy of `apache-arrow` while the driver
  manager loaded the ESM copy, and a parameter `Table` built by one copy
  was mis-serialized by the other (malformed IPC, and in one case a
  native-addon abort). One `apache-arrow` instance is now shared end to
  end.
- Native driver pinned to
  [gizmosql-adbc v2.0.10](https://github.com/gizmodata/gizmosql-adbc/releases/tag/v2.0.10)
  (was 2.0.8): required for parameter binding (the driver now prepares
  automatically on bind), plus server-side query cancellation and the
  geometry-ingest fix for GizmoSQL >= 1.37.
- Dev dependencies: ESLint 10 (`@eslint/js` 10, `eslint-plugin-unicorn`
  74, `typescript-eslint` 8.69, `eslint-plugin-jest` 29.16), Jest 30.5,
  ts-jest 29.4.12. TypeScript stays on 5.9 until ts-jest and
  typescript-eslint support 7.x.

### Verified
- gizmosql-ui (Next.js 16) builds against the packed ESM client with no
  application changes, and its live connect/query/metadata/OAuth-discovery
  routes work through `next start` against a GizmoSQL container.

### Removed
- Leftover gRPC-era dev dependencies (`grpc-tools`,
  `grpc_tools_node_protoc_ts`, `@types/google-protobuf`) that 2.0 no
  longer uses.

### Fixed
- `getQuerySchema()` returned `undefined`: the Arrow stream reader's
  schema is only populated after the stream is opened.
- Integration suite: a stopped `gizmosql-test` container left over from
  an earlier run no longer shadows the live server in the session-close
  log assertion.

## [2.0.1] - 2026-08-24

### Changed
- **Bundled native driver bumped to `gizmosql-adbc` v2.0.8** (was v2.0.1) —
  the postinstall download now fetches the v2.0.8 release assets (hashes
  refreshed in `driver-manifest.json`). v2.0.8 fixes geometry-aware bulk
  ingest against GizmoSQL ≥ 1.37.0, which creates `GEOMETRY` columns
  server-side; earlier driver builds failed there with
  `No function matches 'st_geomfromwkb(GEOMETRY)'`.
- CI: bumped `actions/checkout` and `actions/setup-node` to v7 and
  `softprops/action-gh-release` to v3 (retiring Node 20-era action
  majors).

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
