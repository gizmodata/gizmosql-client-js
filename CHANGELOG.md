# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
