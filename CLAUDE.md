# Claude Code Guidelines for @gizmodata/gizmosql-client

## Pre-Commit Checklist

Before committing any change, ensure you have completed ALL of the following:

### 1. Changelog
- [ ] **ALWAYS** update `CHANGELOG.md` for every user-facing change
- [ ] Add entries under `## [Unreleased]` section
- [ ] Follow [Keep a Changelog](https://keepachangelog.com/) format:
  - `### Added` - new features
  - `### Changed` - changes in existing functionality
  - `### Deprecated` - soon-to-be removed features
  - `### Removed` - removed features
  - `### Fixed` - bug fixes
  - `### Security` - vulnerability fixes

### 2. Documentation
- [ ] **ALWAYS** update `README.md` for every relevant change (new features, API changes, new options, etc.)
- [ ] Update JSDoc comments on public methods and interfaces
- [ ] Add usage examples for new features

### 3. Tests
- [ ] **ALWAYS** create or update tests for every code change
- [ ] Add unit tests in `test/` for new methods and logic
- [ ] Add integration tests for new server-facing functionality
- [ ] Run `npm test` for unit tests
- [ ] Run `npm run test:integration` for integration tests (requires a running GizmoSQL server)

### 4. Lint & Typecheck
- [ ] Run `npm run lint` to fix lint issues
- [ ] Run `npm run typecheck` to verify types

## Build Commands

```bash
# Build
npm run build

# Run unit tests
npm test

# Run integration tests (requires running GizmoSQL server)
npm run test:integration

# Lint
npm run lint

# Type check
npm run typecheck
```

## Project Structure

- `src/` - TypeScript source
  - `flight-client.ts` - Low-level Arrow Flight gRPC client
  - `flight-sql-client.ts` - High-level Flight SQL client (extends FlightClient)
  - `types.ts` - Type definitions
  - `errors.ts` - Error classes
  - `utils.ts` - Utility functions
  - `generated/` - Protobuf-generated code (do not edit manually)
- `proto/` - Protobuf definitions
- `test/` - Tests
- `dist/` - Build output (not committed)

## Key Patterns

- `FlightSQLClient` extends `FlightClient` - high-level SQL operations built on low-level Flight RPC
- All gRPC calls use `@grpc/grpc-js` (pure JS, no native addons)
- Protobuf code is generated via `npm run proto:generate`
- OAuth/SSO discovery uses the Flight `Handshake` RPC with `username="__discover__"` via Basic Auth; server returns the OAuth URL in the `x-gizmosql-oauth-url` response header
