# Work plan — @gizmodata/gizmosql-client 2.0

Checklist for the (partly autonomous) rewrite. Worked in order; each
completed item is checked off, committed, and pushed with green tests.
See `docs/go-driver-rewrite-plan.md` for design.

## Ground rules (for autonomous iterations)

- Every push builds (`npm run build`), lints (`npm run lint`),
  typechecks, and passes `npm test`; integration tests run against a
  live GizmoSQL server (the `gizmosql` PyPI package can launch one, or
  Docker). No red pushes to `main`.
- Public API compatibility for everything `gizmosql-ui` imports
  (`FlightSQLClient`) is the hard constraint; consult
  /Users/philip/LocalOnly/git/gizmosql-ui for actual call sites.
- PRE-EXISTING LOCAL STATE: the repo has an uncommitted CHANGELOG.md
  modification from Philip — inspect it first, preserve its intent
  (fold it into the [Unreleased] section if it's an in-progress note),
  and mention it in the next report.
- Update CHANGELOG.md (`[Unreleased]`) with every functional change.
- **Do not publish to npm and do not tag releases** — Philip cuts
  releases. Local `npm pack` is fine for gizmosql-ui verification.
- When blocked, record the blocker at the bottom and move on.

## Phase 1 — Scaffold + spike

- [x] Add `@apache-arrow/adbc-driver-manager` dependency (0.24.0);
      engines raised to Node >=22; `scripts/spike-adbc.mjs` proves
      SELECT / DDL-DML-without-fetch / RETURNING against a live server
      via the local Go driver build (key API learning: driver options
      go under `databaseOptions`, and AdbcConnection exposes query/
      queryStream/ingest/getObjects/getInfo/prepared statements —
      everything FlightSQLClient needs)
- [ ] Driver library resolver (`src/driver-lib.ts`): env override →
      packaged/cache path → clear error; postinstall downloader with
      SHA-256 verification against the gizmosql-adbc v2.0.0 release
      assets

## Phase 2 — FlightSQLClient on ADBC

- [ ] Config mapping: FlightSQLClientConfig → AdbcDatabase options
      (host/port/tls → `gizmosql://` URI, credentials, OAuth fields →
      `adbc.gizmosql.*`)
- [ ] `execute()` and prepared-statement methods on ADBC
- [ ] Metadata methods (`getCatalogs`, `getSchemas`, `getTables`,
      `getTableTypes`, `getSqlInfo`, `getPrimaryKeys`,
      `getForeignKeys`) on ADBC metadata APIs
- [ ] Remove `flight-client.ts`, generated protos, grpc-js and
      google-protobuf deps; errors.ts mapped to ADBC error surface
- [ ] Unit tests adapted; integration tests green

## Phase 3 — GizmoSQL-semantics tests + docs

- [ ] Integration tests: DDL/DML immediate execution (no fetch),
      RETURNING, gizmosql:// URI, OAuth option passthrough (mock-level)
- [ ] README rewrite: 2.0 architecture (Go driver), Node 22+ note,
      migration notes (FlightClient removed, OAuth now in-driver)
- [ ] CHANGELOG [Unreleased] complete enough to be the 2.0.0 release
      notes

## Phase 4 — gizmosql-ui verification

- [ ] `npm pack` the client; install the tarball into
      /Users/philip/LocalOnly/git/gizmosql-ui (in a throwaway copy or
      via npm overrides — do NOT commit changes to gizmosql-ui);
      `npm run build` + its test suite green; note any API friction
- [ ] Record UI findings + any needed client fixes

## Philip-gated (do not do autonomously)

- [ ] npm publish of @gizmodata/gizmosql-client 2.0.0 (+ tag/release)
- [ ] Optional fast-follow: per-platform @gizmodata/gizmosql-driver-*
      npm packages (needs npm token in gizmosql-adbc CI)
- [ ] gizmosql-ui dependency bump + release once the client publishes

## Blockers / notes

(none yet)
