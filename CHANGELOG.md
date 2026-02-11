# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
