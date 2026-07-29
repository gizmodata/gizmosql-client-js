# GizmoSQL Client for JavaScript/TypeScript

[![GitHub](https://img.shields.io/badge/GitHub-gizmodata%2Fgizmosql--client--js-blue.svg?logo=Github)](https://github.com/gizmodata/gizmosql-client-js)
[![npm](https://img.shields.io/badge/npm-@gizmodata%2Fgizmosql--client-red.svg?logo=npm)](https://www.npmjs.com/package/@gizmodata/gizmosql-client)

A TypeScript/JavaScript client for [GizmoSQL](https://github.com/gizmodata/gizmosql) and Apache Arrow Flight SQL servers.

## Features

> **New in 2.0:** the client is powered by the
> [native Go GizmoSQL ADBC driver](https://github.com/gizmodata/gizmosql-adbc)
> (downloaded automatically at install time) via
> [`@apache-arrow/adbc-driver-manager`](https://www.npmjs.com/package/@apache-arrow/adbc-driver-manager)
> — the same shared driver library used by Python, Go, C/C++, and R.
> You get GizmoSQL's DDL/DML immediate execution (no fetch required),
> `INSERT/UPDATE/DELETE ... RETURNING`, `gizmosql://` TLS-by-default
> transport, and geometry-preserving bulk ingest, all with the same
> public API as 1.x. See **Migrating to 2.0** below.


- Full support for Apache Arrow Flight SQL protocol
- TLS with certificate verification skip option for self-signed certificates
- Basic authentication (username/password)
- Bearer token authentication
- OAuth/SSO URL discovery via Flight handshake
- Query execution with Apache Arrow table results
- Database metadata operations (catalogs, schemas, tables)
- Prepared statements support

## Installation

```bash
npm install @gizmodata/gizmosql-client
```

## Quick Start

### Connecting to GizmoSQL with TLS

```typescript
import { FlightSQLClient } from "@gizmodata/gizmosql-client";

const client = new FlightSQLClient({
  host: "localhost",
  port: 31337,
  tlsSkipVerify: true,  // Skip certificate verification for self-signed certs
  username: "gizmosql",
  password: "your-password",
});

// Execute a query - returns an Apache Arrow Table
const table = await client.execute("SELECT * FROM my_table LIMIT 10");

// Convert to array of row objects
console.log(table.toArray());
// Output: [ { id: 1, name: "Alice" }, { id: 2, name: "Bob" }, ... ]

await client.close();
```

### Connection Options

```typescript
interface FlightClientConfig {
  host: string;           // Server hostname
  port: number;           // Server port (default: 31337 for GizmoSQL)
  plaintext?: boolean;    // Use unencrypted connection (default: false)
  tlsSkipVerify?: boolean; // Skip TLS certificate verification (default: false)
  username?: string;      // Username for basic auth
  password?: string;      // Password for basic auth
  token?: string;         // Bearer token for token auth
}
```

### Using Bearer Token Authentication

```typescript
const client = new FlightSQLClient({
  host: "localhost",
  port: 31337,
  tlsSkipVerify: true,
  token: "your-bearer-token",
});
```

### OAuth/SSO URL Discovery

If the GizmoSQL server has OAuth/SSO configured, you can discover the OAuth base URL (the client probes the server's OAuth HTTP endpoint over HTTPS, then HTTP — set `oauthPort` in the config if the server uses a non-default port; default 31339):

```typescript
import { FlightSQLClient } from "@gizmodata/gizmosql-client";

const client = new FlightSQLClient({
  host: "localhost",
  port: 31337,
  tlsSkipVerify: true,
});

// Discover the OAuth URL (no credentials needed)
const oauthUrl = await client.discoverOAuthUrl();

if (oauthUrl) {
  console.log(`OAuth server URL: ${oauthUrl}`);
  // Use oauthUrl to initiate OAuth flow:
  //   GET ${oauthUrl}/oauth/initiate → { session_uuid, auth_url }
  //   Direct user to auth_url for IdP login
  //   Poll GET ${oauthUrl}/oauth/token/${session_uuid} for the token
  //   Connect with username="token", password=<identity_token>
} else {
  console.log("Server does not have OAuth configured");
}

await client.close();
```

### Connecting with an OAuth/SSO Token

After completing the OAuth flow, connect using the identity token via Basic Auth:

```typescript
const client = new FlightSQLClient({
  host: "localhost",
  port: 31337,
  tlsSkipVerify: true,
  username: "token",
  password: identityToken,  // JWT from the OAuth flow
});

const table = await client.execute("SELECT * FROM my_table LIMIT 10");
```

### Plaintext Connection (Development Only)

```typescript
const client = new FlightSQLClient({
  host: "localhost",
  port: 31337,
  plaintext: true,  // No TLS encryption
  username: "gizmosql",
  password: "your-password",
});
```

## Starting a GizmoSQL Server

To use this client, you need a running GizmoSQL server. The easiest way is via Docker:

```bash
docker run --name gizmosql \
  --detach --tty --init \
  --publish 31337:31337 \
  --env TLS_ENABLED="1" \
  --env GIZMOSQL_USERNAME="gizmosql" \
  --env GIZMOSQL_PASSWORD="your-password" \
  gizmodata/gizmosql:latest
```

For more options and configuration, see the [GizmoSQL repository](https://github.com/gizmodata/gizmosql).

### Mounting Your Own Database

```bash
docker run --name gizmosql \
  --detach --tty --init \
  --publish 31337:31337 \
  --mount type=bind,source=$(pwd)/data,target=/opt/gizmosql/data \
  --env TLS_ENABLED="1" \
  --env GIZMOSQL_USERNAME="gizmosql" \
  --env GIZMOSQL_PASSWORD="your-password" \
  --env DATABASE_FILENAME="data/mydb.duckdb" \
  gizmodata/gizmosql:latest
```

## API Reference

### Query Execution

```typescript
// Execute a SQL query
const table = await client.execute("SELECT * FROM users WHERE active = true");

// Get results as array
const rows = table.toArray();
```

### Database Metadata

```typescript
// Get all catalogs
const catalogs = await client.getCatalogs();

// Get schemas (optionally filtered by catalog)
const schemas = await client.getSchemas("my_catalog");

// Get tables (with optional filters)
const tables = await client.getTables(
  "my_catalog",    // catalog (optional)
  "my_schema",     // schema pattern (optional)
  "my_table%",     // table name pattern (optional)
  ["TABLE", "VIEW"] // table types (optional)
);

// Get table types
const tableTypes = await client.getTableTypes();
```

### OAuth/SSO Discovery

```typescript
// Discover OAuth URL from server (no credentials required)
const oauthUrl = await client.discoverOAuthUrl();
// Returns the OAuth base URL (e.g., "http://localhost:31339"), or null
// if the server does not have OAuth configured.
```

### Prepared Statements

```typescript
// Prepare a statement
const prepared = await client.prepare("SELECT * FROM users WHERE id = ?");

// Execute the prepared statement
const results = await client.executePrepared(prepared);

// Close the prepared statement
await client.closePrepared(prepared);
```

## Working with Arrow Tables

The `execute()` method returns an Apache Arrow `Table` object. See the [Apache Arrow JS documentation](https://arrow.apache.org/docs/js/) for full details.

```typescript
const table = await client.execute("SELECT id, name, score FROM players");

// Get column by name
const names = table.getChild("name");

// Iterate over rows
for (const row of table) {
  console.log(row.id, row.name, row.score);
}

// Convert to array of objects
const rows = table.toArray();

// Get schema information
console.log(table.schema.fields);
```

## Dependencies

- [`@apache-arrow/adbc-driver-manager`](https://www.npmjs.com/package/@apache-arrow/adbc-driver-manager) — loads the native GizmoSQL driver library
- [`apache-arrow`](https://www.npmjs.com/package/apache-arrow) — Arrow tables/schemas for results

The native driver library (`libadbc_driver_gizmosql`) is fetched at
install time from the pinned
[gizmosql-adbc release](https://github.com/gizmodata/gizmosql-adbc/releases)
and verified by SHA-256. Offline/airgapped installs can set
`GIZMOSQL_DRIVER_SKIP_DOWNLOAD=1` and point `GIZMOSQL_DRIVER_LIB` at a
locally built library (`make -C gizmosql-adbc/go lib`).

## Migrating to 2.0

2.0 keeps the `FlightSQLClient` API compatible with 1.x. Breaking
changes are at the edges:

- **Node.js >= 22** is required (native ADBC driver-manager addon).
- **`FlightClient` (the low-level Flight RPC class) is removed**, along
  with the generated protobufs and the `@grpc/grpc-js` dependency. If
  you used raw Flight RPCs, use the ADBC APIs or the Go driver directly.
- `discoverOAuthUrl()` probes the server's OAuth HTTP endpoint
  (configurable via the new `oauthPort` option, default 31339) instead
  of the gRPC handshake header — same result for standard deployments.
- `PreparedStatement.handle` is now an opaque client-side identifier and
  `parameterSchema`/`resultSchema` are no longer populated (ADBC manages
  server-side prepared state internally).

## Requirements

- Node.js >= 22

## License

Apache License 2.0

## Acknowledgements

This project is a fork of [flight-sql-client-js](https://github.com/firetiger-oss/flight-sql-client-js) originally developed by [Firetiger Inc](https://github.com/firetiger-oss). We thank them for their foundational work on this client.

## Links

- [GizmoSQL](https://github.com/gizmodata/gizmosql) - High-performance SQL server with Arrow Flight SQL
- [GizmoData](https://gizmodata.com) - Company behind GizmoSQL
- [Apache Arrow](https://arrow.apache.org/) - In-memory columnar data format
- [Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html) - SQL protocol specification
