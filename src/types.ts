import type { Table } from 'apache-arrow';

export interface FlightClientConfig {
  host: string;
  port: number;
  plaintext?: boolean;
  tlsSkipVerify?: boolean;
  username?: string;
  password?: string;
  token?: string;
  /** OAuth HTTP port probed by discoverOAuthUrl() (default 31339). */
  oauthPort?: number;
}

export type FlightSQLClientConfig = FlightClientConfig;

/**
 * A single query parameter value. Mapped to Arrow types as follows:
 * `string` → Utf8, integer `number` → Int32/Int64 (by range), other
 * `number` → Float64, `bigint` → Int64, `boolean` → Bool, `Date` →
 * Timestamp (millisecond, UTC), `Uint8Array`/`Buffer` → Binary,
 * `null`/`undefined` → Null.
 */
export type SqlParameterValue =
  | string
  | number
  | bigint
  | boolean
  | Date
  | Uint8Array
  | null
  | undefined;

/**
 * Parameters for a query with `?` (or `$1`, `$2`, ...) placeholders:
 * either an array of values, one per placeholder in order, or a
 * pre-built one-row Arrow `Table` (one column per placeholder) for full
 * control over the Arrow types sent to the server.
 */
export type SqlParameters = readonly SqlParameterValue[] | Table;

export interface PreparedStatement {
  handle: Uint8Array;
  parameterSchema?: any;
  resultSchema?: any;
}

export interface FlightInfo {
  endpoint: string;
  ticket: Uint8Array;
  totalRecords: number;
  totalBytes: number;
}

export interface DatabaseMetadata {
  catalogs: string[];
  schemas: Array<{ catalog: string; schema: string }>;
  tables: Array<{
    catalog: string;
    schema: string;
    tableName: string;
    tableType: string;
  }>;
}

export type SqlInfoValue = string | boolean | number | bigint | string[] | null;

export const GIZMOSQL_SQL_INFO = {
  INSTRUMENTATION_ENABLED: 10000,
  INSTRUMENTATION_CATALOG: 10001,
  INSTRUMENTATION_SCHEMA: 10002,
} as const;

export interface TableMetadata {
  primaryKeys: Array<{
    catalogName: string;
    schemaName: string;
    tableName: string;
    columnName: string;
    keySequence: number;
  }>;
  foreignKeys: Array<{
    pkCatalogName: string;
    pkSchemaName: string;
    pkTableName: string;
    pkColumnName: string;
    fkCatalogName: string;
    fkSchemaName: string;
    fkTableName: string;
    fkColumnName: string;
  }>;
}