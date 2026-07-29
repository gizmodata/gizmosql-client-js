import { randomBytes } from 'node:crypto';
import * as http from 'node:http';
import * as https from 'node:https';
import { Schema, Table } from 'apache-arrow';
import {
  AdbcConnection,
  AdbcDatabase,
  ObjectDepth,
} from '@apache-arrow/adbc-driver-manager';
import { FlightSQLClientConfig, PreparedStatement, SqlInfoValue, TableMetadata } from './types';
import { FlightSQLError } from './errors';
import { validateConfig, toClientError } from './utils';
import { resolveDriverLib } from './driver-lib';

/**
 * A TypeScript/JavaScript client for GizmoSQL.
 *
 * As of 2.0 this client is backed by the native Go GizmoSQL ADBC driver
 * (https://github.com/gizmodata/gizmosql-adbc) loaded through
 * `@apache-arrow/adbc-driver-manager` — the same shared driver library
 * used by Python, Go, C/C++, and R. GizmoSQL semantics (DDL/DML
 * immediate execution under the lazy-execution model, `RETURNING`
 * handling, geometry-preserving ingest, OAuth) live inside the driver.
 *
 * The public API is compatible with the 1.x pure-TypeScript client.
 */
export class FlightSQLClient {
  protected config: FlightSQLClientConfig;
  private db: AdbcDatabase | null = null;
  private conn: AdbcConnection | null = null;
  private prepared = new Map<string, { sql: string }>();

  constructor(config: FlightSQLClientConfig) {
    validateConfig(config);
    this.config = { plaintext: false, ...config };
  }

  /** Builds the gizmosql:// URI for the configured host/port/transport. */
  private uri(): string {
    const transport = this.config.plaintext ? '?transport=tcp' : '';
    return `gizmosql://${this.config.host}:${this.config.port}${transport}`;
  }

  /** Maps the client config onto ADBC database options. */
  private databaseOptions(): Record<string, string> {
    const options: Record<string, string> = { uri: this.uri() };
    if (this.config.tlsSkipVerify) {
      options['adbc.flight.sql.client_option.tls_skip_verify'] = 'true';
    }
    if (this.config.token) {
      options['adbc.flight.sql.authorization_header'] = `Bearer ${this.config.token}`;
    } else if (this.config.username !== undefined && this.config.password !== undefined) {
      options.username = this.config.username;
      options.password = this.config.password;
    }
    return options;
  }

  async connect(): Promise<void> {
    if (this.conn) return;
    try {
      this.db = new AdbcDatabase({
        driver: resolveDriverLib(),
        databaseOptions: this.databaseOptions(),
      });
      this.conn = await this.db.connect();
    } catch (error) {
      await this.close().catch(() => {});
      throw toClientError(error, `Failed to connect to ${this.config.host}:${this.config.port}`);
    }
  }

  private async ensureConn(): Promise<AdbcConnection> {
    if (!this.conn) {
      await this.connect();
    }
    return this.conn!;
  }

  /**
   * Executes a SQL query and returns the Arrow result table.
   * DDL/DML executes immediately on the server (no fetch required) and
   * `INSERT/UPDATE/DELETE ... RETURNING` rows are returned — both
   * handled inside the Go driver.
   */
  async execute(query: string): Promise<Table> {
    const conn = await this.ensureConn();
    try {
      return await conn.query(query);
    } catch (error) {
      throw toClientError(error, 'Failed to execute query', FlightSQLError);
    }
  }

  /** Returns the result schema of a query without materializing rows. */
  async getQuerySchema(query: string): Promise<Schema> {
    const conn = await this.ensureConn();
    try {
      const reader = await conn.queryStream(query);
      const schema = reader.schema;
      if (typeof (reader as { cancel?: () => void }).cancel === 'function') {
        (reader as unknown as { cancel: () => void }).cancel();
      }
      return schema;
    } catch (error) {
      throw toClientError(error, 'Failed to get query schema', FlightSQLError);
    }
  }

  /**
   * Prepares a statement for repeated execution.
   *
   * The returned handle is an opaque client-side identifier (ADBC
   * manages server-side prepared statements internally).
   */
  async prepare(query: string): Promise<PreparedStatement> {
    await this.ensureConn();
    const handle = randomBytes(16);
    this.prepared.set(Buffer.from(handle).toString('hex'), { sql: query });
    return { handle };
  }

  async executePrepared(prepared: PreparedStatement): Promise<any[]> {
    const entry = this.prepared.get(Buffer.from(prepared.handle).toString('hex'));
    if (!entry) {
      throw new FlightSQLError('Unknown prepared statement handle (was it closed?)');
    }
    const table = await this.execute(entry.sql);
    return table.toArray();
  }

  async closePrepared(prepared: PreparedStatement): Promise<void> {
    this.prepared.delete(Buffer.from(prepared.handle).toString('hex'));
  }

  /**
   * Gets SQL metadata information from the server.
   * Returns a Map of SqlInfo ID to value for each requested ID.
   * @param infoIds - Array of SqlInfo IDs to request. If empty, returns all available info.
   */
  async getSqlInfo(infoIds: number[] = []): Promise<Map<number, SqlInfoValue>> {
    const conn = await this.ensureConn();
    try {
      const table = await conn.getInfo(infoIds.length > 0 ? (infoIds as unknown as Parameters<AdbcConnection['getInfo']>[0]) : undefined);
      return this.parseSqlInfoTable(table);
    } catch (error) {
      throw toClientError(error, 'Failed to get SQL info', FlightSQLError);
    }
  }

  /**
   * Parses a SqlInfo response table into a Map of info_name -> value.
   * The table schema is: info_name (uint32), value (dense_union).
   */
  private parseSqlInfoTable(table: Table): Map<number, SqlInfoValue> {
    const result = new Map<number, SqlInfoValue>();
    const infoNameVector = table.getChild('info_name');
    const valueVector = table.getChild('info_value') ?? table.getChild('value');

    if (!infoNameVector || !valueVector) {
      return result;
    }

    for (let i = 0; i < table.numRows; i++) {
      const infoName = Number(infoNameVector.get(i));
      const value = valueVector.get(i);

      if (value === null || value === undefined) {
        result.set(infoName, null);
      } else if (
        typeof value === 'string' ||
        typeof value === 'boolean' ||
        typeof value === 'number' ||
        typeof value === 'bigint'
      ) {
        result.set(infoName, value);
      } else if (Array.isArray(value)) {
        result.set(infoName, value.map(String));
      } else {
        result.set(infoName, String(value));
      }
    }

    return result;
  }

  /** Rows of the ADBC GetObjects hierarchy, materialized to JS objects. */
  private async getObjectRows(options: {
    depth: (typeof ObjectDepth)[keyof typeof ObjectDepth];
    catalog?: string;
    dbSchema?: string;
    tableName?: string;
    tableType?: string[];
  }): Promise<any[]> {
    const conn = await this.ensureConn();
    const table = await conn.getObjects(options);
    return table.toArray().map((row) => (typeof row.toJSON === 'function' ? row.toJSON() : row));
  }

  async getCatalogs(): Promise<string[]> {
    try {
      const rows = await this.getObjectRows({ depth: ObjectDepth.Catalogs });
      return rows.map((row) => row.catalog_name).filter((name) => name != null);
    } catch (error) {
      throw toClientError(error, 'Failed to get catalogs', FlightSQLError);
    }
  }

  async getSchemas(catalog?: string): Promise<Array<{ catalog: string; schema: string }>> {
    try {
      const rows = await this.getObjectRows({ depth: ObjectDepth.Schemas, catalog });
      const out: Array<{ catalog: string; schema: string }> = [];
      for (const row of rows) {
        for (const schema of materialize(row.catalog_db_schemas)) {
          out.push({ catalog: row.catalog_name, schema: schema.db_schema_name });
        }
      }
      return out;
    } catch (error) {
      throw toClientError(error, 'Failed to get schemas', FlightSQLError);
    }
  }

  async getTables(
    catalog?: string,
    dbSchema?: string,
    tableName?: string,
    tableTypes?: string[]
  ): Promise<Array<{ catalog: string; schema: string; tableName: string; tableType: string }>> {
    try {
      const rows = await this.getObjectRows({
        depth: ObjectDepth.Tables,
        catalog,
        dbSchema,
        tableName,
        tableType: tableTypes,
      });
      const out: Array<{ catalog: string; schema: string; tableName: string; tableType: string }> =
        [];
      for (const row of rows) {
        for (const schema of materialize(row.catalog_db_schemas)) {
          for (const tbl of materialize(schema.db_schema_tables)) {
            out.push({
              catalog: row.catalog_name,
              schema: schema.db_schema_name,
              tableName: tbl.table_name,
              tableType: tbl.table_type,
            });
          }
        }
      }
      return out;
    } catch (error) {
      throw toClientError(error, 'Failed to get tables', FlightSQLError);
    }
  }

  async getTableTypes(): Promise<string[]> {
    const conn = await this.ensureConn();
    try {
      const table = await conn.getTableTypes();
      return table.toArray().map((row) => row.table_type);
    } catch (error) {
      throw toClientError(error, 'Failed to get table types', FlightSQLError);
    }
  }

  async getPrimaryKeys(
    catalog: string,
    dbSchema: string,
    tableName: string
  ): Promise<TableMetadata['primaryKeys']> {
    try {
      const rows = await this.getObjectRows({
        depth: ObjectDepth.All,
        catalog,
        dbSchema,
        tableName,
      });
      const out: TableMetadata['primaryKeys'] = [];
      for (const row of rows) {
        for (const schema of materialize(row.catalog_db_schemas)) {
          for (const tbl of materialize(schema.db_schema_tables)) {
            for (const constraint of materialize(tbl.table_constraints)) {
              if (constraint.constraint_type !== 'PRIMARY KEY') continue;
              const columns = materialize(constraint.constraint_column_names);
              for (const [idx, columnName] of columns.entries()) {
                out.push({
                  catalogName: row.catalog_name,
                  schemaName: schema.db_schema_name,
                  tableName: tbl.table_name,
                  columnName: String(columnName),
                  keySequence: idx + 1,
                });
              }
            }
          }
        }
      }
      return out;
    } catch (error) {
      throw toClientError(error, 'Failed to get primary keys', FlightSQLError);
    }
  }

  async getForeignKeys(
    catalog: string,
    dbSchema: string,
    tableName: string
  ): Promise<TableMetadata['foreignKeys']> {
    try {
      const rows = await this.getObjectRows({
        depth: ObjectDepth.All,
        catalog,
        dbSchema,
        tableName,
      });
      const out: TableMetadata['foreignKeys'] = [];
      for (const row of rows) {
        for (const schema of materialize(row.catalog_db_schemas)) {
          for (const tbl of materialize(schema.db_schema_tables)) {
            for (const constraint of materialize(tbl.table_constraints)) {
              if (constraint.constraint_type !== 'FOREIGN KEY') continue;
              const columns = materialize(constraint.constraint_column_names);
              const usage = materialize(constraint.constraint_column_usage);
              for (const [idx, ref] of usage.entries()) {
                out.push({
                  pkCatalogName: ref.fk_catalog ?? ref.catalog ?? '',
                  pkSchemaName: ref.fk_db_schema ?? ref.db_schema ?? '',
                  pkTableName: ref.fk_table ?? ref.table ?? '',
                  pkColumnName: ref.fk_column_name ?? ref.column ?? '',
                  fkCatalogName: row.catalog_name,
                  fkSchemaName: schema.db_schema_name,
                  fkTableName: tbl.table_name,
                  fkColumnName: columns[idx] ?? '',
                });
              }
            }
          }
        }
      }
      return out;
    } catch (error) {
      throw toClientError(error, 'Failed to get foreign keys', FlightSQLError);
    }
  }

  /**
   * Discovers the GizmoSQL OAuth base URL by probing the server's OAuth
   * HTTP endpoint (HTTPS first, then HTTP) — the same discovery the Go
   * and Python drivers perform.
   *
   * @returns The OAuth base URL (e.g., "http://localhost:31339"), or
   *          null if the server does not expose OAuth.
   */
  async discoverOAuthUrl(): Promise<string | null> {
    const port = this.config.oauthPort ?? 31339;
    const host = this.config.host;
    for (const scheme of ['https', 'http'] as const) {
      const base = `${scheme}://${host}:${port}`;
      const ok = await probeOAuthEndpoint(base, this.config.tlsSkipVerify === true);
      if (ok) return base;
    }
    return null;
  }

  async close(): Promise<void> {
    this.prepared.clear();
    const conn = this.conn;
    const db = this.db;
    this.conn = null;
    this.db = null;
    if (conn) {
      await conn.close().catch(() => {});
    }
    if (db) {
      await db.close().catch(() => {});
    }
  }
}

/** Normalizes Arrow nested values (Vectors/StructRows) to plain JS arrays/objects. */
function materialize(value: any): any[] {
  if (value == null) return [];
  if (Array.isArray(value)) return value.map((v) => normalizeRow(v));
  if (typeof value.toArray === 'function') {
    return Array.from(value.toArray()).map((v) => normalizeRow(v));
  }
  return [];
}

function normalizeRow(row: any): any {
  return row && typeof row.toJSON === 'function' ? row.toJSON() : row;
}

/** GET {base}/oauth/initiate and report whether it answered with JSON. */
function probeOAuthEndpoint(base: string, tlsSkipVerify: boolean): Promise<boolean> {
  return new Promise((resolve) => {
    const lib = base.startsWith('https') ? https : http;
    const req = lib.get(
      `${base}/oauth/initiate`,
      base.startsWith('https') ? { rejectUnauthorized: !tlsSkipVerify, timeout: 5000 } : { timeout: 5000 },
      (res) => {
        let body = '';
        res.on('data', (chunk) => (body += chunk));
        res.on('end', () => {
          try {
            const parsed = JSON.parse(body);
            resolve(res.statusCode === 200 && typeof parsed.auth_url === 'string');
          } catch {
            resolve(false);
          }
        });
      }
    );
    req.on('timeout', () => {
      req.destroy();
      resolve(false);
    });
    req.on('error', () => resolve(false));
  });
}
