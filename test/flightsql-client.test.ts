import { jest } from '@jest/globals';
import { Table } from 'apache-arrow';
import { FlightSQLClient } from '../src/flightsql-client.js';
import { FlightError, FlightSQLError } from '../src/errors.js';

// Unit tests for the 2.0 ADBC-backed client: config mapping and the
// client-side lifecycle logic. Server-facing behavior is covered by the
// integration suite (test/integration) against a live GizmoSQL server.

// Access private members for mapping assertions without a live driver.
const asAny = (c: FlightSQLClient) => c as any;

describe('FlightSQLClient config mapping', () => {
  it('builds a TLS-by-default gizmosql:// URI', () => {
    const client = new FlightSQLClient({ host: 'db.example.com', port: 31337 });
    expect(asAny(client).uri()).toBe('gizmosql://db.example.com:31337');
  });

  it('appends transport=tcp for plaintext', () => {
    const client = new FlightSQLClient({ host: 'localhost', port: 31337, plaintext: true });
    expect(asAny(client).uri()).toBe('gizmosql://localhost:31337?transport=tcp');
  });

  it('maps username/password to driver options', () => {
    const client = new FlightSQLClient({
      host: 'h', port: 1, username: 'u', password: 'p',
    });
    expect(asAny(client).databaseOptions()).toEqual({
      uri: 'gizmosql://h:1',
      username: 'u',
      password: 'p',
    });
  });

  it('maps token auth to a Bearer authorization header option', () => {
    const client = new FlightSQLClient({ host: 'h', port: 1, token: 'jwt-abc' });
    expect(asAny(client).databaseOptions()).toEqual({
      uri: 'gizmosql://h:1',
      'adbc.flight.sql.authorization_header': 'Bearer jwt-abc',
    });
  });

  it('token takes precedence over username/password', () => {
    const client = new FlightSQLClient({
      host: 'h', port: 1, token: 't', username: 'u', password: 'p',
    });
    const options = asAny(client).databaseOptions();
    expect(options['adbc.flight.sql.authorization_header']).toBe('Bearer t');
    expect(options.username).toBeUndefined();
  });

  it('maps tlsSkipVerify to the Flight SQL client option', () => {
    const client = new FlightSQLClient({ host: 'h', port: 1, tlsSkipVerify: true });
    expect(asAny(client).databaseOptions()['adbc.flight.sql.client_option.tls_skip_verify'])
      .toBe('true');
  });

  it('rejects invalid configs at construction', () => {
    expect(() => new FlightSQLClient({ host: '', port: 31337 })).toThrow(FlightError);
    expect(() => new FlightSQLClient({ host: 'h', port: 0 })).toThrow(FlightError);
  });
});

describe('prepared statement lifecycle (client-side)', () => {
  it('prepare returns an opaque handle and executePrepared runs the SQL', async () => {
    const client = new FlightSQLClient({ host: 'h', port: 1 });
    // Stub out connection + execution — lifecycle logic only.
    asAny(client).ensureConn = jest.fn().mockResolvedValue({});
    const fakeTable = { toArray: () => [{ v: 1 }] };
    client.execute = jest.fn().mockResolvedValue(fakeTable) as any;

    const prepared = await client.prepare('SELECT 1 AS v');
    expect(prepared.handle).toBeInstanceOf(Uint8Array);
    expect(prepared.handle.length).toBeGreaterThan(0);

    const rows = await client.executePrepared(prepared);
    expect(client.execute).toHaveBeenCalledWith('SELECT 1 AS v', undefined);
    expect(rows).toEqual([{ v: 1 }]);
  });

  it('executePrepared forwards parameters to execute', async () => {
    const client = new FlightSQLClient({ host: 'h', port: 1 });
    asAny(client).ensureConn = jest.fn().mockResolvedValue({});
    client.execute = jest.fn().mockResolvedValue({ toArray: () => [] }) as any;

    const prepared = await client.prepare('SELECT * FROM t WHERE id = ?');
    await client.executePrepared(prepared, [7]);
    expect(client.execute).toHaveBeenCalledWith('SELECT * FROM t WHERE id = ?', [7]);
  });

  it('closePrepared invalidates the handle', async () => {
    const client = new FlightSQLClient({ host: 'h', port: 1 });
    asAny(client).ensureConn = jest.fn().mockResolvedValue({});
    client.execute = jest.fn() as any;

    const prepared = await client.prepare('SELECT 1');
    await client.closePrepared(prepared);
    await expect(client.executePrepared(prepared)).rejects.toThrow(FlightSQLError);
  });
});

describe('parameter binding (client-side plumbing)', () => {
  const connWith = (client: FlightSQLClient) => {
    const conn = {
      query: jest.fn().mockResolvedValue({ toArray: () => [] }),
      execute: jest.fn().mockResolvedValue(3),
      queryStream: jest.fn().mockResolvedValue({ open: jest.fn().mockResolvedValue(undefined), schema: { fields: [] } }),
    };
    asAny(client).ensureConn = jest.fn().mockResolvedValue(conn);
    return conn;
  };

  it('execute without params binds nothing', async () => {
    const client = new FlightSQLClient({ host: 'h', port: 1 });
    const conn = connWith(client);
    await client.execute('SELECT 1');
    expect(conn.query).toHaveBeenCalledWith('SELECT 1', undefined);
  });

  it('execute converts params to a one-row Arrow Table', async () => {
    const client = new FlightSQLClient({ host: 'h', port: 1 });
    const conn = connWith(client);
    await client.execute('SELECT * FROM t WHERE id = ? AND name = ?', [7, 'x']);
    const bound = conn.query.mock.calls[0][1] as Table;
    expect(bound).toBeInstanceOf(Table);
    expect(bound.numRows).toBe(1);
    expect(bound.schema.fields.map((f) => f.type.toString())).toEqual(['Int32', 'Utf8']);
  });

  it('executeUpdate returns the affected-row count and binds params', async () => {
    const client = new FlightSQLClient({ host: 'h', port: 1 });
    const conn = connWith(client);
    const affected = await client.executeUpdate('DELETE FROM t WHERE id > ?', [10]);
    expect(affected).toBe(3);
    expect(conn.execute.mock.calls[0][0]).toBe('DELETE FROM t WHERE id > ?');
    expect((conn.execute.mock.calls[0][1] as Table).numRows).toBe(1);
  });

  it('getQuerySchema binds params', async () => {
    const client = new FlightSQLClient({ host: 'h', port: 1 });
    const conn = connWith(client);
    await client.getQuerySchema('SELECT * FROM t WHERE id = ?', [1]);
    expect((conn.queryStream.mock.calls[0][1] as Table).numRows).toBe(1);
  });

  it('rejects unsupported parameter values before touching the driver', async () => {
    const client = new FlightSQLClient({ host: 'h', port: 1 });
    const conn = connWith(client);
    await expect(client.execute('SELECT ?', [{ nested: true } as any])).rejects.toThrow(FlightSQLError);
    expect(conn.query).not.toHaveBeenCalled();
  });

  it('wraps driver errors from executeUpdate as FlightSQLError', async () => {
    const client = new FlightSQLClient({ host: 'h', port: 1 });
    const conn = connWith(client);
    conn.execute.mockRejectedValue(Object.assign(new Error('boom'), { code: 'Internal' }));
    await expect(client.executeUpdate('DELETE FROM t')).rejects.toThrow(/Failed to execute update: boom/);
  });
});
