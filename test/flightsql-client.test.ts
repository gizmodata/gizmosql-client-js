import { FlightSQLClient } from '../src/flightsql-client';
import { FlightError, FlightSQLError } from '../src/errors';

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
    expect(client.execute).toHaveBeenCalledWith('SELECT 1 AS v');
    expect(rows).toEqual([{ v: 1 }]);
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
