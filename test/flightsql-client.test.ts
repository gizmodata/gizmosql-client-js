import { jest } from '@jest/globals';
import { Int32, RecordBatchReader, Table, Utf8, tableToIPC, vectorFromArray } from 'apache-arrow';
import { FlightSQLClient, QueryStream } from '../src/flightsql-client.js';
import { FlightError, FlightSQLError, QueryCancelledError } from '../src/errors.js';

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
  const emptyReader = async () => {
    const empty = new Table({ v: vectorFromArray([], new Int32()) });
    return RecordBatchReader.from((async function* () { yield tableToIPC(empty, 'stream'); })());
  };
  const connWith = (client: FlightSQLClient) => {
    const stmt = {
      setSqlQuery: jest.fn(async () => {}),
      bind: jest.fn(async () => {}),
      executeQuery: jest.fn(async () => emptyReader()),
      executeUpdate: jest.fn(async () => 3),
      close: jest.fn(async () => {}),
    };
    const conn = {
      createStatement: jest.fn(async () => stmt),
      queryStream: jest.fn().mockResolvedValue({ open: jest.fn().mockResolvedValue(undefined), schema: { fields: [] }, cancel: jest.fn() }),
    };
    asAny(client).ensureConn = jest.fn().mockResolvedValue(conn);
    return { conn, stmt };
  };

  it('execute without params binds nothing', async () => {
    const client = new FlightSQLClient({ host: 'h', port: 1 });
    const { stmt } = connWith(client);
    await client.execute('SELECT 1');
    expect(stmt.setSqlQuery).toHaveBeenCalledWith('SELECT 1');
    expect(stmt.bind).not.toHaveBeenCalled();
    expect(stmt.close).toHaveBeenCalledTimes(1);
  });

  it('execute converts params to a one-row Arrow Table', async () => {
    const client = new FlightSQLClient({ host: 'h', port: 1 });
    const { stmt } = connWith(client);
    await client.execute('SELECT * FROM t WHERE id = ? AND name = ?', [7, 'x']);
    const bound = (stmt.bind.mock.calls[0] as unknown[])[0] as Table;
    expect(bound).toBeInstanceOf(Table);
    expect(bound.numRows).toBe(1);
    expect(bound.schema.fields.map((f) => f.type.toString())).toEqual(['Int32', 'Utf8']);
  });

  it('executeUpdate returns the affected-row count and binds params', async () => {
    const client = new FlightSQLClient({ host: 'h', port: 1 });
    const { stmt } = connWith(client);
    const affected = await client.executeUpdate('DELETE FROM t WHERE id > ?', [10]);
    expect(affected).toBe(3);
    expect(stmt.setSqlQuery).toHaveBeenCalledWith('DELETE FROM t WHERE id > ?');
    expect(((stmt.bind.mock.calls[0] as unknown[])[0] as Table).numRows).toBe(1);
    expect(stmt.close).toHaveBeenCalledTimes(1);
  });

  it('getQuerySchema binds params', async () => {
    const client = new FlightSQLClient({ host: 'h', port: 1 });
    const { conn } = connWith(client);
    await client.getQuerySchema('SELECT * FROM t WHERE id = ?', [1]);
    expect((conn.queryStream.mock.calls[0][1] as Table).numRows).toBe(1);
  });

  it('rejects unsupported parameter values before touching the driver', async () => {
    const client = new FlightSQLClient({ host: 'h', port: 1 });
    const { conn } = connWith(client);
    await expect(client.execute('SELECT ?', [{ nested: true } as any])).rejects.toThrow(FlightSQLError);
    expect(conn.createStatement).not.toHaveBeenCalled();
  });

  it('wraps driver errors from executeUpdate as FlightSQLError and releases the statement', async () => {
    const client = new FlightSQLClient({ host: 'h', port: 1 });
    const { stmt } = connWith(client);
    stmt.executeUpdate.mockRejectedValue(Object.assign(new Error('boom'), { code: 'Internal' }));
    await expect(client.executeUpdate('DELETE FROM t')).rejects.toThrow(/Failed to execute update: boom/);
    expect(stmt.close).toHaveBeenCalledTimes(1);
  });
});

describe('adbcOptions passthrough', () => {
  it('merges caller-supplied driver options after the derived ones', () => {
    const client = new FlightSQLClient({
      host: 'h', port: 1, username: 'u', password: 'p',
      adbcOptions: { 'adbc.flight.sql.rpc.call_header.x-trace': 'abc' },
    });
    expect(asAny(client).databaseOptions()).toEqual({
      uri: 'gizmosql://h:1',
      username: 'u',
      password: 'p',
      'adbc.flight.sql.rpc.call_header.x-trace': 'abc',
    });
  });

  it('lets adbcOptions override derived options', () => {
    const client = new FlightSQLClient({
      host: 'h', port: 1, tlsSkipVerify: true,
      adbcOptions: { 'adbc.flight.sql.client_option.tls_skip_verify': 'false' },
    });
    expect(asAny(client).databaseOptions()['adbc.flight.sql.client_option.tls_skip_verify'])
      .toBe('false');
  });
});

describe('executeStream / execute / QueryStream (fake ADBC statement)', () => {
  const makeTable = (n: number) =>
    new Table({
      id: vectorFromArray(Array.from({ length: n }, (_, i) => i), new Int32()),
      name: vectorFromArray(Array.from({ length: n }, (_, i) => `row-${i}`), new Utf8()),
    });

  // An async reader over the IPC bytes of `table` — the same shape the
  // driver manager returns (AsyncRecordBatchStreamReader).
  const readerFor = async (table: Table, cancelSpy?: () => void) => {
    const bytes = tableToIPC(table, 'stream');
    const reader = await RecordBatchReader.from((async function* () { yield bytes; })());
    if (cancelSpy) {
      const original = reader.cancel.bind(reader);
      reader.cancel = (async () => {
        cancelSpy();
        await original();
      }) as typeof reader.cancel;
    }
    return reader;
  };

  interface FakeHooks {
    cancelSpy?: () => void;
    /** When set, executeQuery never resolves until close() rejects it. */
    hang?: boolean;
    affected?: number;
  }

  // A fake ADBC connection whose statements serve `table`.
  const fakeConn = (table: Table, hooks: FakeHooks = {}) => {
    let rejectPending: ((e: unknown) => void) | undefined;
    const stmt = {
      setSqlQuery: jest.fn(async () => {}),
      bind: jest.fn(async () => {}),
      executeQuery: jest.fn(async () => {
        if (hooks.hang) {
          // eslint-disable-next-line unicorn/prefer-promise-with-resolvers -- lib is ES2022
          return new Promise<never>((_, reject) => { rejectPending = reject; });
        }
        return readerFor(table, hooks.cancelSpy);
      }),
      executeUpdate: jest.fn(async () => {
        if (hooks.hang) {
          // eslint-disable-next-line unicorn/prefer-promise-with-resolvers -- lib is ES2022
          return new Promise<never>((_, reject) => { rejectPending = reject; });
        }
        return hooks.affected ?? 0;
      }),
      close: jest.fn(async () => {
        // Closing a statement mid-execution makes the driver reject the call.
        rejectPending?.(Object.assign(new Error('context canceled'), { code: 'Cancelled' }));
        rejectPending = undefined;
      }),
    };
    return { stmt, createStatement: jest.fn(async () => stmt) };
  };

  const clientWith = (conn: ReturnType<typeof fakeConn>) => {
    const client = new FlightSQLClient({ host: 'h', port: 1 });
    asAny(client).ensureConn = jest.fn().mockResolvedValue(conn);
    return client;
  };

  it('exposes the schema, iterates every batch and releases the statement', async () => {
    const conn = fakeConn(makeTable(10));
    const stream = await clientWith(conn).executeStream('SELECT * FROM t');
    expect(stream).toBeInstanceOf(QueryStream);
    expect(stream.schema.fields.map(f => f.name)).toEqual(['id', 'name']);
    let rows = 0;
    for await (const batch of stream) {
      rows += batch.numRows;
    }
    expect(rows).toBe(10);
    expect(stream.done).toBe(true);
    expect(conn.stmt.setSqlQuery).toHaveBeenCalledWith('SELECT * FROM t');
    expect(conn.stmt.bind).not.toHaveBeenCalled();
    expect(conn.stmt.close).toHaveBeenCalledTimes(1);
  });

  it('binds parameters as a one-row Arrow table', async () => {
    const conn = fakeConn(makeTable(1));
    await clientWith(conn).executeStream('SELECT * FROM t WHERE id = ?', [7]);
    const bound = (conn.stmt.bind.mock.calls[0] as unknown[])[0] as Table;
    expect(bound.numRows).toBe(1);
    expect(bound.schema.fields).toHaveLength(1);
  });

  it('execute() materializes the stream into a Table', async () => {
    const conn = fakeConn(makeTable(5));
    const table = await clientWith(conn).execute('SELECT * FROM t');
    expect(table.numRows).toBe(5);
    expect(table.toArray()[4].name).toBe('row-4');
    expect(conn.stmt.close).toHaveBeenCalledTimes(1);
  });

  it('releases the reader when the consumer breaks out early', async () => {
    const cancelled = jest.fn();
    const stream = await clientWith(fakeConn(makeTable(3), { cancelSpy: cancelled }))
      .executeStream('SELECT * FROM t');
    let seen = 0;
    for await (const _batch of stream) {
      seen++;
      break;
    }
    expect(seen).toBe(1);
    expect(cancelled).toHaveBeenCalledTimes(1);
    expect(stream.done).toBe(true);
    for await (const _batch of stream) {
      seen++;
    }
    expect(seen).toBe(1);
    expect(cancelled).toHaveBeenCalledTimes(1);
  });

  it('cancel() is idempotent', async () => {
    const cancelled = jest.fn();
    const stream = await clientWith(fakeConn(makeTable(2), { cancelSpy: cancelled }))
      .executeStream('SELECT 1');
    await stream.cancel();
    await stream.cancel();
    expect(cancelled).toHaveBeenCalledTimes(1);
    expect(stream.done).toBe(true);
  });

  it('toTable() collects the remaining batches', async () => {
    const stream = await clientWith(fakeConn(makeTable(5))).executeStream('SELECT * FROM t');
    const table = await stream.toTable();
    expect(table.numRows).toBe(5);
  });

  it('maps driver errors on the execute phase to FlightSQLError and still closes the statement', async () => {
    const conn = fakeConn(makeTable(1));
    conn.stmt.executeQuery.mockImplementation(async () => {
      throw Object.assign(new Error('Binder Error: no such column'), { code: 'InvalidArguments' });
    });
    const client = clientWith(conn);
    await expect(client.executeStream('SELECT nope')).rejects.toThrow(FlightSQLError);
    await expect(client.executeStream('SELECT nope')).rejects.toThrow(/Binder Error/);
    expect(conn.stmt.close).toHaveBeenCalledTimes(2);
  });

  describe('cancellation (signal)', () => {
    it('aborting during execution closes the statement and rejects with QueryCancelledError', async () => {
      const conn = fakeConn(makeTable(1), { hang: true });
      const controller = new AbortController();
      const pending = clientWith(conn).execute('SELECT slow()', undefined, { signal: controller.signal });
      await new Promise(r => setTimeout(r, 20));
      expect(conn.stmt.close).not.toHaveBeenCalled();
      controller.abort(new Error('user pressed stop'));
      await expect(pending).rejects.toBeInstanceOf(QueryCancelledError);
      await expect(pending).rejects.toMatchObject({ reason: expect.objectContaining({ message: 'user pressed stop' }) });
      // Closed exactly once: by the abort, not again by the finally block.
      expect(conn.stmt.close).toHaveBeenCalledTimes(1);
    });

    it('AbortSignal.timeout() acts as a client-side deadline', async () => {
      const conn = fakeConn(makeTable(1), { hang: true });
      const started = Date.now();
      await expect(
        clientWith(conn).executeStream('SELECT slow()', undefined, { signal: AbortSignal.timeout(30) })
      ).rejects.toBeInstanceOf(QueryCancelledError);
      expect(Date.now() - started).toBeLessThan(1000);
      expect(conn.stmt.close).toHaveBeenCalledTimes(1);
    });

    it('a signal that is already aborted rejects before touching the connection', async () => {
      const conn = fakeConn(makeTable(1));
      const client = clientWith(conn);
      const signal = AbortSignal.abort('nope');
      await expect(client.execute('SELECT 1', undefined, { signal })).rejects.toBeInstanceOf(QueryCancelledError);
      await expect(client.executeUpdate('DELETE FROM t', undefined, { signal })).rejects.toBeInstanceOf(QueryCancelledError);
      expect(conn.createStatement).not.toHaveBeenCalled();
    });

    it('aborting while fetching cancels the stream and the iteration throws QueryCancelledError', async () => {
      const cancelled = jest.fn();
      const controller = new AbortController();
      const stream = await clientWith(fakeConn(makeTable(4), { cancelSpy: cancelled }))
        .executeStream('SELECT * FROM t', undefined, { signal: controller.signal });
      let batches = 0;
      await expect((async () => {
        for await (const _batch of stream) {
          batches++;
          controller.abort();
        }
      })()).rejects.toBeInstanceOf(QueryCancelledError);
      expect(batches).toBe(1);
      expect(cancelled).toHaveBeenCalledTimes(1);
      expect(stream.done).toBe(true);
    });

    it('executeUpdate honors the signal: closing the statement interrupts the update', async () => {
      const conn = fakeConn(makeTable(1), { hang: true });
      const controller = new AbortController();
      const pending = clientWith(conn).executeUpdate('DELETE FROM huge', undefined, { signal: controller.signal });
      await new Promise(r => setTimeout(r, 20));
      controller.abort();
      await expect(pending).rejects.toBeInstanceOf(QueryCancelledError);
      expect(conn.stmt.close).toHaveBeenCalledTimes(1);
    });

    it('executeUpdate returns the count when an older driver lets the update finish despite the abort', async () => {
      const conn = fakeConn(makeTable(1), { affected: 7 });
      const controller = new AbortController();
      conn.stmt.executeUpdate.mockImplementation(async () => {
        controller.abort(); // close() is a no-op for the update on old drivers
        return 7;
      });
      await expect(
        clientWith(conn).executeUpdate('DELETE FROM huge', undefined, { signal: controller.signal })
      ).resolves.toBe(7);
      expect(conn.stmt.close).toHaveBeenCalledTimes(1);
    });

    it('executeUpdate returns the affected-row count and releases the statement', async () => {
      const conn = fakeConn(makeTable(1), { affected: 3 });
      await expect(clientWith(conn).executeUpdate('DELETE FROM t WHERE id = ?', [1])).resolves.toBe(3);
      expect(conn.stmt.bind).toHaveBeenCalledTimes(1);
      expect(conn.stmt.close).toHaveBeenCalledTimes(1);
    });

    it('a signal that never fires leaves no listener behind', async () => {
      const conn = fakeConn(makeTable(2));
      const controller = new AbortController();
      const table = await clientWith(conn).execute('SELECT * FROM t', undefined, { signal: controller.signal });
      expect(table.numRows).toBe(2);
      controller.abort(); // must be a no-op now
      expect(conn.stmt.close).toHaveBeenCalledTimes(1);
    });
  });
});
