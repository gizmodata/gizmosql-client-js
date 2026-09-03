import { execSync } from 'node:child_process';
import { FlightSQLClient } from '../../src/flightsql-client.js';
import { FlightSQLClientConfig } from '../../src/types.js';
import { QueryCancelledError } from '../../src/errors.js';
import { Table, vectorFromArray, Int32, Utf8 } from 'apache-arrow';

// By default the suite starts its own TLS-enabled GizmoSQL container. To
// run against an already-running server instead (e.g. a dev container on
// a different port, or one without TLS), set:
//   GIZMOSQL_TEST_EXTERNAL=1              don't start/stop a container
//   GIZMOSQL_TEST_HOST / GIZMOSQL_TEST_PORT
//   GIZMOSQL_TEST_USERNAME / GIZMOSQL_TEST_PASSWORD
//   GIZMOSQL_TEST_PLAINTEXT=1             connect without TLS
const EXTERNAL_SERVER = process.env.GIZMOSQL_TEST_EXTERNAL === '1';
const GIZMOSQL_HOST = process.env.GIZMOSQL_TEST_HOST ?? 'localhost';
const GIZMOSQL_PORT = Number(process.env.GIZMOSQL_TEST_PORT ?? 31337);
const GIZMOSQL_USERNAME = process.env.GIZMOSQL_TEST_USERNAME ?? 'gizmosql';
const GIZMOSQL_PASSWORD = process.env.GIZMOSQL_TEST_PASSWORD ?? 'test_password';
const GIZMOSQL_PLAINTEXT = process.env.GIZMOSQL_TEST_PLAINTEXT === '1';
const CONTAINER_NAME = 'gizmosql-test';
const STARTUP_TIMEOUT_MS = 30000;
const RETRY_INTERVAL_MS = 1000;

const config: FlightSQLClientConfig = {
  host: GIZMOSQL_HOST,
  port: GIZMOSQL_PORT,
  plaintext: GIZMOSQL_PLAINTEXT,
  tlsSkipVerify: !GIZMOSQL_PLAINTEXT,
  username: GIZMOSQL_USERNAME,
  password: GIZMOSQL_PASSWORD
};

/**
 * Resolves the name of the running GizmoSQL container. Locally this is the
 * fixed CONTAINER_NAME started by startGizmoSQL(); in CI the server runs as a
 * GitHub Actions service container with a generated name, so fall back to
 * finding it by image ancestry on the runner's Docker daemon.
 */
function resolveServerContainer(): string | null {
  // Must check the running state, not just existence: a stopped container
  // from an earlier local run would otherwise shadow the live server.
  if (isContainerRunning()) {
    return CONTAINER_NAME;
  }
  try {
    const names = execSync(
      'docker ps --filter "ancestor=gizmodata/gizmosql:latest" --format "{{.Names}}"',
      { encoding: 'utf-8' }
    ).trim();
    return names.split('\n', 1)[0] || null;
  } catch {
    return null;
  }
}

function isDockerAvailable(): boolean {
  try {
    execSync('docker --version', { stdio: 'ignore' });
    return true;
  } catch {
    return false;
  }
}

function isContainerRunning(): boolean {
  try {
    const result = execSync(`docker inspect -f '{{.State.Running}}' ${CONTAINER_NAME} 2>/dev/null`, {
      encoding: 'utf-8'
    });
    return result.trim() === 'true';
  } catch {
    return false;
  }
}

function startGizmoSQL(): void {
  if (EXTERNAL_SERVER) {
    return;
  }
  // Check if already running (e.g., in CI with services)
  if (isContainerRunning()) {
    console.log('GizmoSQL container already running');
    return;
  }

  // Remove any existing stopped container
  try {
    execSync(`docker rm -f ${CONTAINER_NAME} 2>/dev/null`, { stdio: 'ignore' });
  } catch {
    // Container doesn't exist, that's fine
  }

  console.log('Starting GizmoSQL container...');
  execSync(
    `docker run --name ${CONTAINER_NAME} ` +
    `--detach --tty --init ` +
    `--publish ${GIZMOSQL_PORT}:${GIZMOSQL_PORT} ` +
    `--env TLS_ENABLED="1" ` +
    `--env GIZMOSQL_USERNAME="gizmosql" ` +
    `--env GIZMOSQL_PASSWORD="${GIZMOSQL_PASSWORD}" ` +
    `gizmodata/gizmosql:latest`,
    { stdio: 'inherit' }
  );
}

function stopGizmoSQL(): void {
  // Don't stop if running in CI (managed by service) or externally
  if (EXTERNAL_SERVER || process.env.CI) {
    return;
  }

  try {
    execSync(`docker stop ${CONTAINER_NAME}`, { stdio: 'ignore' });
  } catch {
    // Container may already be stopped
  }
}

async function waitForGizmoSQL(): Promise<void> {
  const startTime = Date.now();
  let client: FlightSQLClient | null = null;
  let lastError: unknown;

  while (Date.now() - startTime < STARTUP_TIMEOUT_MS) {
    try {
      client = new FlightSQLClient(config);
      await client.execute('SELECT 1');
      console.log('GizmoSQL is ready');
      return;
    } catch (error) {
      // Not ready yet — remember why, so a persistent failure isn't silent
      lastError = error;
      await new Promise(resolve => setTimeout(resolve, RETRY_INTERVAL_MS));
    } finally {
      if (client) {
        await client.close();
      }
    }
  }

  const detail = lastError instanceof Error ? lastError.message : String(lastError);
  throw new Error(`GizmoSQL did not start within ${STARTUP_TIMEOUT_MS}ms (last error: ${detail})`);
}

const describeIfDocker = EXTERNAL_SERVER || isDockerAvailable() ? describe : describe.skip;

describeIfDocker('GizmoSQL Integration Tests', () => {
  let client: FlightSQLClient;

  beforeAll(async () => {
    // In CI, the container is started by the service
    // Locally, we need to start it ourselves
    if (!process.env.CI) {
      startGizmoSQL();
    }
    await waitForGizmoSQL();
  }, 60000);

  afterAll(async () => {
    stopGizmoSQL();
  });

  beforeEach(() => {
    client = new FlightSQLClient(config);
  });

  afterEach(async () => {
    await client.close();
  });

  describe('Basic SQL Operations', () => {
    it('should execute SELECT 1', async () => {
      const result = await client.execute('SELECT 1 AS num');
      const rows = result.toArray();
      expect(rows).toHaveLength(1);
      expect(rows[0].num).toBe(1);
    });

    it('should execute arithmetic expressions', async () => {
      const result = await client.execute('SELECT 2 + 2 AS sum');
      const rows = result.toArray();
      expect(rows).toHaveLength(1);
      expect(rows[0].sum).toBe(4);
    });

    it('should handle string queries', async () => {
      const result = await client.execute("SELECT 'hello' AS greeting");
      const rows = result.toArray();
      expect(rows).toHaveLength(1);
      expect(rows[0].greeting).toBe('hello');
    });

    it('should execute queries with multiple rows', async () => {
      const result = await client.execute('SELECT * FROM (VALUES (1), (2), (3)) AS t(num)');
      const rows = result.toArray();
      expect(rows).toHaveLength(3);
    });
  });

  describe('Table Operations', () => {
    const testTableName = 'test_table_' + Date.now();

    afterAll(async () => {
      // Clean up test table
      const cleanupClient = new FlightSQLClient(config);
      try {
        await cleanupClient.execute(`DROP TABLE IF EXISTS ${testTableName}`);
      } finally {
        await cleanupClient.close();
      }
    });

    it('should create a table', async () => {
      await client.execute(`CREATE TABLE ${testTableName} (id INTEGER, name VARCHAR)`);
      // If we get here without error, the table was created
      expect(true).toBe(true);
    });

    it('should insert data', async () => {
      await client.execute(`INSERT INTO ${testTableName} VALUES (1, 'Alice'), (2, 'Bob')`);
      expect(true).toBe(true);
    });

    it('should query inserted data', async () => {
      const result = await client.execute(`SELECT * FROM ${testTableName} ORDER BY id`);
      const rows = result.toArray();
      expect(rows).toHaveLength(2);
      expect(rows[0].id).toBe(1);
      expect(rows[0].name).toBe('Alice');
      expect(rows[1].id).toBe(2);
      expect(rows[1].name).toBe('Bob');
    });
  });

  describe('Metadata Operations', () => {
    it('should get catalogs', async () => {
      const catalogs = await client.getCatalogs();
      expect(Array.isArray(catalogs)).toBe(true);
    });

    it('should get schemas', async () => {
      const schemas = await client.getSchemas();
      expect(Array.isArray(schemas)).toBe(true);
    });

    it('should get tables', async () => {
      const tables = await client.getTables();
      expect(Array.isArray(tables)).toBe(true);
    });

    it('should get table types', async () => {
      const tableTypes = await client.getTableTypes();
      expect(Array.isArray(tableTypes)).toBe(true);
    });
  });

  describe('Connection Options', () => {
    it('should connect with tlsSkipVerify', async () => {
      const tlsClient = new FlightSQLClient({
        ...config,
        tlsSkipVerify: true,
      });

      try {
        const result = await tlsClient.execute('SELECT 1 AS num');
        const rows = result.toArray();
        expect(rows[0].num).toBe(1);
      } finally {
        await tlsClient.close();
      }
    });

    it('should handle authentication with username/password', async () => {
      const authClient = new FlightSQLClient({
        ...config,
        username: GIZMOSQL_USERNAME,
        password: GIZMOSQL_PASSWORD
      });

      try {
        const result = await authClient.execute('SELECT 1');
        expect(result).toBeDefined();
      } finally {
        await authClient.close();
      }
    });
  });

  describe('Session Lifecycle', () => {
    it('should send CloseSession RPC when closing the client', async () => {
      // The log assertion needs access to the server container's logs; in CI
      // the service container has a generated name, so resolve it dynamically.
      const container = resolveServerContainer();

      // Count existing session close messages in server logs
      const logsBefore = container
        ? execSync(`docker logs ${container} 2>&1`, { encoding: 'utf-8' })
        : '';
      const countBefore = (logsBefore.match(/Client session was successfully closed/g) || []).length;

      // Create a new client, establish a session, then close it
      const sessionClient = new FlightSQLClient(config);
      await sessionClient.execute('SELECT 1');
      await sessionClient.close();

      if (!container) {
        // Server container not visible to this Docker daemon — the close()
        // round-trip above still exercised the RPC; skip the log assertion.
        console.warn('GizmoSQL container not resolvable; skipping server-log assertion');
        return;
      }

      // Allow the server a moment to flush the log
      await new Promise(resolve => setTimeout(resolve, 500));

      // Verify the server logged a successful session close
      const logsAfter = execSync(`docker logs ${container} 2>&1`, { encoding: 'utf-8' });
      const countAfter = (logsAfter.match(/Client session was successfully closed/g) || []).length;

      expect(countAfter).toBeGreaterThan(countBefore);
    });

    it('should not throw when closing a client that was never connected', async () => {
      const unusedClient = new FlightSQLClient(config);
      await expect(unusedClient.close()).resolves.toBeUndefined();
    });

    it('should not throw when closing a client twice', async () => {
      const doubleCloseClient = new FlightSQLClient(config);
      await doubleCloseClient.execute('SELECT 1');
      await doubleCloseClient.close();
      await expect(doubleCloseClient.close()).resolves.toBeUndefined();
    });
  });
});

describeIfDocker('GizmoSQL Semantics (via the Go driver)', () => {
  let client: FlightSQLClient;

  beforeAll(async () => {
    if (!process.env.CI) {
      startGizmoSQL();
    }
    await waitForGizmoSQL();
  }, 60000);

  beforeEach(() => {
    client = new FlightSQLClient(config);
  });

  afterEach(async () => {
    const cleanup = new FlightSQLClient(config);
    try {
      await cleanup.execute('DROP TABLE IF EXISTS js_semantics_t');
    } finally {
      await cleanup.close();
      await client.close();
    }
  });

  it('executes DDL/DML immediately without any fetch', async () => {
    // The lazy-execution regression: results deliberately ignored.
    await client.execute('CREATE TABLE js_semantics_t (id INT)');
    await client.execute('INSERT INTO js_semantics_t VALUES (1), (2)');
    const count = await client.execute('SELECT COUNT(*)::INT AS n FROM js_semantics_t');
    expect(count.toArray()[0].n).toBe(2);
  });

  it('returns RETURNING rows and always persists the DML', async () => {
    await client.execute('CREATE TABLE js_semantics_t (id INT)');
    const returned = await client.execute(
      'INSERT INTO js_semantics_t VALUES (41), (42) RETURNING id'
    );
    expect(returned.toArray().map((r: any) => Number(r.id))).toEqual([41, 42]);

    // And persistence even when the result of a RETURNING is ignored:
    await client.execute('INSERT INTO js_semantics_t VALUES (43) RETURNING id');
    const count = await client.execute('SELECT COUNT(*)::INT AS n FROM js_semantics_t');
    expect(count.toArray()[0].n).toBe(3);
  });

  it('reports rows-affected semantics through DML statements', async () => {
    await client.execute('CREATE TABLE js_semantics_t (id INT)');
    await client.execute('INSERT INTO js_semantics_t VALUES (1), (2), (3)');
    await client.execute('UPDATE js_semantics_t SET id = id + 10 WHERE id > 1');
    const rows = await client.execute('SELECT id FROM js_semantics_t ORDER BY id');
    expect(rows.toArray().map((r: any) => Number(r.id))).toEqual([1, 12, 13]);
  });
});

describeIfDocker('Parameter binding', () => {
  let client: FlightSQLClient;
  const table = 'js_params_t';

  beforeAll(async () => {
    startGizmoSQL();
    await waitForGizmoSQL();
    const setup = new FlightSQLClient(config);
    try {
      await setup.execute(`DROP TABLE IF EXISTS ${table}`);
      await setup.execute(
        `CREATE TABLE ${table} (id INTEGER, name VARCHAR, score DOUBLE, big BIGINT, ` +
          'active BOOLEAN, born DATE, seen TIMESTAMP, payload BLOB)'
      );
      await setup.execute(`INSERT INTO ${table} (id, name, score) VALUES (1, 'Alice', 1.5), (2, 'Bob', 2.5), (3, 'Carol', 3.5)`);
    } finally {
      await setup.close();
    }
  }, 60000);

  afterAll(async () => {
    const cleanup = new FlightSQLClient(config);
    try {
      await cleanup.execute(`DROP TABLE IF EXISTS ${table}`);
    } finally {
      await cleanup.close();
    }
  });

  beforeEach(() => {
    client = new FlightSQLClient(config);
  });

  afterEach(async () => {
    await client.close();
  });

  it('binds a positional integer parameter in a WHERE clause', async () => {
    const rows = (await client.execute(`SELECT id, name FROM ${table} WHERE id = ?`, [2])).toArray();
    expect(rows).toHaveLength(1);
    expect(rows[0].name).toBe('Bob');
  });

  it('binds string and numeric parameters together', async () => {
    const rows = (
      await client.execute(`SELECT id FROM ${table} WHERE name = ? AND score > ?`, ['Carol', 3])
    ).toArray();
    expect(rows.map((r: any) => Number(r.id))).toEqual([3]);
  });

  it('supports $1-style positional placeholders', async () => {
    const rows = (await client.execute(`SELECT name FROM ${table} WHERE id = $1`, [1])).toArray();
    expect(rows[0].name).toBe('Alice');
  });

  it('inserts every supported JS type and reads it back', async () => {
    const born = new Date('1990-05-06T00:00:00Z');
    const seen = new Date('2024-01-02T03:04:05.678Z');
    const affected = await client.executeUpdate(
      `INSERT INTO ${table} VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      [10, 'Dave', 9.25, 9007199254740993n, true, born, seen, new Uint8Array([1, 2, 3])]
    );
    expect(affected).toBe(1);

    const row = (await client.execute(`SELECT * FROM ${table} WHERE id = ?`, [10])).toArray()[0].toJSON();
    expect(row.name).toBe('Dave');
    expect(row.score).toBe(9.25);
    expect(BigInt(row.big)).toBe(9007199254740993n);
    expect(row.active).toBe(true);
    expect(Number(row.born)).toBe(born.getTime());
    expect(Number(row.seen)).toBe(seen.getTime());
    expect(Array.from(row.payload)).toEqual([1, 2, 3]);
  });

  it('executeUpdate reports affected rows for parameterized DML', async () => {
    const updated = await client.executeUpdate(`UPDATE ${table} SET score = ? WHERE id > ?`, [0.5, 1]);
    expect(updated).toBeGreaterThanOrEqual(2);
    const deleted = await client.executeUpdate(`DELETE FROM ${table} WHERE id = ?`, [10]);
    expect(deleted).toBe(1);
  });

  it('accepts a pre-built one-row Arrow Table', async () => {
    const params = new Table({
      id: vectorFromArray([1], new Int32()),
      name: vectorFromArray(['Alice'], new Utf8()),
    });
    const rows = (await client.execute(`SELECT id FROM ${table} WHERE id = ? AND name = ?`, params)).toArray();
    expect(rows).toHaveLength(1);
  });

  it('binds parameters on prepared statements, re-executing with new values', async () => {
    const prepared = await client.prepare(`SELECT name FROM ${table} WHERE id = ?`);
    try {
      expect((await client.executePrepared(prepared, [1]))[0].name).toBe('Alice');
      expect((await client.executePrepared(prepared, [2]))[0].name).toBe('Bob');
    } finally {
      await client.closePrepared(prepared);
    }
  });

  it('getQuerySchema works with parameters', async () => {
    const schema = await client.getQuerySchema(`SELECT id, name FROM ${table} WHERE id = ?`, [1]);
    expect(schema.fields.map((f) => f.name)).toEqual(['id', 'name']);
  });

  it('rejects a multi-row parameter table client-side', async () => {
    const params = new Table({ id: vectorFromArray([1, 2], new Int32()) });
    await expect(client.execute(`SELECT id FROM ${table} WHERE id = ?`, params)).rejects.toThrow(/exactly one row/);
  });

  it('surfaces a placeholder-count mismatch as a FlightSQLError', async () => {
    await expect(client.execute(`SELECT id FROM ${table} WHERE id = ?`, [1, 2])).rejects.toThrow(/Failed to execute query/);
  });

  // GizmoSQL servers up to the current release stringify bound parameters,
  // so a NULL arrives as the text 'null'. A server fix is in progress; run
  // with GIZMOSQL_TEST_NULL_PARAMS=1 against a fixed server.
  const describeIfNullParams = process.env.GIZMOSQL_TEST_NULL_PARAMS === '1' ? describe : describe.skip;
  describeIfNullParams('null parameters (requires a server that binds Arrow nulls)', () => {
    it('binds null as SQL NULL', async () => {
      const rows = (await client.execute('SELECT ?::INTEGER IS NULL AS is_null', [null])).toArray();
      expect(rows[0].is_null).toBe(true);
    });
  });
});

describeIfDocker('Streaming results (executeStream)', () => {
  let client: FlightSQLClient;

  beforeAll(async () => {
    if (!process.env.CI) {
      startGizmoSQL();
    }
    await waitForGizmoSQL();
  }, 60000);

  beforeEach(() => {
    client = new FlightSQLClient(config);
  });

  afterEach(async () => {
    await client.close();
  });

  it('streams a multi-batch result incrementally', async () => {
    const stream = await client.executeStream(
      'SELECT range AS i, \'v\' || range AS s FROM range(100000)'
    );
    expect(stream.schema.fields.map(f => f.name)).toEqual(['i', 's']);
    let batches = 0;
    let rows = 0;
    for await (const batch of stream) {
      batches++;
      rows += batch.numRows;
    }
    expect(batches).toBeGreaterThan(1);
    expect(rows).toBe(100000);
    expect(stream.done).toBe(true);
  });

  it('breaking out early releases the stream and keeps the client usable', async () => {
    const stream = await client.executeStream('SELECT range AS i FROM range(5000000)');
    let seen = 0;
    for await (const batch of stream) {
      seen += batch.numRows;
      if (seen >= 1) break;
    }
    expect(seen).toBeGreaterThan(0);
    expect(seen).toBeLessThan(5000000);
    expect(stream.done).toBe(true);
    const after = await client.execute('SELECT 42 AS answer');
    expect(after.toArray()[0].answer).toBe(42);
  });

  it('cancel() before iterating is safe and the client stays usable', async () => {
    const stream = await client.executeStream('SELECT range AS i FROM range(1000000)');
    await stream.cancel();
    let seen = 0;
    for await (const _batch of stream) {
      seen++;
    }
    expect(seen).toBe(0);
    expect((await client.execute('SELECT 1 AS ok')).toArray()[0].ok).toBe(1);
  });

  it('binds parameters and toTable() materializes the rest', async () => {
    const stream = await client.executeStream('SELECT range AS i FROM range(?::BIGINT)', [10]);
    const table = await stream.toTable();
    expect(table.numRows).toBe(10);
    expect(Number(table.toArray()[9].i)).toBe(9);
  });

  it('surfaces server errors from the execute phase as FlightSQLError', async () => {
    await expect(client.executeStream('SELECT * FROM no_such_table_stream')).rejects.toThrow(
      /no_such_table_stream/
    );
    expect((await client.execute('SELECT 1 AS ok')).toArray()[0].ok).toBe(1);
  });

  it('accepts extra driver options via adbcOptions', async () => {
    const custom = new FlightSQLClient({
      ...config,
      adbcOptions: { 'adbc.flight.sql.rpc.call_header.x-gizmosql-client-test': 'streaming' },
    });
    try {
      expect((await custom.execute('SELECT 1 AS ok')).toArray()[0].ok).toBe(1);
    } finally {
      await custom.close();
    }
  });
});

/**
 * Server log lines mentioning `marker` (a literal embedded in the SQL under
 * test). Returns null when the server container cannot be found (e.g. an
 * external server without Docker access), in which case log assertions are
 * skipped and only the client-visible behavior is checked.
 */
function serverLogLines(marker: string): string[] | null {
  const container = process.env.GIZMOSQL_TEST_CONTAINER ?? resolveServerContainer();
  if (!container) return null;
  try {
    const out = execSync(`docker logs --since 120s ${container} 2>&1`, { encoding: 'utf-8' });
    return out.split('\n').filter(line => line.includes(marker));
  } catch {
    return null;
  }
}

// A query that keeps DuckDB busy for a long time (minutes) but is cheap to
// plan; `marker` makes its server log lines identifiable.
const slowQuery = (marker: string) =>
  `SELECT count(*) AS n, '${marker}' AS marker FROM range(300000000000)`;

describeIfDocker('Cancellation and timeouts', () => {
  let client: FlightSQLClient;

  beforeAll(async () => {
    if (!process.env.CI) {
      startGizmoSQL();
    }
    await waitForGizmoSQL();
  }, 60000);

  beforeEach(() => {
    client = new FlightSQLClient(config);
  });

  afterEach(async () => {
    await client.close();
  });

  it('aborting the signal cancels a statement the server is still executing', async () => {
    const marker = `abort-exec-${Date.now()}`;
    const controller = new AbortController();
    const started = Date.now();
    const pending = client.execute(slowQuery(marker), undefined, { signal: controller.signal });
    await new Promise(r => setTimeout(r, 1000));
    controller.abort();
    await expect(pending).rejects.toBeInstanceOf(QueryCancelledError);
    expect(Date.now() - started).toBeLessThan(10000);

    // The same client is immediately usable again.
    expect((await client.execute('SELECT 1 AS ok')).toArray()[0].ok).toBe(1);

    // And the server actually interrupted the statement (GizmoSQL >= 1.38.0).
    await new Promise(r => setTimeout(r, 1000));
    const lines = serverLogLines(marker);
    if (lines !== null) {
      expect(lines.length).toBeGreaterThan(0);
      expect(lines.some(l => /interrupt/i.test(l))).toBe(true);
    }
  }, 30000);

  it('AbortSignal.timeout() gives a client-side deadline', async () => {
    const marker = `abort-timeout-${Date.now()}`;
    const started = Date.now();
    await expect(
      client.execute(slowQuery(marker), undefined, { signal: AbortSignal.timeout(1000) })
    ).rejects.toBeInstanceOf(QueryCancelledError);
    expect(Date.now() - started).toBeLessThan(10000);
    expect((await client.execute('SELECT 2 AS ok')).toArray()[0].ok).toBe(2);
  }, 30000);

  it('executeStream honors the signal during execution', async () => {
    const marker = `abort-stream-${Date.now()}`;
    await expect(
      client.executeStream(slowQuery(marker), undefined, { signal: AbortSignal.timeout(1000) })
    ).rejects.toBeInstanceOf(QueryCancelledError);
    expect((await client.execute('SELECT 3 AS ok')).toArray()[0].ok).toBe(3);
  }, 30000);

  it('aborting while fetching stops the stream with QueryCancelledError', async () => {
    const controller = new AbortController();
    const stream = await client.executeStream(
      'SELECT range AS i FROM range(20000000)',
      undefined,
      { signal: controller.signal }
    );
    let batches = 0;
    await expect((async () => {
      for await (const _batch of stream) {
        batches++;
        if (batches === 2) controller.abort();
      }
    })()).rejects.toBeInstanceOf(QueryCancelledError);
    expect(batches).toBeGreaterThanOrEqual(2);
    expect(batches).toBeLessThan(1000);
    expect(stream.done).toBe(true);
    expect((await client.execute('SELECT 4 AS ok')).toArray()[0].ok).toBe(4);
  }, 30000);

  it('executeUpdate cannot be interrupted mid-flight (DoPut): the statement completes', async () => {
    await client.executeUpdate('DROP TABLE IF EXISTS ctas_not_cancelled');
    const controller = new AbortController();
    // Long enough to still be running when the abort fires, short enough for a test.
    const pending = client.executeUpdate(
      'CREATE TABLE ctas_not_cancelled AS SELECT count(*) AS n FROM range(2000000000)',
      undefined,
      { signal: controller.signal }
    );
    await new Promise(r => setTimeout(r, 200));
    controller.abort();
    await expect(pending).resolves.toBeGreaterThanOrEqual(0);
    const tables = await client.getTables(undefined, undefined, 'ctas_not_cancelled');
    expect(tables).toHaveLength(1);
    await client.executeUpdate('DROP TABLE ctas_not_cancelled');
  }, 60000);

  it('executeUpdate rejects an already-aborted signal before running', async () => {
    await expect(
      client.executeUpdate('CREATE TABLE never_created (id INT)', undefined, { signal: AbortSignal.abort() })
    ).rejects.toBeInstanceOf(QueryCancelledError);
    expect(await client.getTables(undefined, undefined, 'never_created')).toHaveLength(0);
  });

  it('a pre-aborted signal rejects without running anything', async () => {
    await expect(
      client.execute('SELECT 1', undefined, { signal: AbortSignal.abort() })
    ).rejects.toBeInstanceOf(QueryCancelledError);
  });

  it('SET gizmosql.query_timeout interrupts long statements server-side and the session stays usable', async () => {
    const marker = `server-timeout-${Date.now()}`;
    await client.executeUpdate('SET gizmosql.query_timeout = 1');
    const started = Date.now();
    await expect(client.execute(slowQuery(marker))).rejects.toThrow(/timed out/i);
    expect(Date.now() - started).toBeLessThan(10000);
    expect((await client.execute('SELECT 5 AS ok')).toArray()[0].ok).toBe(5);
    await client.executeUpdate('SET gizmosql.query_timeout = 0');

    const lines = serverLogLines(marker);
    if (lines !== null) {
      expect(lines.some(l => /status=timeout/i.test(l))).toBe(true);
    }
  }, 30000);

  it('a killed client process makes the server interrupt its statement', async () => {
    const marker = `kill9-${Date.now()}`;
    const { spawn } = await import('node:child_process');
    const script = `
      import('${new URL('../../dist/index.js', import.meta.url).href}').then(async ({ FlightSQLClient }) => {
        const c = new FlightSQLClient(${JSON.stringify(config)});
        await c.execute(${JSON.stringify(slowQuery(marker))});
      });
    `;
    const child = spawn(process.execPath, ['--input-type=module', '-e', script], {
      stdio: 'ignore',
      env: { ...process.env, GIZMOSQL_DRIVER_LIB: process.env.GIZMOSQL_DRIVER_LIB ?? '' },
    });
    await new Promise(r => setTimeout(r, 2500));
    child.kill('SIGKILL');
    await new Promise(r => setTimeout(r, 3000));
    const lines = serverLogLines(marker);
    if (lines === null) {
      console.warn('server container not reachable; skipping log assertion');
      return;
    }
    expect(lines.some(l => /client_disconnected|went away/i.test(l))).toBe(true);
  }, 30000);
});
