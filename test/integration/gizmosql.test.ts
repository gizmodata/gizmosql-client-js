import { execSync } from 'node:child_process';
import { FlightSQLClient } from '../../src/flightsql-client.js';
import { FlightSQLClientConfig } from '../../src/types.js';
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
