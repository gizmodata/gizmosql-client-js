import { execSync } from 'node:child_process';
import { FlightSQLClient } from '../../src/flightsql-client';
import { FlightSQLClientConfig } from '../../src/types';

const GIZMOSQL_PORT = 31337;
const GIZMOSQL_PASSWORD = 'test_password';
const CONTAINER_NAME = 'gizmosql-test';
const STARTUP_TIMEOUT_MS = 30000;
const RETRY_INTERVAL_MS = 1000;

const config: FlightSQLClientConfig = {
  host: 'localhost',
  port: GIZMOSQL_PORT,
  tlsSkipVerify: true,
  username: 'gizmosql',
  password: GIZMOSQL_PASSWORD
};

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
  // Don't stop if running in CI (managed by service)
  if (process.env.CI) {
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

  while (Date.now() - startTime < STARTUP_TIMEOUT_MS) {
    try {
      client = new FlightSQLClient(config);
      await client.execute('SELECT 1');
      console.log('GizmoSQL is ready');
      return;
    } catch {
      // Not ready yet
      await new Promise(resolve => setTimeout(resolve, RETRY_INTERVAL_MS));
    } finally {
      if (client) {
        await client.close();
      }
    }
  }

  throw new Error(`GizmoSQL did not start within ${STARTUP_TIMEOUT_MS}ms`);
}

const describeIfDocker = isDockerAvailable() ? describe : describe.skip;

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
        host: 'localhost',
        port: GIZMOSQL_PORT,
        tlsSkipVerify: true,
        username: 'gizmosql',
        password: GIZMOSQL_PASSWORD
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
        host: 'localhost',
        port: GIZMOSQL_PORT,
        tlsSkipVerify: true,
        username: 'gizmosql',
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
});
