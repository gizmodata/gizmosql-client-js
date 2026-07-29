// Spike: prove the native Go GizmoSQL ADBC driver works from Node.js
// via @apache-arrow/adbc-driver-manager — the architecture basis for
// @gizmodata/gizmosql-client 2.0 (docs/go-driver-rewrite-plan.md).
//
// Usage:
//   GIZMOSQL_DRIVER_LIB=/path/to/libadbc_driver_gizmosql.dylib \
//   GIZMOSQL_URI="gizmosql://localhost:PORT?transport=tcp" \
//   GIZMOSQL_USERNAME=u GIZMOSQL_PASSWORD=p \
//   node scripts/spike-adbc.mjs
//
// Proves: SELECT round trip, DDL/DML immediate execution WITHOUT any
// fetch (GizmoSQL's lazy-execution model handled inside the driver),
// and INSERT ... RETURNING persistence.

import { AdbcDatabase } from '@apache-arrow/adbc-driver-manager';

const lib = process.env.GIZMOSQL_DRIVER_LIB;
const uri = process.env.GIZMOSQL_URI;
if (!lib || !uri) {
  console.error('Set GIZMOSQL_DRIVER_LIB and GIZMOSQL_URI (and credentials).');
  process.exit(2);
}

const db = new AdbcDatabase({
  driver: lib,
  databaseOptions: {
    uri,
    username: process.env.GIZMOSQL_USERNAME ?? 'gizmosql_user',
    password: process.env.GIZMOSQL_PASSWORD ?? 'gizmosql_password',
  },
});

const conn = await db.connect();
try {
  // 1. SELECT round trip → Arrow table
  const t1 = await conn.query('SELECT 1 AS v');
  const v = t1.toArray()[0].v;
  if (Number(v) !== 1) throw new Error(`SELECT 1 returned ${v}`);
  console.log('PASS: SELECT round trip (Arrow table)');

  // 2. DDL/DML immediate execution — results deliberately unused
  await conn.query('CREATE TABLE spike_t (id INT)');
  await conn.query('INSERT INTO spike_t VALUES (1), (2)');
  const t2 = await conn.query('SELECT COUNT(*) AS n FROM spike_t');
  const n = Number(t2.toArray()[0].n);
  if (n !== 2) throw new Error(`lazy-execution bug: COUNT(*)=${n}, want 2`);
  console.log('PASS: DDL/DML executes immediately (no fetch needed)');

  // 3. RETURNING persists (reader never consumed beyond the call)
  await conn.query('INSERT INTO spike_t VALUES (3) RETURNING id');
  const t3 = await conn.query('SELECT COUNT(*) AS n FROM spike_t');
  const n3 = Number(t3.toArray()[0].n);
  if (n3 !== 3) throw new Error(`RETURNING not persisted: COUNT(*)=${n3}, want 3`);
  console.log('PASS: INSERT..RETURNING persists');

  await conn.query('DROP TABLE spike_t');
  console.log('ALL PASS — Go driver serves Node.js via the ADBC driver manager');
} finally {
  await conn.close();
  await db.close();
}
