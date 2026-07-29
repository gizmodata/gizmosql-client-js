// Unit-test stub for @apache-arrow/adbc-driver-manager (a native ESM
// addon Jest cannot parse). Unit tests exercise config mapping and
// client-side lifecycle only; anything driver-facing belongs in
// test/integration, which uses the real package.

export const ObjectDepth = {
  All: 0,
  Catalogs: 1,
  Schemas: 2,
  Tables: 3,
} as const;

export class AdbcError extends Error {
  constructor(
    message: string,
    public code: string = 'UNKNOWN',
    public vendorCode?: number,
    public sqlState?: string
  ) {
    super(message);
    this.name = 'AdbcError';
  }
}

export class AdbcConnection {
  // Never used by unit tests — they mock at the FlightSQLClient level.
}

export class AdbcDatabase {
  constructor(public readonly options: unknown) {}
  async connect(): Promise<AdbcConnection> {
    throw new Error('adbc-driver-manager stub: use test/integration for driver-facing tests');
  }
  async close(): Promise<void> {}
}
