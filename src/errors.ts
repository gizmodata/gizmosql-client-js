export class FlightError extends Error {
  constructor(message: string, public code?: string) {
    super(message);
    this.name = 'FlightError';
  }
}

export class FlightSQLError extends FlightError {
  constructor(message: string, public sqlState?: string, code?: string) {
    super(message, code);
    this.name = 'FlightSQLError';
  }
}

export class ConnectionError extends FlightError {
  constructor(message: string, public originalError?: Error) {
    super(message);
    this.name = 'ConnectionError';
  }
}

export class AuthenticationError extends FlightError {
  constructor(message: string = 'Authentication failed') {
    super(message);
    this.name = 'AuthenticationError';
  }
}

export class SchemaError extends FlightError {
  constructor(message: string) {
    super(message);
    this.name = 'SchemaError';
  }
}

/**
 * Thrown when a statement is cancelled through the `signal` option of
 * `execute()` / `executeStream()` / `executeUpdate()` (including
 * `AbortSignal.timeout()`), or when the server interrupts it.
 */
export class QueryCancelledError extends FlightSQLError {
  constructor(message: string = 'Query cancelled', public reason?: unknown) {
    super(message);
    this.name = 'QueryCancelledError';
  }
}
