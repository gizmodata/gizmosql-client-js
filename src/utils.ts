import { AuthenticationError, ConnectionError, FlightError } from './errors';

export function createConnectionString(host: string, port: number, plaintext: boolean): string {
  const protocol = plaintext ? 'http' : 'https';
  return `${protocol}://${host}:${port}`;
}

export function validateConfig(config: { host: string; port: number }): void {
  if (!config.host) {
    throw new FlightError('Host is required');
  }
  if (!config.port || config.port <= 0) {
    throw new FlightError('Valid port number is required');
  }
}

/**
 * Maps an error from the ADBC driver manager onto this package's error
 * hierarchy. AdbcError carries a string status `code` (e.g.
 * 'Unauthenticated', 'IO', 'InvalidArguments') plus optional
 * vendorCode/sqlState.
 */
export function toClientError(
  error: unknown,
  context: string,
  fallback: new (message: string, ...rest: any[]) => FlightError = ConnectionError
): FlightError {
  if (error instanceof FlightError) {
    return error;
  }
  const anyErr = error as { message?: string; code?: string } | undefined;
  const detail = anyErr?.message ?? String(error);
  const code = anyErr?.code;
  if (code === 'Unauthenticated' || code === 'Unauthorized') {
    return new AuthenticationError(`${context}: ${detail}`);
  }
  if (code === 'IO' || code === 'Timeout' || code === 'Cancelled') {
    return new ConnectionError(`${context}: ${detail}`);
  }
  const err = new fallback(`${context}: ${detail}`);
  err.code = code;
  return err;
}