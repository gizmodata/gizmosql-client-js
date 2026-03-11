import { FlightError } from './errors';

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

export function parseErrorFromGrpc(error: any): FlightError {
  // gRPC errors have a `details` field with the server's actual error message,
  // and a `message` field formatted as "CODE STATUS_TEXT: details".
  // Prefer `details` for a cleaner message; fall back to `message`.
  const detail: string = error.details || error.message || 'Unknown error';

  if (error.code === 16) { // UNAUTHENTICATED
    return new FlightError(detail, 'UNAUTHENTICATED');
  }
  if (error.code === 14) { // UNAVAILABLE
    return new FlightError(detail, 'UNAVAILABLE');
  }

  return new FlightError(detail, error.code?.toString());
}