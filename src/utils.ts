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
  if (error.code === 16) { // UNAUTHENTICATED
    return new FlightError('Authentication failed', 'UNAUTHENTICATED');
  }
  if (error.code === 14) { // UNAVAILABLE
    return new FlightError('Service unavailable', 'UNAVAILABLE');
  }

  return new FlightError(error.message || 'Unknown error', error.code?.toString());
}