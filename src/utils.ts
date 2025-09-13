import { RecordBatch, Table } from 'apache-arrow';
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

export function arrowToJsonRows(batches: RecordBatch[]): any[] {
  const rows: any[] = [];

  for (const batch of batches) {
    const table = new Table([batch]);
    for (let i = 0; i < table.numRows; i++) {
      const row: any = {};
      for (let j = 0; j < table.numCols; j++) {
        const field = table.schema.fields[j];
        const column = table.getChild(field.name);
        row[field.name] = column?.get(i);
      }
      rows.push(row);
    }
  }

  return rows;
}

export function createCredentialsMetadata(username?: string, password?: string): any {
  if (!username || !password) {
    return {};
  }

  const credentials = Buffer.from(`${username}:${password}`).toString('base64');
  return {
    authorization: `Basic ${credentials}`
  };
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