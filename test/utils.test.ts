import { createConnectionString, validateConfig, toClientError } from '../src/utils';
import {
  AuthenticationError,
  ConnectionError,
  FlightError,
  FlightSQLError,
} from '../src/errors';

describe('createConnectionString', () => {
  it('builds https URLs by default', () => {
    expect(createConnectionString('localhost', 31337, false)).toBe('https://localhost:31337');
  });

  it('builds http URLs for plaintext', () => {
    expect(createConnectionString('localhost', 31337, true)).toBe('http://localhost:31337');
  });
});

describe('validateConfig', () => {
  it('accepts a valid config', () => {
    expect(() => validateConfig({ host: 'localhost', port: 31337 })).not.toThrow();
  });

  it('rejects a missing host', () => {
    expect(() => validateConfig({ host: '', port: 31337 })).toThrow(FlightError);
  });

  it('rejects an invalid port', () => {
    expect(() => validateConfig({ host: 'localhost', port: 0 })).toThrow(FlightError);
    expect(() => validateConfig({ host: 'localhost', port: -1 })).toThrow(FlightError);
  });
});

describe('toClientError (ADBC error mapping)', () => {
  it('passes through existing FlightErrors unchanged', () => {
    const original = new FlightSQLError('boom');
    expect(toClientError(original, 'ctx')).toBe(original);
  });

  it('maps Unauthenticated to AuthenticationError', () => {
    const err = toClientError({ message: 'bad creds', code: 'Unauthenticated' }, 'Failed to connect');
    expect(err).toBeInstanceOf(AuthenticationError);
    expect(err.message).toContain('bad creds');
  });

  it('maps IO errors to ConnectionError', () => {
    const err = toClientError({ message: 'refused', code: 'IO' }, 'Failed to connect');
    expect(err).toBeInstanceOf(ConnectionError);
    expect(err.message).toContain('Failed to connect');
  });

  it('uses the fallback class with the ADBC code preserved', () => {
    const err = toClientError({ message: 'syntax', code: 'InvalidArguments' }, 'Failed to execute', FlightSQLError);
    expect(err).toBeInstanceOf(FlightSQLError);
    expect(err.code).toBe('InvalidArguments');
  });

  it('stringifies unknown error shapes', () => {
    const err = toClientError('plain string failure', 'ctx');
    expect(err.message).toContain('plain string failure');
  });
});
