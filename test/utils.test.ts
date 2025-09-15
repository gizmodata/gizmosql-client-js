import {
  createConnectionString,
  parseErrorFromGrpc,
  validateConfig
} from '../src/utils';
import {FlightError} from '../src';

describe('Utils', () => {
  describe('validateConfig', () => {
    it('should pass validation for valid config', () => {
      expect(() => {
        validateConfig({ host: 'localhost', port: 4317 });
      }).not.toThrow();
    });

    it('should pass validation with different hosts', () => {
      expect(() => {
        validateConfig({ host: '127.0.0.1', port: 8080 });
      }).not.toThrow();

      expect(() => {
        validateConfig({ host: 'example.com', port: 443 });
      }).not.toThrow();
    });

    it('should throw for missing host', () => {
      expect(() => {
        validateConfig({ host: '', port: 4317 });
      }).toThrow('Host is required');

      expect(() => {
        validateConfig({ port: 4317 } as any);
      }).toThrow('Host is required');
    });

    it('should throw for invalid port', () => {
      expect(() => {
        validateConfig({ host: 'localhost', port: 0 });
      }).toThrow('Valid port number is required');

      expect(() => {
        validateConfig({ host: 'localhost', port: -1 });
      }).toThrow('Valid port number is required');

      expect(() => {
        validateConfig({ host: 'localhost' } as any);
      }).toThrow('Valid port number is required');
    });
  });

  describe('createConnectionString', () => {
    it('should create HTTP connection string for plaintext', () => {
      const connectionString = createConnectionString('localhost', 4317, true);
      expect(connectionString).toBe('http://localhost:4317');
    });

    it('should create HTTPS connection string by default', () => {
      const connectionString = createConnectionString('localhost', 4317, false);
      expect(connectionString).toBe('https://localhost:4317');
    });

    it('should handle different hosts and ports', () => {
      expect(createConnectionString('example.com', 8080, true)).toBe('http://example.com:8080');
      expect(createConnectionString('127.0.0.1', 443, false)).toBe('https://127.0.0.1:443');
    });
  });

  describe('parseErrorFromGrpc', () => {
    it('should handle authentication errors', () => {
      const grpcError = { code: 16, message: 'Unauthenticated' };
      const flightError = parseErrorFromGrpc(grpcError);

      expect(flightError).toBeInstanceOf(FlightError);
      expect(flightError.message).toBe('Authentication failed');
      expect(flightError.code).toBe('UNAUTHENTICATED');
    });

    it('should handle unavailable errors', () => {
      const grpcError = { code: 14, message: 'Unavailable' };
      const flightError = parseErrorFromGrpc(grpcError);

      expect(flightError).toBeInstanceOf(FlightError);
      expect(flightError.message).toBe('Service unavailable');
      expect(flightError.code).toBe('UNAVAILABLE');
    });

    it('should handle unknown errors', () => {
      const grpcError = { code: 99, message: 'Unknown error' };
      const flightError = parseErrorFromGrpc(grpcError);

      expect(flightError).toBeInstanceOf(FlightError);
      expect(flightError.message).toBe('Unknown error');
      expect(flightError.code).toBe('99');
    });

    it('should handle errors without message', () => {
      const grpcError = { code: 1 };
      const flightError = parseErrorFromGrpc(grpcError);

      expect(flightError).toBeInstanceOf(FlightError);
      expect(flightError.message).toBe('Unknown error');
      expect(flightError.code).toBe('1');
    });

    it('should handle errors without code', () => {
      const grpcError = { message: 'Some error' };
      const flightError = parseErrorFromGrpc(grpcError);

      expect(flightError).toBeInstanceOf(FlightError);
      expect(flightError.message).toBe('Some error');
      expect(flightError.code).toBeUndefined();
    });

    it('should handle completely malformed errors', () => {
      const grpcError = {};
      const flightError = parseErrorFromGrpc(grpcError);

      expect(flightError).toBeInstanceOf(FlightError);
      expect(flightError.message).toBe('Unknown error');
      expect(flightError.code).toBeUndefined();
    });
  });
});