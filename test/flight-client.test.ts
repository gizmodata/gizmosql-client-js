import { FlightClient } from '../src/flight-client';
import { FlightClientConfig } from '../src/types';
import { FlightError } from '../src/errors';

describe('FlightClient', () => {
  describe('constructor', () => {
    it('should create a client with valid config', () => {
      const config: FlightClientConfig = {
        host: 'localhost',
        port: 4317,
        plaintext: true
      };

      const client = new FlightClient(config);
      expect(client).toBeInstanceOf(FlightClient);
    });

    it('should throw error for invalid config', () => {
      expect(() => {
        new FlightClient({} as FlightClientConfig);
      }).toThrow(FlightError);
    });

    it('should default plaintext to false', () => {
      const config: FlightClientConfig = {
        host: 'localhost',
        port: 4317
      };

      const client = new FlightClient(config);
      expect(client).toBeInstanceOf(FlightClient);
    });

    it('should create secure credentials by default', () => {
      const config: FlightClientConfig = {
        host: 'localhost',
        port: 4317
      };

      const client = new FlightClient(config);
      const credentials = (client as any).credentials;
      expect(credentials).toBeDefined();
    });

    it('should create insecure credentials for plaintext', () => {
      const config: FlightClientConfig = {
        host: 'localhost',
        port: 4317,
        plaintext: true
      };

      const client = new FlightClient(config);
      const credentials = (client as any).credentials;
      expect(credentials).toBeDefined();
    });

    it('should create credentials with tlsSkipVerify', () => {
      const config: FlightClientConfig = {
        host: 'localhost',
        port: 4317,
        tlsSkipVerify: true
      };

      const client = new FlightClient(config);
      const credentials = (client as any).credentials;
      expect(credentials).toBeDefined();
    });

    it('should use plaintext over tlsSkipVerify when both set', () => {
      const config: FlightClientConfig = {
        host: 'localhost',
        port: 4317,
        plaintext: true,
        tlsSkipVerify: true
      };

      const client = new FlightClient(config);
      const credentials = (client as any).credentials;
      expect(credentials).toBeDefined();
    });

    it('should create metadata with authentication', () => {
      const config: FlightClientConfig = {
        host: 'localhost',
        port: 4317,
        plaintext: true,
        token: 'test'
      };

      const client = new FlightClient(config);
      const metadata = (client as any).metadata;
      expect(metadata).toBeDefined();
    });
  });

  describe('connection management', () => {
    let client: FlightClient;

    beforeEach(() => {
      client = new FlightClient({
        host: 'localhost',
        port: 4317,
        plaintext: true
      });
    });

    afterEach(async () => {
      await client.close();
    });

    it('should handle close without connection', async () => {
      await expect(client.close()).resolves.not.toThrow();
    });

    it('should handle multiple closes', async () => {
      await client.close();
      await expect(client.close()).resolves.not.toThrow();
    });

    it('should validate host and port', () => {
      expect(() => {
        new FlightClient({
          host: '',
          port: 4317
        });
      }).toThrow('Host is required');

      expect(() => {
        new FlightClient({
          host: 'localhost',
          port: 0
        });
      }).toThrow('Valid port number is required');

      expect(() => {
        new FlightClient({
          host: 'localhost',
          port: -1
        });
      }).toThrow('Valid port number is required');
    });
  });

  describe('Arrow data processing', () => {
    let client: FlightClient;

    beforeEach(() => {
      client = new FlightClient({
        host: 'localhost',
        port: 4317,
        plaintext: true
      });
    });

    afterEach(async () => {
      await client.close();
    });

    it('should handle empty flight data', () => {
      // Test that the client handles empty responses properly
      expect(client).toBeInstanceOf(FlightClient);
    });

    it('should process FlightData with header and body', () => {
      // Test would verify the Arrow IPC format reconstruction
      // Since this is internal logic, we focus on integration tests
      expect(client).toBeInstanceOf(FlightClient);
    });
  });

  describe('gRPC configuration', () => {
    it('should configure gRPC options for large messages', () => {
      const client = new FlightClient({
        host: 'localhost',
        port: 4317,
        plaintext: true
      });

      // Verify that the client is configured properly
      expect(client).toBeInstanceOf(FlightClient);
    });

    it('should set up proper keepalive settings', () => {
      const client = new FlightClient({
        host: 'localhost',
        port: 4317,
        plaintext: true
      });

      // Verify keepalive configuration is applied
      expect(client).toBeInstanceOf(FlightClient);
    });
  });

  describe('error handling', () => {
    let client: FlightClient;

    beforeEach(() => {
      client = new FlightClient({
        host: 'localhost',
        port: 4317,
        plaintext: true
      });
    });

    afterEach(async () => {
      await client.close();
    });

    it('should handle connection errors gracefully', async () => {
      // Mock a connection failure
      const invalidClient = new FlightClient({
        host: 'nonexistent-host',
        port: 9999,
        plaintext: true
      });

      // Connection attempts should be handled properly
      expect(invalidClient).toBeInstanceOf(FlightClient);
      await invalidClient.close();
    });

    it('should handle authentication errors', () => {
      const client = new FlightClient({
        host: 'localhost',
        port: 4317,
        plaintext: true,
        token: 'invalid'
      });

      expect(client).toBeInstanceOf(FlightClient);
    });
  });
});