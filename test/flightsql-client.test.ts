import { FlightSQLClient } from '../src/flightsql-client';
import { FlightSQLClientConfig } from '../src/types';
import { FlightSQLError } from '../src/errors';

describe('FlightSQLClient', () => {
  describe('constructor', () => {
    it('should create a FlightSQL client with valid config', () => {
      const config: FlightSQLClientConfig = {
        host: 'localhost',
        port: 4317,
        plaintext: true
      };

      const client = new FlightSQLClient(config);
      expect(client).toBeInstanceOf(FlightSQLClient);
    });

    it('should support authentication config', () => {
      const config: FlightSQLClientConfig = {
        host: 'localhost',
        port: 4317,
        plaintext: true,
        username: 'test',
        password: 'test'
      };

      const client = new FlightSQLClient(config);
      expect(client).toBeInstanceOf(FlightSQLClient);
    });

    it('should inherit from FlightClient', () => {
      const config: FlightSQLClientConfig = {
        host: 'localhost',
        port: 4317,
        plaintext: true
      };

      const client = new FlightSQLClient(config);
      expect(typeof client.connect).toBe('function');
      expect(typeof client.close).toBe('function');
    });
  });

  describe('packCommand', () => {
    let client: FlightSQLClient;

    beforeEach(() => {
      client = new FlightSQLClient({
        host: 'localhost',
        port: 4317,
        plaintext: true
      });
    });

    afterEach(async () => {
      await client.close();
    });

    it('should pack command with Any wrapper', () => {
      // Access private method for testing
      const packCommand = (client as any).packCommand.bind(client);

      const mockCommand = {
        serializeBinary: jest.fn().mockReturnValue(new Uint8Array([1, 2, 3]))
      };

      const result = packCommand(mockCommand, 'test.type.url');

      expect(result).toBeInstanceOf(Uint8Array);
      expect(result.length).toBeGreaterThan(0);
      expect(mockCommand.serializeBinary).toHaveBeenCalled();
    });
  });

  describe('SQL operations', () => {
    let client: FlightSQLClient;

    beforeEach(() => {
      client = new FlightSQLClient({
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

    describe('execute method', () => {
      it('should call executeQuery and transform results', async () => {
        const mockQueryResult = {
          schema: null,
          batches: [] as any[],
          recordCount: 0
        };

        jest.spyOn(client, 'executeQuery').mockResolvedValue(mockQueryResult);
        jest.spyOn(require('../src/utils'), 'arrowToJsonRows').mockReturnValue([{ test: 'value' }]);

        const result = await client.execute('SELECT 1');

        expect(client.executeQuery).toHaveBeenCalledWith('SELECT 1');
        expect(Array.isArray(result)).toBe(true);
      });
    });

    describe('prepared statements', () => {
      it('should create prepared statement structure', async () => {
        // Mock the doAction method to simulate server response
        jest.spyOn(client as any, 'doAction').mockResolvedValue([{
          getBody_asU8: jest.fn().mockReturnValue(new Uint8Array([1, 2, 3, 4]))
        }]);

        const prepared = await client.prepare('SELECT * FROM table WHERE id = ?');

        expect(prepared).toHaveProperty('handle');
        expect(prepared).toHaveProperty('parameterSchema');
        expect(prepared).toHaveProperty('resultSchema');
        expect(prepared.handle).toBeInstanceOf(Uint8Array);
      });

      it('should handle prepare statement errors', async () => {
        jest.spyOn(client as any, 'doAction').mockRejectedValue(new Error('Server error'));

        await expect(client.prepare('INVALID SQL')).rejects.toThrow(FlightSQLError);
      });

      it('should handle empty prepare results', async () => {
        jest.spyOn(client as any, 'doAction').mockResolvedValue([]);

        await expect(client.prepare('SELECT 1')).rejects.toThrow('No results returned from prepare statement');
      });
    });

    describe('metadata operations', () => {
      it('should handle empty catalogs result', async () => {
        jest.spyOn(client as any, 'getFlightInfo').mockResolvedValue({
          getEndpointList: jest.fn().mockReturnValue([])
        });

        const catalogs = await client.getCatalogs();
        expect(catalogs).toEqual([]);
      });

      it('should handle empty schemas result', async () => {
        jest.spyOn(client as any, 'getFlightInfo').mockResolvedValue({
          getEndpointList: jest.fn().mockReturnValue([])
        });

        const schemas = await client.getSchemas();
        expect(schemas).toEqual([]);
      });

      it('should handle catalogs with data', async () => {
        const mockTicket = { mock: 'ticket' };
        const mockEndpoint = {
          getTicket: jest.fn().mockReturnValue(mockTicket)
        };

        jest.spyOn(client as any, 'getFlightInfo').mockResolvedValue({
          getEndpointList: jest.fn().mockReturnValue([mockEndpoint])
        });

        jest.spyOn(client as any, 'doGet').mockResolvedValue({
          batches: [{
            numRows: 2,
            schema: { fields: [{ name: 'catalog_name' }] }
          }]
        });

        // Mock arrowToJsonRows utility
        const mockArrowToJsonRows = jest.fn().mockReturnValue([
          { catalog_name: 'catalog1' },
          { catalog_name: 'catalog2' }
        ]);

        // Replace the import temporarily for this test
        const utils = require('../src/utils');
        const originalFn = utils.arrowToJsonRows;
        utils.arrowToJsonRows = mockArrowToJsonRows;

        const catalogs = await client.getCatalogs();

        expect(catalogs).toEqual(['catalog1', 'catalog2']);

        // Restore original function
        utils.arrowToJsonRows = originalFn;
      });
    });
  });
});