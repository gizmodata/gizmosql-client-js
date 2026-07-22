import { FlightSQLClient } from '../src/flightsql-client';
import { FlightSQLClientConfig } from '../src/types';
import { FlightSQLError } from '../src/errors';
import { Action } from '../src/generated/proto/Flight_pb';
import { ActionCreatePreparedStatementResult } from '../src/generated/proto/FlightSql_pb';
import { Any } from 'google-protobuf/google/protobuf/any_pb';

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
        token: 'test'
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

    describe('prepared statements', () => {
      it('should create prepared statement structure', async () => {
        // Mock the doAction method to simulate a spec-compliant server
        // response: an Any-wrapped ActionCreatePreparedStatementResult.
        const result = new ActionCreatePreparedStatementResult();
        result.setPreparedStatementHandle(new Uint8Array([1, 2, 3, 4]));
        const resultAny = new Any();
        resultAny.setTypeUrl('type.googleapis.com/arrow.flight.protocol.sql.ActionCreatePreparedStatementResult');
        resultAny.setValue(result.serializeBinary());
        const doAction = jest.spyOn(client as any, 'doAction').mockResolvedValue([{
          getBody_asU8: jest.fn().mockReturnValue(resultAny.serializeBinary())
        }]);

        const prepared = await client.prepare('SELECT * FROM table WHERE id = ?');

        expect(prepared).toHaveProperty('handle');
        expect(prepared).toHaveProperty('parameterSchema');
        expect(prepared).toHaveProperty('resultSchema');
        expect(prepared.handle).toBeInstanceOf(Uint8Array);
        expect(Array.from(prepared.handle)).toEqual([1, 2, 3, 4]);

        // The request must be a real Flight Action carrying the packed
        // CreatePreparedStatement request (regression: a FlightDescriptor
        // was previously sent, failing gRPC serialization client-side).
        const sentAction = doAction.mock.calls[0][0] as Action;
        expect(sentAction).toBeInstanceOf(Action);
        expect(sentAction.getType()).toBe('CreatePreparedStatement');
        expect(sentAction.getBody_asU8().length).toBeGreaterThan(0);
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

        const mockTable = {
          toArray: jest.fn().mockReturnValue([
            { catalog_name: 'catalog1' },
            { catalog_name: 'catalog2' }
          ])
        };

        jest.spyOn(client as any, 'doGet').mockResolvedValue(mockTable);

        const catalogs = await client.getCatalogs();

        expect(catalogs).toEqual(['catalog1', 'catalog2']);
      });
    });
  });
});