import {Schema, Table} from 'apache-arrow';
import {FlightClient} from './flight-client';
import {FlightDescriptor} from './generated/proto/Flight_pb';
import {
  ActionClosePreparedStatementRequest,
  ActionCreatePreparedStatementRequest,
  CommandGetCatalogs,
  CommandGetDbSchemas,
  CommandGetImportedKeys,
  CommandGetPrimaryKeys,
  CommandGetSqlInfo,
  CommandGetTables,
  CommandGetTableTypes,
  CommandPreparedStatementQuery,
  CommandStatementQuery
} from './generated/proto/FlightSql_pb';
import {Any} from 'google-protobuf/google/protobuf/any_pb';
import {FlightSQLClientConfig, PreparedStatement, SqlInfoValue, TableMetadata} from './types';
import {FlightError, FlightSQLError} from './errors';

/**
 * FlightSQL client implementation extending the base Flight client.
 * Provides SQL-specific operations on top of Apache Arrow Flight protocol.
 */
export class FlightSQLClient extends FlightClient {
  // FlightSQL command type URLs for Any wrapper
  private static readonly TYPE_URLS = {
    COMMAND_STATEMENT_QUERY: 'type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery',
    COMMAND_PREPARED_STATEMENT_QUERY: 'type.googleapis.com/arrow.flight.protocol.sql.CommandPreparedStatementQuery',
    COMMAND_GET_SQL_INFO: 'type.googleapis.com/arrow.flight.protocol.sql.CommandGetSqlInfo',
    COMMAND_GET_CATALOGS: 'type.googleapis.com/arrow.flight.protocol.sql.CommandGetCatalogs',
    COMMAND_GET_DB_SCHEMAS: 'type.googleapis.com/arrow.flight.protocol.sql.CommandGetDbSchemas',
    COMMAND_GET_TABLES: 'type.googleapis.com/arrow.flight.protocol.sql.CommandGetTables',
    COMMAND_GET_TABLE_TYPES: 'type.googleapis.com/arrow.flight.protocol.sql.CommandGetTableTypes',
    COMMAND_GET_PRIMARY_KEYS: 'type.googleapis.com/arrow.flight.protocol.sql.CommandGetPrimaryKeys',
    COMMAND_GET_IMPORTED_KEYS: 'type.googleapis.com/arrow.flight.protocol.sql.CommandGetImportedKeys',
    ACTION_CREATE_PREPARED_STATEMENT: 'type.googleapis.com/arrow.flight.protocol.sql.ActionCreatePreparedStatementRequest',
    ACTION_CLOSE_PREPARED_STATEMENT: 'type.googleapis.com/arrow.flight.protocol.sql.ActionClosePreparedStatementRequest'
  };

  constructor(config: FlightSQLClientConfig) {
    super(config);
  }

  /**
   * Wraps a FlightSQL command in a google.protobuf.Any message.
   * This is required by the FlightSQL specification for proper command serialization.
   */
  private packCommand(command: any, typeUrl: string): Uint8Array {
    const any = new Any();
    any.setTypeUrl(typeUrl);
    any.setValue(command.serializeBinary());
    return any.serializeBinary();
  }

  /**
   * Executes a SQL query and returns the raw Arrow result.
   * Provides access to schema information and Arrow batches.
   */
  async execute(query: string): Promise<Table> {
    try {
      const command = new CommandStatementQuery();
      command.setQuery(query);

      const descriptor = this.createCommandDescriptor(command, FlightSQLClient.TYPE_URLS.COMMAND_STATEMENT_QUERY);
      const ticket = await this.getQueryTicket(descriptor);

      return await this.doGet(ticket);
    } catch (error) {
      if (error instanceof FlightError) {
        throw error;
      }
      throw new FlightSQLError(`Failed to execute query: ${error}`);
    }
  }

  /**
   * Creates a FlightDescriptor for a FlightSQL command.
   */
  private createCommandDescriptor(command: any, typeUrl: string): FlightDescriptor {
    const descriptor = new FlightDescriptor();
    descriptor.setType(FlightDescriptor.DescriptorType.CMD);
    descriptor.setCmd(this.packCommand(command, typeUrl));
    return descriptor;
  }

  /**
   * Gets a ticket from a FlightDescriptor for query execution.
   */
  private async getQueryTicket(descriptor: FlightDescriptor) {
    const flightInfo = await this.getFlightInfo(descriptor);
    const endpoints = flightInfo.getEndpointList();

    if (endpoints.length === 0) {
      throw new FlightSQLError('No endpoints returned from query');
    }

    const ticket = endpoints[0].getTicket();
    if (!ticket) {
      throw new FlightSQLError('No ticket returned from endpoint');
    }

    return ticket;
  }

  async getQuerySchema(query: string): Promise<Schema> {
    try {
      const command = new CommandStatementQuery();
      command.setQuery(query);

      const descriptor = new FlightDescriptor();
      descriptor.setType(FlightDescriptor.DescriptorType.CMD);
      descriptor.setCmd(command.serializeBinary());

      return await this.getSchema(descriptor);
    } catch (error) {
      throw new FlightSQLError(`Failed to get query schema: ${error}`);
    }
  }

  async prepare(query: string): Promise<PreparedStatement> {
    try {
      const request = new ActionCreatePreparedStatementRequest();
      request.setQuery(query);

      const action = new FlightDescriptor();
      action.setType(FlightDescriptor.DescriptorType.CMD);

      const results = await this.doAction(action as any);

      if (results.length === 0) {
        throw new FlightSQLError('No results returned from prepare statement');
      }

      const handle = results[0].getBody_asU8();
      return {
        handle,
        parameterSchema: undefined,
        resultSchema: undefined
      };
    } catch (error) {
      throw new FlightSQLError(`Failed to prepare statement: ${error}`);
    }
  }

  async executePrepared(prepared: PreparedStatement): Promise<any[]> {
    try {
      const command = new CommandPreparedStatementQuery();
      command.setPreparedStatementHandle(prepared.handle);

      const descriptor = new FlightDescriptor();
      descriptor.setType(FlightDescriptor.DescriptorType.CMD);
      descriptor.setCmd(command.serializeBinary());

      const flightInfo = await this.getFlightInfo(descriptor);
      const endpoints = flightInfo.getEndpointList();

      if (endpoints.length === 0) {
        throw new FlightSQLError('No endpoints returned from prepared query');
      }

      const ticket = endpoints[0].getTicket();
      if (!ticket) {
        throw new FlightSQLError('No ticket returned from endpoint');
      }

      const table = await this.doGet(ticket);
      return table.toArray();
    } catch (error) {
      throw new FlightSQLError(`Failed to execute prepared statement: ${error}`);
    }
  }

  async closePrepared(prepared: PreparedStatement): Promise<void> {
    try {
      const request = new ActionClosePreparedStatementRequest();
      request.setPreparedStatementHandle(prepared.handle);

      const action = new FlightDescriptor();
      action.setType(FlightDescriptor.DescriptorType.CMD);

      await this.doAction(action as any);
    } catch (error) {
      throw new FlightSQLError(`Failed to close prepared statement: ${error}`);
    }
  }

  /**
   * Gets SQL metadata information from the server.
   * Returns a Map of SqlInfo ID to value for each requested ID.
   * @param infoIds - Array of SqlInfo IDs to request. If empty, returns all available info.
   */
  async getSqlInfo(infoIds: number[] = []): Promise<Map<number, SqlInfoValue>> {
    try {
      const command = new CommandGetSqlInfo();
      if (infoIds.length > 0) {
        command.setInfoList(infoIds);
      }

      const descriptor = this.createCommandDescriptor(command, FlightSQLClient.TYPE_URLS.COMMAND_GET_SQL_INFO);
      const flightInfo = await this.getFlightInfo(descriptor);
      const endpoints = flightInfo.getEndpointList();

      if (endpoints.length === 0) {
        return new Map();
      }

      const ticket = endpoints[0].getTicket();
      if (!ticket) {
        return new Map();
      }

      const table = await this.doGet(ticket);
      return this.parseSqlInfoTable(table);
    } catch (error) {
      if (error instanceof FlightError) {
        throw error;
      }
      throw new FlightSQLError(`Failed to get SQL info: ${error}`);
    }
  }

  /**
   * Parses a SqlInfo response table into a Map of info_name -> value.
   * The table schema is: info_name (uint32), value (dense_union).
   * Dense union children: 0=string_value, 1=bool_value, 2=bigint_value,
   * 3=int32_bitmask, 4=string_list, 5=int32_to_int32_list_map
   */
  private parseSqlInfoTable(table: Table): Map<number, SqlInfoValue> {
    const result = new Map<number, SqlInfoValue>();
    const infoNameVector = table.getChild('info_name');
    const valueVector = table.getChild('value');

    if (!infoNameVector || !valueVector) {
      return result;
    }

    for (let i = 0; i < table.numRows; i++) {
      const infoName = infoNameVector.get(i) as number;
      const value = valueVector.get(i);

      // The dense union's .get() returns the unwrapped value for primitive types
      // For strings it returns a string, for bools a boolean, for ints a number/bigint
      if (value === null || value === undefined) {
        result.set(infoName, null);
      } else if (typeof value === 'string' || typeof value === 'boolean' ||
                 typeof value === 'number' || typeof value === 'bigint') {
        result.set(infoName, value);
      } else if (Array.isArray(value)) {
        // string_list: array of strings
        result.set(infoName, value.map(String));
      } else {
        // int32_to_int32_list_map or other complex types - convert to string representation
        result.set(infoName, String(value));
      }
    }

    return result;
  }

  async getCatalogs(): Promise<string[]> {
    try {
      const command = new CommandGetCatalogs();

      const descriptor = this.createCommandDescriptor(command, FlightSQLClient.TYPE_URLS.COMMAND_GET_CATALOGS);
      const flightInfo = await this.getFlightInfo(descriptor);
      const endpoints = flightInfo.getEndpointList();

      if (endpoints.length === 0) {
        return [];
      }

      const ticket = endpoints[0].getTicket();
      if (!ticket) {
        return [];
      }

      const table = await this.doGet(ticket);
      const rows = table.toArray();
      return rows.map(row => row.catalog_name);
    } catch (error) {
      throw new FlightSQLError(`Failed to get catalogs: ${error}`);
    }
  }

  async getSchemas(catalog?: string): Promise<Array<{ catalog: string; schema: string }>> {
    try {
      const command = new CommandGetDbSchemas();
      if (catalog) {
        command.setCatalog(catalog);
      }

      const descriptor = this.createCommandDescriptor(command, FlightSQLClient.TYPE_URLS.COMMAND_GET_DB_SCHEMAS);
      const flightInfo = await this.getFlightInfo(descriptor);
      const endpoints = flightInfo.getEndpointList();

      if (endpoints.length === 0) {
        return [];
      }

      const ticket = endpoints[0].getTicket();
      if (!ticket) {
        return [];
      }

      const table = await this.doGet(ticket);
      const rows = table.toArray();
      return rows.map(row => ({
        catalog: row.catalog_name,
        schema: row.db_schema_name
      }));
    } catch (error) {
      throw new FlightSQLError(`Failed to get schemas: ${error}`);
    }
  }

  async getTables(catalog?: string, dbSchema?: string, tableName?: string, tableTypes?: string[]): Promise<Array<{
    catalog: string;
    schema: string;
    tableName: string;
    tableType: string;
  }>> {
    try {
      const command = new CommandGetTables();
      if (catalog) command.setCatalog(catalog);
      if (dbSchema) command.setDbSchemaFilterPattern(dbSchema);
      if (tableName) command.setTableNameFilterPattern(tableName);
      if (tableTypes) command.setTableTypesList(tableTypes);

      const descriptor = this.createCommandDescriptor(command, FlightSQLClient.TYPE_URLS.COMMAND_GET_TABLES);
      const flightInfo = await this.getFlightInfo(descriptor);
      const endpoints = flightInfo.getEndpointList();

      if (endpoints.length === 0) {
        return [];
      }

      const ticket = endpoints[0].getTicket();
      if (!ticket) {
        return [];
      }

      const table = await this.doGet(ticket);
      const rows = table.toArray();
      return rows.map(row => ({
        catalog: row.catalog_name,
        schema: row.db_schema_name,
        tableName: row.table_name,
        tableType: row.table_type
      }));
    } catch (error) {
      throw new FlightSQLError(`Failed to get tables: ${error}`);
    }
  }

  async getTableTypes(): Promise<string[]> {
    try {
      const command = new CommandGetTableTypes();

      const descriptor = this.createCommandDescriptor(command, FlightSQLClient.TYPE_URLS.COMMAND_GET_TABLE_TYPES);
      const flightInfo = await this.getFlightInfo(descriptor);
      const endpoints = flightInfo.getEndpointList();

      if (endpoints.length === 0) {
        return [];
      }

      const ticket = endpoints[0].getTicket();
      if (!ticket) {
        return [];
      }

      const table = await this.doGet(ticket);
      const rows = table.toArray();
      return rows.map(row => row.table_type);
    } catch (error) {
      throw new FlightSQLError(`Failed to get table types: ${error}`);
    }
  }

  async getPrimaryKeys(catalog: string, dbSchema: string, tableName: string): Promise<TableMetadata['primaryKeys']> {
    try {
      const command = new CommandGetPrimaryKeys();
      command.setCatalog(catalog);
      command.setDbSchema(dbSchema);
      command.setTable(tableName);

      const descriptor = new FlightDescriptor();
      descriptor.setType(FlightDescriptor.DescriptorType.CMD);
      descriptor.setCmd(command.serializeBinary());

      const flightInfo = await this.getFlightInfo(descriptor);
      const endpoints = flightInfo.getEndpointList();

      if (endpoints.length === 0) {
        return [];
      }

      const ticket = endpoints[0].getTicket();
      if (!ticket) {
        return [];
      }

      const table = await this.doGet(ticket);
      const rows = table.toArray();
      return rows.map(row => ({
        catalogName: row.catalog_name,
        schemaName: row.db_schema_name,
        tableName: row.table_name,
        columnName: row.column_name,
        keySequence: row.key_sequence
      }));
    } catch (error) {
      throw new FlightSQLError(`Failed to get primary keys: ${error}`);
    }
  }

  async getForeignKeys(catalog: string, dbSchema: string, tableName: string): Promise<TableMetadata['foreignKeys']> {
    try {
      const command = new CommandGetImportedKeys();
      command.setCatalog(catalog);
      command.setDbSchema(dbSchema);
      command.setTable(tableName);

      const descriptor = new FlightDescriptor();
      descriptor.setType(FlightDescriptor.DescriptorType.CMD);
      descriptor.setCmd(command.serializeBinary());

      const flightInfo = await this.getFlightInfo(descriptor);
      const endpoints = flightInfo.getEndpointList();

      if (endpoints.length === 0) {
        return [];
      }

      const ticket = endpoints[0].getTicket();
      if (!ticket) {
        return [];
      }

      const table = await this.doGet(ticket);
      const rows = table.toArray();
      return rows.map(row => ({
        pkCatalogName: row.pk_catalog_name,
        pkSchemaName: row.pk_db_schema_name,
        pkTableName: row.pk_table_name,
        pkColumnName: row.pk_column_name,
        fkCatalogName: row.fk_catalog_name,
        fkSchemaName: row.fk_db_schema_name,
        fkTableName: row.fk_table_name,
        fkColumnName: row.fk_column_name
      }));
    } catch (error) {
      throw new FlightSQLError(`Failed to get foreign keys: ${error}`);
    }
  }
}