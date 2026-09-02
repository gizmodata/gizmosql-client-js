import { Table, vectorFromArray, Int32, Utf8, tableFromArrays } from 'apache-arrow';
import { parametersToTable } from '../src/parameters.js';
import { FlightSQLError } from '../src/errors.js';

// The one-row parameter table handed to ADBC. Column types are asserted
// by their Arrow type name so a change in inference is caught here rather
// than by the server (which cannot type an untyped `?`).
const typeNames = (table: Table) => table.schema.fields.map((f) => f.type.toString());

describe('parametersToTable', () => {
  it('returns undefined when there is nothing to bind', () => {
    expect(parametersToTable(undefined)).toBeUndefined();
    expect(parametersToTable([])).toBeUndefined();
  });

  it('builds a one-row table with one column per positional value', () => {
    const table = parametersToTable([1, 'a'])!;
    expect(table.numRows).toBe(1);
    expect(table.schema.fields.map((f) => f.name)).toEqual(['param_0', 'param_1']);
  });

  it('maps JS values to explicit Arrow types', () => {
    const table = parametersToTable([
      'text',
      42,
      2 ** 40,
      1.5,
      7n,
      true,
      new Date('2024-01-02T03:04:05.678Z'),
      new Uint8Array([1, 2, 3]),
      null,
      undefined,
    ])!;
    expect(typeNames(table)).toEqual([
      'Utf8',
      'Int32',
      'Int64',
      'Float64',
      'Int64',
      'Bool',
      'Timestamp<MILLISECOND>',
      'Binary',
      'Null',
      'Null',
    ]);
  });

  it('never dictionary-encodes strings', () => {
    // tableFromArrays would infer Dictionary<Utf8>, which the server
    // cannot consume as a scalar; we must send plain Utf8.
    expect(typeNames(tableFromArrays({ s: ['x'] }))[0]).toMatch(/^Dictionary/);
    expect(typeNames(parametersToTable(['x'])!)[0]).toBe('Utf8');
  });

  it('round-trips the values it encodes', () => {
    const date = new Date('2024-01-02T03:04:05.678Z');
    const row = parametersToTable(['text', 42, 1.5, 7n, true, date, new Uint8Array([9]), null])!
      .toArray()[0]
      .toJSON();
    expect(row.param_0).toBe('text');
    expect(row.param_1).toBe(42);
    expect(row.param_2).toBe(1.5);
    expect(row.param_3).toBe(7n);
    expect(row.param_4).toBe(true);
    expect(Number(row.param_5)).toBe(date.getTime());
    expect(Array.from(row.param_6)).toEqual([9]);
    expect(row.param_7).toBeNull();
  });

  it('uses Int32 within range and Int64 for larger safe integers', () => {
    expect(typeNames(parametersToTable([2_147_483_647, -2_147_483_648])!)).toEqual(['Int32', 'Int32']);
    expect(typeNames(parametersToTable([2_147_483_648, Number.MAX_SAFE_INTEGER])!)).toEqual(['Int64', 'Int64']);
    expect(typeNames(parametersToTable([2 ** 60])!)).toEqual(['Float64']);
  });

  it('accepts Buffer as binary', () => {
    const table = parametersToTable([Buffer.from('ab')])!;
    expect(typeNames(table)).toEqual(['Binary']);
  });

  it('passes a one-row Arrow Table through unchanged', () => {
    const table = new Table({ id: vectorFromArray([5], new Int32()), name: vectorFromArray(['n'], new Utf8()) });
    expect(parametersToTable(table)).toBe(table);
  });

  it('rejects multi-row and empty Arrow Tables', () => {
    const multi = new Table({ id: vectorFromArray([1, 2], new Int32()) });
    expect(() => parametersToTable(multi)).toThrow(FlightSQLError);
    expect(() => parametersToTable(multi)).toThrow(/exactly one row/);
    const empty = new Table({ id: vectorFromArray([], new Int32()) });
    expect(() => parametersToTable(empty)).toThrow(/exactly one row/);
  });

  it('rejects out-of-range bigints and invalid dates', () => {
    expect(() => parametersToTable([2n ** 63n])).toThrow(/64-bit integer range/);
    expect(() => parametersToTable([-(2n ** 63n) - 1n])).toThrow(/64-bit integer range/);
    expect(() => parametersToTable([new Date('nope')])).toThrow(/invalid Date/);
  });

  it('rejects unsupported value types with a descriptive error', () => {
    expect(() => parametersToTable([{ a: 1 } as any])).toThrow(/Unsupported parameter type at index 0: object \(Object\)/);
    expect(() => parametersToTable([[1, 2] as any])).toThrow(/index 0: object \(Array\)/);
    expect(() => parametersToTable([Symbol('s') as any])).toThrow(/index 0: symbol/);
    expect(() => parametersToTable('not-an-array' as any)).toThrow(/array of values or an Arrow Table/);
  });
});
