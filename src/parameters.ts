// Conversion of JavaScript parameter values into the single-row Arrow
// Table that ADBC binds to a `?` / `$1` placeholder query.

import {
  Binary,
  Bool,
  Float64,
  Int32,
  Int64,
  Null,
  Table,
  TimestampMillisecond,
  Utf8,
  Vector,
  isArrowTable,
  vectorFromArray,
} from 'apache-arrow';
import { FlightSQLError } from './errors.js';
import { SqlParameters, SqlParameterValue } from './types.js';

const INT32_MIN = -2_147_483_648;
const INT32_MAX = 2_147_483_647;
const INT64_MIN = -(2n ** 63n);
const INT64_MAX = 2n ** 63n - 1n;

/**
 * Converts query parameters into the Arrow Table the ADBC driver binds
 * to the statement.
 *
 * - An array of JS values becomes a one-row table with one column per
 *   positional placeholder (`?` / `$1`, `$2`, ...). Values are mapped to
 *   explicit Arrow types (see {@link SqlParameterValue}) — never
 *   dictionary-encoded — so the server receives plain scalars.
 * - An Arrow `Table` is passed through unchanged. It must contain
 *   exactly one row: GizmoSQL binds one parameter set per execution.
 *
 * Returns `undefined` when there is nothing to bind (no parameters or an
 * empty array), so callers can pass the result straight to ADBC.
 */
export function parametersToTable(params?: SqlParameters): Table | undefined {
  if (params === undefined || params === null) {
    return undefined;
  }
  if (isArrowTable(params)) {
    if (params.numRows !== 1) {
      throw new FlightSQLError(
        `Parameter table must contain exactly one row (got ${params.numRows}): ` +
          'GizmoSQL binds one parameter set per execution. Use bulk ingest for multi-row loads.'
      );
    }
    return params;
  }
  if (!Array.isArray(params)) {
    throw new FlightSQLError('Query parameters must be an array of values or an Arrow Table');
  }
  if (params.length === 0) {
    return undefined;
  }
  const columns: Record<string, Vector> = {};
  for (const [index, value] of params.entries()) {
    columns[`param_${index}`] = parameterVector(value, index);
  }
  return new Table(columns);
}

/** Builds the one-element, explicitly typed Arrow vector for a single parameter. */
function parameterVector(value: SqlParameterValue, index: number): Vector {
  if (value === null || value === undefined) {
    return vectorFromArray([null], new Null());
  }
  switch (typeof value) {
    case 'boolean':
      return vectorFromArray([value], new Bool());
    case 'string':
      return vectorFromArray([value], new Utf8());
    case 'bigint':
      if (value < INT64_MIN || value > INT64_MAX) {
        throw new FlightSQLError(`Parameter ${index} (${value}n) is outside the 64-bit integer range`);
      }
      return vectorFromArray([value], new Int64());
    case 'number':
      // Integers within Number's safe range are sent as Int32/Int64;
      // everything else (fractions, huge magnitudes, NaN/Infinity) as Float64.
      if (Number.isSafeInteger(value)) {
        return value >= INT32_MIN && value <= INT32_MAX
          ? vectorFromArray([value], new Int32())
          : vectorFromArray([BigInt(value)], new Int64());
      }
      return vectorFromArray([value], new Float64());
    default:
      break;
  }
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) {
      throw new FlightSQLError(`Parameter ${index} is an invalid Date`);
    }
    return vectorFromArray([value.getTime()], new TimestampMillisecond());
  }
  if (value instanceof Uint8Array) {
    return vectorFromArray([value], new Binary());
  }
  throw new FlightSQLError(
    `Unsupported parameter type at index ${index}: ${describeValue(value)}. ` +
      'Supported: string, number, bigint, boolean, Date, Uint8Array, null — or pass an Arrow Table.'
  );
}

function describeValue(value: unknown): string {
  if (value === null || typeof value !== 'object') {
    return typeof value;
  }
  const name = (value as { constructor?: { name?: string } }).constructor?.name;
  return name ? `object (${name})` : 'object';
}
