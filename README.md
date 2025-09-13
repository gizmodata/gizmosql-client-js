# flight-sql-client

A TypeScript/JavaScript client for Apache Arrow Flight and Flight SQL protocols.

## Installation

```bash
npm install flight-sql-client
```

## Quick Start

```typescript
import { FlightSQLClient } from 'flight-sql-client';

const client = new FlightSQLClient({
  host: 'localhost',
  port: 4317,
  plaintext: true,
  username: 'your-username',
  password: 'your-password'
});

const result = await client.execute('SELECT * FROM table');
console.log(result);

await client.close();
```

## Dependencies

- `@grpc/grpc-js` - gRPC implementation
- `apache-arrow` - Arrow data format support