import baseConfig from './jest.config.js';

// Unit-test config: stubs the native ADBC driver-manager addon (ESM Jest
// cannot parse). Integration tests (npm run test:integration) use the
// base config and the real package.
export default {
  ...baseConfig,
  moduleNameMapper: {
    '^@apache-arrow/adbc-driver-manager$': '<rootDir>/test/stubs/adbc-driver-manager.ts',
  },
};
