export default {
  preset: 'ts-jest/presets/default-esm',
  extensionsToTreatAsEsm: ['.ts'],
  testEnvironment: 'node',
  roots: ['<rootDir>/src', '<rootDir>/test'],
  testMatch: ['**/__tests__/**/*.ts', '**/?(*.)+(spec|test).ts'],
  moduleNameMapper: {
    // NodeNext source imports carry .js extensions; map them back to the .ts sources.
    '^(\\.{1,2}/.*)\\.js$': '$1',
  },
  transform: {
    '^.+\\.ts$': ['ts-jest', { useESM: true, tsconfig: { module: 'ESNext', moduleResolution: 'Bundler' } }],
  },
  collectCoverageFrom: [
    'src/**/*.ts',
    '!src/**/*.d.ts',
    '!src/generated/**/*',
  ],
  moduleFileExtensions: ['ts', 'js', 'json'],
  testTimeout: 30000,
};