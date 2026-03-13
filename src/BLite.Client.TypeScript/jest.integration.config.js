// Configuration for integration tests (require a live BLite server on localhost).
// Run with: npm run test:integration
//
// Environment variables:
//   BLITE_HOST      — server hostname (default: localhost)
//   BLITE_PORT      — gRPC port       (default: 2626)
//   BLITE_API_KEY   — API key         (default: dev)
//   BLITE_TLS       — 'true' to use TLS (default: false)

/** @type {import('jest').Config} */
module.exports = {
  testEnvironment: 'node',
  testMatch: ['**/tests/integration/**/*.test.ts'],
  transform: {
    '^.+\\.tsx?$': ['ts-jest', { tsconfig: 'tsconfig.json' }],
  },
  testTimeout: 15000,
  verbose: true,
  forceExit: true,
};
