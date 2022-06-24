module.exports = {
  extensionsToTreatAsEsm: ['.ts'],
  globals: {
    'ts-jest': {
      tsconfig: 'test/tsconfig.json',
      'useESM': true
    },
  },
  moduleFileExtensions: ['ts', 'js'],
  transform: {
    '^.+\\.(ts|tsx)$': 'ts-jest'
  },
  testTimeout: 60000,
  coverageDirectory: 'coverage',
  verbose: true,
  testMatch: ['**/*.spec.(ts)'],
  testEnvironment: 'node',
}
