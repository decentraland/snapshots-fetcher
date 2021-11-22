module.exports = {
  globals: {
    'ts-jest': {
      tsconfig: 'test/tsconfig.json',
    },
  },
  moduleFileExtensions: ['ts', 'js'],
  transform: {
    '^.+\\.(ts|tsx)$': 'ts-jest',
  },
  testTimeout: 60000,
  coverageDirectory: 'coverage',
  verbose: true,
  testMatch: ['**/*.spec.(ts)'],
  testEnvironment: 'node',
}
