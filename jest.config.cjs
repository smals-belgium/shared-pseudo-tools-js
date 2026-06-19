const { createDefaultPreset } = require("ts-jest");

const tsJestTransformCfg = createDefaultPreset().transform;

module.exports = {
  testEnvironment: "node",
  transform: {
    ...tsJestTransformCfg,
    "^.+\\.(ts|js|mjs)$": "ts-jest",
    "^.+\\.ts$": ["ts-jest", { useESM: true }],
  },
  preset: "ts-jest",
  roots: ["<rootDir>/src"],
  testMatch: ["**/*.spec.ts"],
  moduleFileExtensions: ["ts", "js", "json"],
  collectCoverage: true,
  collectCoverageFrom: ["src/**/*.ts", "!src/**/*.spec.ts", "!src/**/index.ts"],
  coverageDirectory: "coverage",
  clearMocks: true,
  restoreMocks: true,
  resetMocks: true,
  fakeTimers: {
    enableGlobally: true,
  },
  setupFilesAfterEnv: ["<rootDir>/jest.setup.ts"],
  extensionsToTreatAsEsm: [".ts"],
  transformIgnorePatterns: [
    "/node_modules/(?!@smals-belgium-shared/pseudo-helper)",
  ],
};
