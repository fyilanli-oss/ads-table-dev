"use strict";

const assert = require("node:assert/strict");
const path = require("node:path");
const { test } = require("node:test");
const {
  loadRuntimeConfig,
  parsePort,
} = require("../src/config/runtime-config");

const rootDirectory = path.resolve(__dirname, "..");

test("loads one immutable runtime boundary with safe defaults", () => {
  const production = Object.freeze({ production: false, tiktokTestPageEnabled: false });
  const calls = [];
  const config = loadRuntimeConfig({
    env: {},
    rootDirectory,
    logger: { error() {} },
    loadSecurityConfig: (env, logger) => {
      calls.push({ env, logger });
      return production;
    },
  });

  assert.deepEqual(config, {
    port: 3000,
    publicDirectory: path.join(rootDirectory, "public"),
    production,
  });
  assert.equal(Object.isFrozen(config), true);
  assert.equal(calls.length, 1);
});

test("normalizes valid PORT input to a number", () => {
  assert.equal(parsePort(undefined), 3000);
  assert.equal(parsePort("0"), 0);
  assert.equal(parsePort("443"), 443);
  assert.equal(parsePort(65535), 65535);
});

test("rejects invalid runtime configuration before app construction", () => {
  for (const value of ["3.5", " 3000", "3000 ", "abc", -1, 65536]) {
    assert.throws(() => parsePort(value), /PORT must/);
  }
  assert.throws(() => loadRuntimeConfig(), /rootDirectory must be an absolute path/);
  assert.throws(
    () => loadRuntimeConfig({ env: null, rootDirectory }),
    /env must be an object/,
  );
  assert.throws(
    () => loadRuntimeConfig({ rootDirectory: "." }),
    /rootDirectory must be an absolute path/,
  );
  assert.throws(
    () => loadRuntimeConfig({ rootDirectory, loadSecurityConfig: null }),
    /loadSecurityConfig must be a function/,
  );
  assert.throws(
    () => loadRuntimeConfig({ rootDirectory, loadSecurityConfig: () => null }),
    /loadSecurityConfig must return an object/,
  );
});

test("does not expose mutable security config through the runtime boundary", () => {
  const production = { production: false, tiktokTestPageEnabled: false };
  const config = loadRuntimeConfig({
    env: {},
    rootDirectory,
    loadSecurityConfig: () => production,
  });

  assert.notEqual(config.production, production);
  assert.deepEqual(config.production, production);
  assert.equal(Object.isFrozen(config.production), true);
});

test("preserves the existing production security validator", () => {
  assert.throws(
    () =>
      loadRuntimeConfig({
        env: { NODE_ENV: "production", TIKTOK_SANDBOX_ENABLED: "true" },
        rootDirectory,
        logger: { error() {} },
      }),
    (error) => error && error.code === "UNSAFE_PRODUCTION_CONFIG",
  );
});
