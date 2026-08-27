"use strict";

const assert = require("node:assert/strict");
const path = require("node:path");
const { after, test } = require("node:test");
const {
  createApplication,
  startApplication,
} = require("../src/app");

const servers = [];

after(async () => {
  await Promise.all(
    servers.map(
      (server) =>
        new Promise((resolve, reject) =>
          server.close((error) => (error ? reject(error) : resolve())),
        ),
    ),
  );
});

test("creates an Express application without opening a listener", () => {
  const app = createApplication({
    publicDirectory: path.join(__dirname, "..", "public"),
  });

  assert.equal(typeof app, "function");
  assert.equal(app.get("trust proxy"), 1);
  assert.equal(app.listening, undefined);
});

test("starts listening only through the explicit composition-root boundary", async () => {
  const messages = [];
  const app = createApplication({
    publicDirectory: path.join(__dirname, "..", "public"),
  });
  const server = startApplication(app, {
    port: 0,
    logger: { log: (message) => messages.push(message) },
  });
  servers.push(server);

  await new Promise((resolve, reject) => {
    server.once("listening", resolve);
    server.once("error", reject);
  });

  assert.equal(server.listening, true);
  assert.deepEqual(messages, ["AdsTable server running on 0"]);
});

test("fails closed when required composition inputs are missing", () => {
  assert.throws(() => createApplication(), /publicDirectory is required/);
  assert.throws(() => startApplication(null, { port: 3000 }), /application is required/);
  assert.throws(() => startApplication(() => {}, { port: 3000 }), /application is required/);
  assert.throws(
    () => startApplication({ listen() {} }),
    /port is required/,
  );
});

