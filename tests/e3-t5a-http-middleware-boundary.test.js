"use strict";

const assert = require("node:assert/strict");
const http = require("node:http");
const { test } = require("node:test");
const express = require("express");
const { createRequestBoundary, installErrorBoundary } = require("../src/middleware/http-boundary");

function request(app, path, headers = {}) {
  return new Promise((resolve, reject) => {
    const server = app.listen(0, "127.0.0.1", () => {
      const req = http.get({ host: "127.0.0.1", port: server.address().port, path, headers }, (res) => {
        let body = "";
        res.setEncoding("utf8");
        res.on("data", (chunk) => { body += chunk; });
        res.on("end", () => server.close(() => resolve({ res, body })));
      });
      req.on("error", (error) => server.close(() => reject(error)));
    });
  });
}

function memoryLogger() {
  const entries = { info: [], error: [] };
  return { entries, info: (entry) => entries.info.push(entry), error: (entry) => entries.error.push(entry) };
}

test("propagates a safe request ID and emits metadata-only completion logging", async () => {
  const logger = memoryLogger();
  const app = express();
  app.use(createRequestBoundary({ logger, createId: () => "generated-id", clock: (() => { let now = 10; return () => now += 5; })() }));
  app.get("/ok", (req, res) => res.json({ requestId: req.requestId }));

  const { res, body } = await request(app, "/ok?token=must-not-be-logged", { "x-request-id": "client.id-1" });
  assert.equal(res.statusCode, 200);
  assert.equal(res.headers["x-request-id"], "client.id-1");
  assert.deepEqual(JSON.parse(body), { requestId: "client.id-1" });
  assert.deepEqual(logger.entries.info, [{
    event: "http_request_completed",
    requestId: "client.id-1",
    method: "GET",
    path: "/ok",
    status: 200,
    durationMs: 5,
  }]);
  assert.equal(JSON.stringify(logger.entries).includes("must-not-be-logged"), false);
});

test("replaces malformed request IDs instead of reflecting them", async () => {
  const logger = memoryLogger();
  const app = express();
  app.use(createRequestBoundary({ logger, createId: () => "generated-id" }));
  app.get("/ok", (req, res) => res.json({ requestId: req.requestId }));

  const { res, body } = await request(app, "/ok", { "x-request-id": "bad id" });
  assert.equal(res.headers["x-request-id"], "generated-id");
  assert.deepEqual(JSON.parse(body), { requestId: "generated-id" });
});

test("normalizes uncaught errors without leaking internal messages", async () => {
  const logger = memoryLogger();
  const app = express();
  app.use(createRequestBoundary({ logger, createId: () => "error-id" }));
  app.get("/boom", () => { throw new Error("database credential leaked"); });
  installErrorBoundary(app, { logger });

  const { res, body } = await request(app, "/boom");
  assert.equal(res.statusCode, 500);
  assert.deepEqual(JSON.parse(body), {
    error: "Internal server error",
    code: "INTERNAL_ERROR",
    requestId: "error-id",
  });
  assert.equal(JSON.stringify(logger.entries).includes("credential"), false);
  assert.deepEqual(logger.entries.error[0], {
    event: "http_request_failed",
    requestId: "error-id",
    method: "GET",
    path: "/boom",
    status: 500,
    code: "INTERNAL_ERROR",
  });
});

test("fails closed for invalid middleware dependencies", () => {
  assert.throws(() => createRequestBoundary({ logger: {} }), /logger must expose/);
  assert.throws(() => createRequestBoundary({ createId: null }), /createId must be a function/);
  assert.throws(() => createRequestBoundary({ clock: null }), /clock must be a function/);
  assert.throws(() => installErrorBoundary(null), /Express application is required/);
});
