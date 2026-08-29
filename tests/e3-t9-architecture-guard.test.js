"use strict";
const assert = require("node:assert/strict");
const { test } = require("node:test");
const { evaluateArchitecture, runArchitectureGuard } = require("../security/architecture-guard");

const baseline = { server: { max_lines: 2, max_named_functions: 0, max_async_functions: 0, max_route_registrations: 0 } };

test("repository satisfies the frozen E3 architecture baseline", () => {
  const result = runArchitectureGuard();
  assert.equal(result.ok, true, result.violations.join(","));
  assert.deepEqual(result.actual, { lines: 5271, named_functions: 237, async_functions: 83, route_registrations: 89 });
});

test("rejects root server growth for every guarded responsibility metric", () => {
  const result = evaluateArchitecture({ serverSource: "function added() {}\napp.get('/new', added);\nasync function work() {}\n", baseline });
  assert.equal(result.ok, false);
  assert.deepEqual(result.violations, ["SERVER_GROWTH:lines:3>2", "SERVER_GROWTH:named_functions:2>0", "SERVER_GROWTH:async_functions:1>0", "SERVER_GROWTH:route_registrations:1>0"]);
});

test("rejects extracted modules that import the root monolith", () => {
  const result = evaluateArchitecture({ serverSource: "\n", baseline, modules: [{ path: "src/jobs/bad.js", source: "const server=require('../../server');" }] });
  assert.deepEqual(result.violations, ["ROOT_IMPORT:src/jobs/bad.js"]);
});

test("fails closed for invalid inputs and baseline limits", () => {
  assert.throws(() => evaluateArchitecture(), /serverSource/);
  assert.throws(() => evaluateArchitecture({ serverSource: "" }), /baseline/);
  const result = evaluateArchitecture({ serverSource: "", baseline: { server: { ...baseline.server, max_lines: -1 } } });
  assert.deepEqual(result.violations, ["INVALID_LIMIT:lines"]);
});
