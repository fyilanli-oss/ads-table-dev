"use strict";

const fs = require("node:fs");
const path = require("node:path");

const METRICS = Object.freeze({
  lines: source => source.split(/\r?\n/).length - (source.endsWith("\n") ? 1 : 0),
  named_functions: source => (source.match(/\bfunction\s+[A-Za-z_$][\w$]*\s*\(/g) || []).length,
  async_functions: source => (source.match(/\basync function\s+[A-Za-z_$][\w$]*\s*\(/g) || []).length,
  route_registrations: source => (source.match(/\bapp\.(?:get|post|put|patch|delete)\s*\(/g) || []).length,
});

function evaluateArchitecture({ serverSource, baseline, modules = [] } = {}) {
  if (typeof serverSource !== "string") throw new TypeError("serverSource must be a string");
  if (!baseline || typeof baseline !== "object" || !baseline.server) throw new TypeError("architecture baseline is required");
  const actual = Object.fromEntries(Object.entries(METRICS).map(([name, measure]) => [name, measure(serverSource)]));
  const limits = { lines: baseline.server.max_lines, named_functions: baseline.server.max_named_functions, async_functions: baseline.server.max_async_functions, route_registrations: baseline.server.max_route_registrations };
  const violations = [];
  for (const [metric, limit] of Object.entries(limits)) {
    if (!Number.isInteger(limit) || limit < 0) violations.push(`INVALID_LIMIT:${metric}`);
    else if (actual[metric] > limit) violations.push(`SERVER_GROWTH:${metric}:${actual[metric]}>${limit}`);
  }
  for (const module of modules) {
    const normalized = module.path.replaceAll("\\", "/");
    if (/require\(["'][^"']*server(?:\.js)?["']\)/.test(module.source) || /from\s+["'][^"']*server(?:\.js)?["']/.test(module.source)) violations.push(`ROOT_IMPORT:${normalized}`);
  }
  return Object.freeze({ ok: violations.length === 0, actual: Object.freeze(actual), limits: Object.freeze(limits), violations: Object.freeze(violations) });
}

function javascriptModules(root) {
  const modules = [];
  function visit(directory) {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const target = path.join(directory, entry.name);
      if (entry.isDirectory()) visit(target);
      else if (entry.isFile() && entry.name.endsWith(".js")) modules.push({ path: path.relative(root, target), source: fs.readFileSync(target, "utf8") });
    }
  }
  visit(path.join(root, "src"));
  return modules;
}

function runArchitectureGuard(root = path.join(__dirname, "..")) {
  const baseline = JSON.parse(fs.readFileSync(path.join(root, "security/e3-architecture-baseline.json"), "utf8"));
  return evaluateArchitecture({ serverSource: fs.readFileSync(path.join(root, "server.js"), "utf8"), baseline, modules: javascriptModules(root) });
}

if (require.main === module) {
  const result = runArchitectureGuard();
  process.stdout.write(`${JSON.stringify(result)}\n`);
  if (!result.ok) process.exitCode = 1;
}

module.exports = { evaluateArchitecture, runArchitectureGuard };
