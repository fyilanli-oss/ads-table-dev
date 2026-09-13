'use strict';

const fs = require('node:fs');
const path = require('node:path');

function normalize(value) { return value.replaceAll('\\', '/'); }
function runtimeModules(root, roots) {
  const modules = [];
  function visit(directory) {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const target = path.join(directory, entry.name);
      if (entry.isDirectory()) { if (entry.name !== 'tests') visit(target); }
      else if (entry.isFile() && entry.name.endsWith('.js')) modules.push({ path: normalize(path.relative(root, target)), source: fs.readFileSync(target, 'utf8') });
    }
  }
  for (const item of roots) visit(path.join(root, item));
  return modules;
}

function evaluateCanonicalBoundaries({ policy, modules = [] } = {}) {
  if (!policy || typeof policy !== 'object') throw new TypeError('canonical boundary policy is required');
  if (!Array.isArray(modules)) throw new TypeError('modules must be an array');
  const violations = [];
  const tableAllowed = new Set(policy.dataset_table_allowlist || []);
  const upsertAllowed = new Set(policy.canonical_upsert_allowlist || []);
  const mathFiles = new Set(policy.business_math_files || []);
  for (const module of modules) {
    const file = normalize(module.path);
    const source = String(module.source || '');
    if (source.includes(policy.dataset_table) && !tableAllowed.has(file)) violations.push(`DIRECT_DATASET_V2_ACCESS:${file}`);
    if (/\.upsertCanonicalRawFacts\s*\(/.test(source) && !upsertAllowed.has(file)) violations.push(`CANONICAL_WRITE_BYPASS:${file}`);
    if (mathFiles.has(file) && /require\(["'][^"']*(?:providers?|adapters?)[^"']*["']\)/.test(source)) violations.push(`PROVIDER_IMPORT_IN_BUSINESS_MATH:${file}`);
  }
  for (const required of [...tableAllowed, ...upsertAllowed, ...mathFiles]) if (!modules.some(module => normalize(module.path) === required)) violations.push(`MISSING_BOUNDARY_FILE:${required}`);
  return Object.freeze({ ok: violations.length === 0, checked_modules: modules.length, violations: Object.freeze([...new Set(violations)].sort()) });
}

function runCanonicalBoundaryGuard(root = path.join(__dirname, '..')) {
  const policy = JSON.parse(fs.readFileSync(path.join(root, 'security/e3-canonical-boundary-policy.json'), 'utf8'));
  return evaluateCanonicalBoundaries({ policy, modules: runtimeModules(root, policy.runtime_roots) });
}

if (require.main === module) {
  const result = runCanonicalBoundaryGuard();
  process.stdout.write(`${JSON.stringify(result)}\n`);
  if (!result.ok) process.exitCode = 1;
}

module.exports = { evaluateCanonicalBoundaries, runCanonicalBoundaryGuard };
