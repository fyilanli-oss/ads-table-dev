'use strict';
const assert = require('node:assert/strict');
const { test } = require('node:test');
const { CanonicalWriteBoundary } = require('../funnel-core/canonical-write-boundary');
const fixtures = require('../funnel-core/fixtures');
const { evaluateCanonicalBoundaries, runCanonicalBoundaryGuard } = require('../security/canonical-boundary-guard');

const policy = { dataset_table: 'performance_dataset_rows_v2', dataset_table_allowlist: ['funnel-core/supabase-dataset-repository.js'], canonical_upsert_allowlist: ['funnel-core/canonical-write-boundary.js'], business_math_files: ['funnel-core/formula-engine.js'] };
const required = [
  { path: 'funnel-core/supabase-dataset-repository.js', source: "const TABLE='performance_dataset_rows_v2';" },
  { path: 'funnel-core/canonical-write-boundary.js', source: 'repository.upsertCanonicalRawFacts(rows)' },
  { path: 'funnel-core/formula-engine.js', source: "require('./canonical-contract')" },
];

test('repository satisfies the E3 canonical boundary policy', () => {
  const result = runCanonicalBoundaryGuard();
  assert.equal(result.ok, true, result.violations.join(','));
  assert.ok(result.checked_modules > 0);
});

test('canonical write boundary validates before one repository delegation', async () => {
  const calls = [];
  const boundary = new CanonicalWriteBoundary({ repository: { async upsertCanonicalRawFacts(rows) { calls.push(rows); return rows; } } });
  const row = fixtures.metaPaid();
  assert.deepEqual(await boundary.write([row]), [row]);
  assert.equal(calls.length, 1);
  const invalid = { campaign_id: 'provider-dto', impressions: 10 };
  await assert.rejects(boundary.write([invalid]), /identity|canonical row/);
  assert.equal(calls.length, 1);
});

test('rejects direct Dataset V2 access and canonical repository bypass', () => {
  const modules = [...required, { path: 'src/providers/meta/bad.js', source: "client.from('performance_dataset_rows_v2').upsertCanonicalRawFacts(rows)" }];
  assert.deepEqual(evaluateCanonicalBoundaries({ policy, modules }).violations, ['CANONICAL_WRITE_BYPASS:src/providers/meta/bad.js', 'DIRECT_DATASET_V2_ACCESS:src/providers/meta/bad.js']);
});

test('rejects provider-specific imports in Formula and Query business math', () => {
  const modules = required.map(module => module.path === 'funnel-core/formula-engine.js' ? { ...module, source: "require('./providers/meta')" } : module);
  assert.deepEqual(evaluateCanonicalBoundaries({ policy, modules }).violations, ['PROVIDER_IMPORT_IN_BUSINESS_MATH:funnel-core/formula-engine.js']);
});

test('fails closed for missing policy, modules, boundary files and write dependencies', async () => {
  assert.throws(() => evaluateCanonicalBoundaries(), /policy/);
  assert.throws(() => evaluateCanonicalBoundaries({ policy, modules: null }), /modules/);
  assert.deepEqual(evaluateCanonicalBoundaries({ policy, modules: [] }).violations, ['MISSING_BOUNDARY_FILE:funnel-core/canonical-write-boundary.js', 'MISSING_BOUNDARY_FILE:funnel-core/formula-engine.js', 'MISSING_BOUNDARY_FILE:funnel-core/supabase-dataset-repository.js']);
  assert.throws(() => new CanonicalWriteBoundary(), /repository/);
  const boundary = new CanonicalWriteBoundary({ repository: { upsertCanonicalRawFacts() {} } });
  await assert.rejects(boundary.write({}), /array/);
});
