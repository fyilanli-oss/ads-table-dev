'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { dbToCanonicalRow } = require('../funnel-core/supabase-dataset-repository');

const UUID = /\b[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\b/i;
const ALLOWED_RESULT_KEYS = Object.freeze([
  'operation_code', 'inserted_count', 'contract_match_count', 'read_back_count',
  'v1_unchanged', 'snapshot_unchanged', 'oauth_unchanged', 'tokens_unchanged',
  'passed', 'redacted_physical'
]);
const BLOCKS = Object.freeze(['identity', 'entity', 'raw_metrics', 'metric_support', 'currency', 'time', 'provenance']);

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function normalizeNumber(value) {
  if (value === null) return null;
  if (typeof value === 'number') return Number.isFinite(value) ? value : value;
  if (typeof value === 'string' && /^-?(?:\d+|\d+\.\d+)$/.test(value)) return Number(value);
  return value;
}

function normalize(value) {
  if (Array.isArray(value)) return value.map(normalize);
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.keys(value).sort().map((key) => [key, normalize(value[key])]));
  }
  return normalizeNumber(value);
}

function canonicalBlocks(row) {
  return Object.fromEntries(BLOCKS.map((block) => [block, normalize(row[block])]));
}

function buildEvidence(operationResult, expectedCanonical) {
  assert(operationResult && typeof operationResult === 'object' && !Array.isArray(operationResult), 'operation result must be one object');
  assert(!UUID.test(JSON.stringify(operationResult)), 'identity material is forbidden');
  assert(JSON.stringify(Object.keys(operationResult).sort()) === JSON.stringify([...ALLOWED_RESULT_KEYS].sort()), 'operation result fields do not match allowlist');
  assert(operationResult.operation_code === 'E2_T3_TRANSACTION', 'unexpected operation code');
  assert(operationResult.inserted_count === 1, 'insert affected count must equal one');
  assert(operationResult.read_back_count === 1, 'read-back count must equal one');
  assert(operationResult.contract_match_count === 1 && operationResult.passed === true, 'SQL contract assertion failed');
  for (const key of ['v1_unchanged', 'snapshot_unchanged', 'oauth_unchanged', 'tokens_unchanged']) assert(operationResult[key] === true, `${key} must be true`);
  assert(Array.isArray(operationResult.redacted_physical) && operationResult.redacted_physical.length === 1, 'one redacted physical record is required');

  const db = { ...operationResult.redacted_physical[0], user_id: 'e2_t3_runtime_user' };
  const actual = canonicalBlocks(dbToCanonicalRow(db));
  const expected = canonicalBlocks(expectedCanonical);
  assert(JSON.stringify(actual) === JSON.stringify(expected), 'canonical round-trip semantic mismatch');

  return {
    evidence_version: 'e2-t3-roundtrip-v1',
    run_id: 'e2_t3_static_v1',
    operation_status: 'PASS',
    inserted_count: 1,
    read_back_count: 1,
    rollback_required: true,
    blocks: BLOCKS.map((field) => ({ field, result: 'PASS' }))
  };
}

function main(argv) {
  assert(argv.length === 1, 'usage: node scripts/e2-t3-roundtrip-evidence.js <redacted-operation-result.json>');
  const result = JSON.parse(fs.readFileSync(path.resolve(argv[0]), 'utf8'));
  const expected = JSON.parse(fs.readFileSync(path.join(__dirname, '../artifacts/dataset-v2-acceptance/e2-t3-roundtrip/expected-canonical.json'), 'utf8'));
  process.stdout.write(`${JSON.stringify(buildEvidence(result, expected), null, 2)}\n`);
}

if (require.main === module) {
  try { main(process.argv.slice(2)); }
  catch (error) { process.stderr.write(`${error.message}\n`); process.exitCode = 1; }
}

module.exports = Object.freeze({ ALLOWED_RESULT_KEYS, BLOCKS, normalize, canonicalBlocks, buildEvidence });
