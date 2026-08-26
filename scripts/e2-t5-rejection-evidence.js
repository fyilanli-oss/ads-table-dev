'use strict';

const fs = require('node:fs');
const path = require('node:path');
const matrix = require('../artifacts/dataset-v2-acceptance/e2-t5-rejection/rejection-matrix.json');

const TOP_LEVEL_FIELDS = Object.freeze([
  'operation_code','expected_case_count','evaluated_case_count','passed_case_count','failed_case_count',
  'unexpected_accept_count','residue_count','dataset_unchanged','v1_unchanged','snapshots_unchanged',
  'oauth_unchanged','connected_unchanged','encrypted_unchanged','missing_encrypted_unchanged','orphan_encrypted_unchanged','plaintext_unchanged','ledger_unchanged',
  'overall_passed','cases'
]);
const CASE_FIELDS = Object.freeze([
  'case_code','expected_sqlstate','actual_sqlstate','expected_constraints','actual_constraint',
  'expected_column','actual_column','rejected','passed'
]);
const BLOCKED = /postgres(?:ql)?:\/\/|https?:\/\/|authorization\s*:|bearer\s+|-----BEGIN [A-Z ]*PRIVATE KEY-----|\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b|[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}|[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}|\b(?:password|credential|access_token|refresh_token|secret)\b/i;

function assert(condition, message) { if (!condition) throw new Error(message); }
function exactKeys(value, fields) {
  return value && typeof value === 'object' && !Array.isArray(value)
    && JSON.stringify(Object.keys(value).sort()) === JSON.stringify([...fields].sort());
}
function exactArray(actual, expected) {
  return Array.isArray(actual) && JSON.stringify(actual) === JSON.stringify(expected);
}

function buildEvidence(result) {
  assert(result && typeof result === 'object' && !Array.isArray(result), 'result must be one object');
  assert(!Object.keys(result).some((key) => /(?:connection|token).*count|actual_count/i.test(key)), 'actual provider counts are forbidden');
  assert(exactKeys(result, TOP_LEVEL_FIELDS), 'result fields do not match exact top-level allowlist');
  assert(!BLOCKED.test(JSON.stringify(result)), 'forbidden identity, URI, error, or credential material');
  assert(result.operation_code === 'e2_t5_rejection_v1', 'operation_code mismatch');
  assert(result.expected_case_count === 35 && result.evaluated_case_count === 35, 'case count mismatch');
  assert(Array.isArray(result.cases) && result.cases.length === 35, 'cases must contain exactly 35 entries');
  const byCode = new Map();
  for (const item of result.cases) {
    assert(exactKeys(item, CASE_FIELDS), 'case fields do not match exact allowlist');
    assert(!byCode.has(item.case_code), `duplicate case: ${item.case_code}`);
    byCode.set(item.case_code, item);
  }
  assert(byCode.size === matrix.length, 'missing or extra case');
  for (const expected of matrix) {
    const actual = byCode.get(expected.case_code);
    assert(actual, `missing case: ${expected.case_code}`);
    assert(actual.expected_sqlstate === expected.expected_sqlstate, `${expected.case_code}: expected SQLSTATE drift`);
    assert(exactArray(actual.expected_constraints, expected.expected_constraints), `${expected.case_code}: expected_constraints drift`);
    assert(actual.expected_column === expected.expected_column, `${expected.case_code}: expected column drift`);
    assert(actual.actual_sqlstate === expected.expected_sqlstate, `${expected.case_code}: actual SQLSTATE mismatch`);
    assert(actual.rejected === true && actual.passed === true, `${expected.case_code}: rejection did not pass`);
    if (expected.expected_sqlstate === '23514') {
      assert(typeof actual.actual_constraint === 'string' && actual.actual_constraint.length > 0, `${expected.case_code}: empty CHECK constraint`);
      assert(expected.expected_constraints.includes(actual.actual_constraint), `${expected.case_code}: constraint outside closed allowlist`);
      assert(actual.actual_column === null, `${expected.case_code}: CHECK column must be null`);
    } else {
      assert(actual.actual_constraint === null, `${expected.case_code}: NOT NULL constraint must be null`);
      assert(actual.actual_column === expected.expected_column, `${expected.case_code}: NOT NULL column mismatch`);
    }
  }
  assert(result.passed_case_count === 35 && result.failed_case_count === 0, 'failed case count');
  assert(result.unexpected_accept_count === 0, 'unexpected acceptance');
  assert(result.residue_count === 0, 'fixture residue');
  for (const field of ['dataset_unchanged','v1_unchanged','snapshots_unchanged','oauth_unchanged','connected_unchanged','encrypted_unchanged','missing_encrypted_unchanged','orphan_encrypted_unchanged','plaintext_unchanged','ledger_unchanged','overall_passed']) {
    assert(result[field] === true, `${field} must be true`);
  }
  return Object.freeze({
    evidence_version: 'e2-t5-rejection-v1', operation_code: 'e2_t5_rejection_v1', status: 'PASS',
    expected_case_count: 35, passed_case_count: 35, failed_case_count: 0,
    unexpected_accept_count: 0, residue_count: 0, rollback_required: true
  });
}

function main(argv) {
  assert(argv.length === 1, 'usage: node scripts/e2-t5-rejection-evidence.js <redacted-result.json>');
  const result = JSON.parse(fs.readFileSync(path.resolve(argv[0]), 'utf8'));
  process.stdout.write(`${JSON.stringify(buildEvidence(result), null, 2)}\n`);
}

if (require.main === module) {
  try { main(process.argv.slice(2)); }
  catch (error) { process.stderr.write('E2-T5 evidence validation failed\n'); process.exitCode = 1; }
}

module.exports = Object.freeze({ TOP_LEVEL_FIELDS, CASE_FIELDS, BLOCKED, buildEvidence });
