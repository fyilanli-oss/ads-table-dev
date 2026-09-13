'use strict';

const contract = require('../artifacts/dataset-v2-acceptance/e2-t7-cleanup/no-change-contract.json');
const ROW_FIELDS = Object.freeze(['check_code', 'actual_count', 'expected_count', 'comparison', 'passed']);
const OUTPUT_FIELDS = Object.freeze(['evidence_version','status','check_count','passed_count','failed_count','fixture_residue_count','dataset_v2_unchanged','dataset_v1_unchanged','snapshots_unchanged','security_postconditions_unchanged','rollback_cleanup_verified']);
const CODES = Object.freeze(contract.checks.map(({check_code}) => check_code));
const FIXTURES = Object.freeze(['E2_T3_RESIDUE','E2_T4_RESIDUE','E2_T5_RESIDUE','E2_T6_RESIDUE','TOTAL_E2_RESIDUE']);
const PARITY = Object.freeze(CODES.filter(code => !FIXTURES.includes(code) && code !== 'PERSISTENT_EVIDENCE_OBJECTS'));
const LEAK = /postgres(?:ql)?:\/\/|authorization\s*:|bearer\s+|-----BEGIN [A-Z ]*PRIVATE KEY-----|\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b|[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}|[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}|\b(?:user_id|entity_key|account_id|raw_row)\b/i;
function fail(code) { throw new Error(`E2_T7_EVIDENCE_REJECTED:${code}`); }
function validateRows(value, label) {
  if (!Array.isArray(value) || value.length !== CODES.length) fail(`${label}_CHECK_COUNT`);
  const seen = new Map();
  for (const row of value) {
    if (!row || Object.getPrototypeOf(row) !== Object.prototype) fail(`${label}_ROW_TYPE`);
    if (JSON.stringify(Object.keys(row).sort()) !== JSON.stringify([...ROW_FIELDS].sort())) fail(`${label}_FIELDS`);
    if (!CODES.includes(row.check_code) || seen.has(row.check_code)) fail(`${label}_CHECK_CODE`);
    if (!Number.isSafeInteger(row.actual_count) || row.actual_count < 0 || !Number.isSafeInteger(row.expected_count) || row.expected_count < 0) fail(`${label}_COUNT`);
    if (!['eq','capture'].includes(row.comparison) || typeof row.passed !== 'boolean') fail(`${label}_VALUE`);
    const computed = row.comparison === 'capture' || row.actual_count === row.expected_count;
    if (row.passed !== computed) fail(`${label}_PASS_CLAIM`);
    seen.set(row.check_code, row);
  }
  for (const code of CODES) if (!seen.has(code)) fail(`${label}_MISSING`);
  for (const code of ['CONNECTED_CONNECTIONS','ENCRYPTED_TOKEN_ROWS']) {
    const row = seen.get(code);
    if (row.comparison !== (label === 'BASELINE' ? 'capture' : 'eq')) fail(`${label}_PROVIDER_BASELINE_MODE`);
  }
  for (const code of ['MISSING_ENCRYPTED','ORPHAN_ENCRYPTED','PLAINTEXT_TOKENS']) {
    const row = seen.get(code);
    if (row.comparison !== 'eq' || row.actual_count !== 0 || row.expected_count !== 0 || row.passed !== true) fail(`${label}_TOKEN_INTEGRITY`);
  }
  const parity = seen.get('CONNECTION_TOKEN_PARITY');
  if (parity.comparison !== 'eq' || parity.actual_count !== 1 || parity.expected_count !== 1 || parity.passed !== true) fail(`${label}_PROVIDER_PARITY`);
  return seen;
}
function buildEvidence(baselineInput, finalInput) {
  let serialized;
  try { serialized = JSON.stringify([baselineInput, finalInput]); } catch { fail('UNSERIALIZABLE'); }
  if (LEAK.test(serialized)) fail('SENSITIVE_PATTERN');
  const baseline = validateRows(baselineInput, 'BASELINE');
  const final = validateRows(finalInput, 'FINAL');
  let fixtureResidue = 0;
  for (const code of FIXTURES) {
    fixtureResidue += final.get(code).actual_count;
    if (baseline.get(code).actual_count !== 0 || final.get(code).actual_count !== 0) fail('FIXTURE_RESIDUE_STOP');
  }
  if (baseline.get('PERSISTENT_EVIDENCE_OBJECTS').actual_count !== 0 || final.get('PERSISTENT_EVIDENCE_OBJECTS').actual_count !== 0) fail('PERSISTENT_OBJECT_STOP');
  for (const {check_code, required} of contract.checks) {
    if (required && (!baseline.get(check_code).passed || !final.get(check_code).passed)) fail('REQUIRED_CHECK_FAILED');
  }
  for (const code of PARITY) if (baseline.get(code).actual_count !== final.get(code).actual_count) fail('NO_CHANGE_PARITY');
  const output = {
    evidence_version: contract.evidence_version, status: 'PASS', check_count: CODES.length,
    passed_count: CODES.length, failed_count: 0, fixture_residue_count: fixtureResidue,
    dataset_v2_unchanged: true, dataset_v1_unchanged: true, snapshots_unchanged: true,
    security_postconditions_unchanged: true, rollback_cleanup_verified: true
  };
  if (JSON.stringify(Object.keys(output)) !== JSON.stringify(OUTPUT_FIELDS)) fail('OUTPUT_FIELDS');
  return Object.freeze(output);
}
module.exports = Object.freeze({ROW_FIELDS, OUTPUT_FIELDS, CHECK_CODES:CODES, buildEvidence});
if (require.main === module) {
  process.stderr.write('Library-only converter: import buildEvidence with separate baseline and final result arrays.\n');
  process.exitCode = 2;
}
