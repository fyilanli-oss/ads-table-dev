'use strict';

const fs = require('node:fs');
const path = require('node:path');
const matrix = require('../artifacts/dataset-v2-acceptance/e2-t6-rls/rls-matrix.json');

const TOP_LEVEL_FIELDS = Object.freeze(['operation_code','expected_case_count','evaluated_case_count','passed_case_count','failed_case_count','unexpected_allow_count','fixture_count','residue_count','dataset_baseline_preserved','v1_unchanged','snapshots_unchanged','oauth_unchanged','connected_unchanged','encrypted_unchanged','missing_encrypted_unchanged','orphan_encrypted_unchanged','plaintext_unchanged','ledger_unchanged','overall_passed','cases']);
const CASE_FIELDS = Object.freeze(['case_code','actor','operation','target','expected_outcome','actual_outcome','actual_row_count','actual_sqlstate','passed']);
const BLOCKED = /postgres(?:ql)?:\/\/|https?:\/\/|authorization\s*:|bearer\s+|-----BEGIN [A-Z ]*PRIVATE KEY-----|\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b|[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}|[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}|\b(?:password|credential|access_token|refresh_token|service_role_key|secret|user_id|jwt|error|message|detail|hint|context)\b/i;
const exactKeys = (value, fields) => value && typeof value === 'object' && !Array.isArray(value) && JSON.stringify(Object.keys(value).sort()) === JSON.stringify([...fields].sort());
function assert(ok, message) { if (!ok) throw new Error(message); }

function buildEvidence(result) {
  assert(result && typeof result === 'object' && !Array.isArray(result), 'result must be one object');
  assert(!Object.keys(result).some((key) => /(?:connection|token).*count|actual_count/i.test(key)), 'actual provider counts are forbidden');
  assert(exactKeys(result, TOP_LEVEL_FIELDS), 'top-level allowlist mismatch');
  assert(!BLOCKED.test(JSON.stringify(result)), 'forbidden sensitive or raw material');
  assert(result.operation_code === 'e2_t6_rls_v1', 'operation mismatch');
  assert(result.expected_case_count === 16 && result.evaluated_case_count === 16, 'case count mismatch');
  assert(Array.isArray(result.cases) && result.cases.length === 16, 'cases must contain exactly 16 entries');
  const byCode = new Map();
  for (const actual of result.cases) {
    assert(exactKeys(actual, CASE_FIELDS), 'case allowlist mismatch');
    assert(!byCode.has(actual.case_code), 'duplicate case');
    byCode.set(actual.case_code, actual);
  }
  assert(byCode.size === matrix.length, 'missing or extra case');
  for (const expected of matrix) {
    const actual = byCode.get(expected.case_code);
    assert(actual, 'missing case');
    for (const field of ['actor','operation','target','expected_outcome']) assert(actual[field] === expected[field], `${expected.case_code}: ${field} mismatch`);
    assert(actual.actual_outcome === expected.expected_outcome, `${expected.case_code}: outcome mismatch`);
    assert(actual.actual_row_count === expected.expected_row_count, `${expected.case_code}: row count mismatch`);
    if (expected.expected_sqlstate_class === null) assert(actual.actual_sqlstate === null, `${expected.case_code}: unexpected SQLSTATE`);
    else assert(typeof actual.actual_sqlstate === 'string' && actual.actual_sqlstate.length === 5 && actual.actual_sqlstate.startsWith(expected.expected_sqlstate_class), `${expected.case_code}: SQLSTATE class mismatch`);
    assert(actual.passed === true, `${expected.case_code}: case failed`);
  }
  assert(result.passed_case_count === 16 && result.failed_case_count === 0, 'failed cases');
  assert(result.unexpected_allow_count === 0, 'unexpected allow');
  assert(result.fixture_count === 2 && result.residue_count === 0, 'fixture boundary failed');
  for (const field of ['dataset_baseline_preserved','v1_unchanged','snapshots_unchanged','oauth_unchanged','connected_unchanged','encrypted_unchanged','missing_encrypted_unchanged','orphan_encrypted_unchanged','plaintext_unchanged','ledger_unchanged','overall_passed']) assert(result[field] === true, `${field} must be true`);
  return Object.freeze({ evidence_version:'e2-t6-rls-v1', operation_code:'e2_t6_rls_v1', status:'PASS', expected_case_count:16, passed_case_count:16, failed_case_count:0, unexpected_allow_count:0, residue_count:0, connected_unchanged:true, encrypted_unchanged:true, missing_encrypted_unchanged:true, orphan_encrypted_unchanged:true, plaintext_unchanged:true, rollback_required:true });
}
function main(argv) { assert(argv.length === 1, 'usage: evidence converter requires one redacted result file'); const result=JSON.parse(fs.readFileSync(path.resolve(argv[0]),'utf8')); process.stdout.write(`${JSON.stringify(buildEvidence(result),null,2)}\n`); }
if (require.main === module) { try { main(process.argv.slice(2)); } catch (_) { process.stderr.write('E2-T6 evidence validation failed\n'); process.exitCode=1; } }
module.exports=Object.freeze({TOP_LEVEL_FIELDS,CASE_FIELDS,BLOCKED,buildEvidence});
