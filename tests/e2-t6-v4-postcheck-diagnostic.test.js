'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { test } = require('node:test');
const acceptance = require('../operator/e2-t6-v4');
const diagnostic = require('../operator/e2-t6-v4-postcheck-diagnostic');
function rows(failed = 'E2_T6_RESIDUE') { return acceptance.POSTCHECK_CODES.map((check_code) => ({ check_code, passed: check_code !== failed, actual_count: 999, expected_count: 0 })); }
test('emits only allowlisted failed gate codes without production counts', () => {
  assert.deepEqual(diagnostic.buildReport(rows()), { operation: 'e2_t6_rls_v4', status: 'DIAGNOSTIC_COMPLETE', checkedGates: 19, currentPostcheckPass: false, failedGateCodes: ['E2_T6_RESIDUE'], productionCountsExposed: false });
});
test('rejects missing, extra, duplicate, and malformed results while reporting current all-pass', () => {
  assert.throws(() => diagnostic.buildReport(rows().slice(1)), /count/);
  assert.throws(() => diagnostic.buildReport([...rows(), rows()[0]]), /count/);
  const duplicate = rows(); duplicate[1] = { ...duplicate[0] }; assert.throws(() => diagnostic.buildReport(duplicate), /allowlist/);
  const malformed = rows(); malformed[0].passed = 'false'; assert.throws(() => diagnostic.buildReport(malformed), /passed/);
  assert.deepEqual(diagnostic.buildReport(rows('missing')), { operation: 'e2_t6_rls_v4', status: 'DIAGNOSTIC_COMPLETE', checkedGates: 19, currentPostcheckPass: true, failedGateCodes: [], productionCountsExposed: false });
});
test('requires the exact consumed failure outcome and confirmation constant', () => {
  assert.equal(diagnostic.CONFIRMATION, 'E2-T6-V4-POSTCHECK-DIAGNOSTIC');
  assert.deepEqual(diagnostic.validateOutcome({ schemaVersion: 1, operation: 'e2_t6_rls_v4', version: 'e2-t6-rls-v4', code: 'POSTCHECK_FAILED' }).code, 'POSTCHECK_FAILED');
  assert.throws(() => diagnostic.validateOutcome({ schemaVersion: 1, operation: 'e2_t6_rls_v4', version: 'e2-t6-rls-v4', code: 'PASS' }), /POSTCHECK_FAILED/);
});
test('committed V4 outcome is redacted, consumed, and retry-free', () => {
  const evidence = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'artifacts/dataset-v2-acceptance/e2-t6-rls/v4-live-outcome.json'), 'utf8'));
  assert.deepEqual(evidence, { operation:'e2_t6_rls_v4', status:'FAIL_CLOSED', safeCode:'POSTCHECK_FAILED', transactionRequests:1, transactionRetries:0, postcheckRequests:1, postcheckRetries:0, capsuleState:'CONSUMED', productionCountsExposed:false, productionIdentitiesExposed:false });
  assert.doesNotMatch(JSON.stringify(evidence), /(?:user_id|email|token|uuid|https?:|actual_count|expected_count)/i);
});
