'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { test } = require('node:test');
const acceptance = require('../operator/e2-t6-v4');
const diagnostic = require('../operator/e2-t6-v4-postcheck-diagnostic');
function rows(failed = 'E2_T6_RESIDUE') { return acceptance.POSTCHECK_CODES.map((check_code) => ({ check_code, passed: check_code !== failed, actual_count: 999, expected_count: 0 })); }
test('emits only allowlisted failure and shape codes without production values', () => {
  assert.deepEqual(diagnostic.buildReport(rows()), { operation:'e2_t6_rls_v4', status:'DIAGNOSTIC_COMPLETE', checkedGates:19, currentPostcheckPass:false, failedGateCodes:['E2_T6_RESIDUE'], missingGateCodes:[], duplicateGateCodes:[], malformedGateCodes:[], unknownGatePresent:false, productionCountsExposed:false, productionIdentitiesExposed:false });
  const allPass=diagnostic.buildReport(rows('missing'));
  assert.equal(allPass.currentPostcheckPass,true);
  assert.deepEqual(allPass.failedGateCodes,[]);
});
test('reports structural contract failures without echoing unknown values', () => {
  const missing=diagnostic.buildReport(rows().slice(1));
  assert.deepEqual(missing.missingGateCodes,[acceptance.POSTCHECK_CODES[0]]);
  const duplicate=diagnostic.buildReport([...rows(),rows()[0]]);
  assert.deepEqual(duplicate.duplicateGateCodes,[acceptance.POSTCHECK_CODES[0]]);
  const malformedRows=rows(); malformedRows[0].passed='false';
  assert.deepEqual(diagnostic.buildReport(malformedRows).malformedGateCodes,[acceptance.POSTCHECK_CODES[0]]);
  const unknownRows=rows(); unknownRows[0]={check_code:'SECRET_VALUE',passed:false};
  const unknown=diagnostic.buildReport(unknownRows);
  assert.equal(unknown.unknownGatePresent,true);
  assert.doesNotMatch(JSON.stringify(unknown),/SECRET_VALUE/);
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
test('committed first diagnostic outcome is redacted and retry-free', () => {
  const evidence = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'artifacts/dataset-v2-acceptance/e2-t6-rls/v4-diagnostic-outcome-v1.json'), 'utf8'));
  assert.deepEqual(evidence, { operation:'e2_t6_rls_v4', status:'DIAGNOSTIC_FAIL_CLOSED', safeCode:'DIAGNOSTIC_CONTRACT_FAILED', requests:1, retries:0, productionCountsExposed:false, productionIdentitiesExposed:false });
});
test('shape diagnostic identifies the zero-baseline row omission without values', () => {
  const evidence = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'artifacts/dataset-v2-acceptance/e2-t6-rls/v4-diagnostic-shape-v2.json'), 'utf8'));
  assert.equal(evidence.checkedGates, 18);
  assert.deepEqual(evidence.missingGateCodes, ['DATASET_V2_BASELINE']);
  assert.deepEqual(evidence.failedGateCodes, []);
  assert.equal(evidence.requests, 1);
  assert.equal(evidence.retries, 0);
  assert.doesNotMatch(JSON.stringify(evidence), /actual_count|expected_count|user_id|email|uuid|https?:/i);
});
test('corrected diagnostic preserves zero-row baseline gates as scalar rows', () => {
  const sql = fs.readFileSync(path.join(__dirname, '..', diagnostic.DIAGNOSTIC_SQL), 'utf8');
  for (const [code, relation, baseline] of [
    ['DATASET_V2_BASELINE','public.performance_dataset_rows_v2','dataset_v2_rows'],
    ['DATASET_V1_BASELINE','public.performance_dataset_rows','dataset_v1_rows'],
    ['SNAPSHOT_BASELINE','public.dashboard_snapshots','snapshot_rows']
  ]) {
    assert.match(sql, new RegExp(`select '${code}',\\(select count\\(\\*\\) from ${relation.replaceAll('.', '\\.') }\\),\\(select ${baseline} from expected\\)`));
  }
  assert.doesNotMatch(sql, /cross join expected e group by e\.(?:dataset_v2_rows|dataset_v1_rows|snapshot_rows)/);
  assert.equal((sql.match(/\(-1\)::bigint/g)||[]).length, 5);
});
