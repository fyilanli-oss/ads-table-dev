'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const acceptance = require('../operator/e2-t6-v3');
const audit = require('../operator/e2-t6-current-state-audit');
const repo = path.join(__dirname, '..');
function rows(failed) { return acceptance.PREFLIGHT_CODES.map((check_code) => ({ check_code, passed: check_code !== failed, actual_count: 1, expected_count: 1 })); }
test('current-state audit is one read-only capsule-independent request', () => {
  const sql = fs.readFileSync(path.join(repo, audit.SQL), 'utf8');
  const executable = sql.replace(/^\s*--.*$/gm, '').trim();
  assert.match(executable, /^WITH\b/i);
  const statements = executable.replace(/'(?:''|[^'])*'/g, "''");
  assert.doesNotMatch(statements, /\b(INSERT|UPDATE|DELETE|ALTER|DROP|CREATE|GRANT|REVOKE|COMMIT|ROLLBACK)\b/i);
  assert.ok(!audit.ARTIFACTS.some((file) => /state-store|postcheck|transaction/i.test(file)));
});
test('current-state audit emits only safe gate codes and no counts or identities', () => {
  assert.deepEqual(audit.buildReport(rows('E2_T6_RESIDUE')), {
    operation:'e2_t6_current_state_audit_v1', status:'AUDIT_COMPLETE', checkedGates:21,
    safeToPrepareFreshAcceptance:false, failedGateCodes:['E2_T6_RESIDUE'], productionCountsExposed:false,
    productionIdentitiesExposed:false, requests:1, retries:0
  });
  assert.equal(audit.buildReport(rows()).safeToPrepareFreshAcceptance, true);
});
test('current-state audit fails closed for malformed contracts', () => {
  assert.throws(() => audit.buildReport(rows().slice(1)), /count/);
  const duplicate = rows(); duplicate[1] = { ...duplicate[0] }; assert.throws(() => audit.buildReport(duplicate), /allowlist/);
  const malformed = rows(); malformed[0].actual_count = '1'; assert.throws(() => audit.buildReport(malformed), /contract/);
});
test('current-state audit requires exact confirmation and maps query/contract stages', async () => {
  const valid = { query: async () => ({ rows: rows() }) };
  await assert.rejects(audit.audit({ repo, client:valid, confirmation:'wrong', verifyRepository(){} }), /confirmation/);
  await assert.rejects(audit.audit({ repo, client:{query:async()=>{throw new Error('raw')}} , confirmation:audit.CONFIRMATION, verifyRepository(){} }), (error) => error.safeCode === 'AUDIT_QUERY_FAILED');
  await assert.rejects(audit.audit({ repo, client:{query:async()=>({rows:[]})}, confirmation:audit.CONFIRMATION, verifyRepository(){} }), (error) => error.safeCode === 'AUDIT_CONTRACT_FAILED');
  const report = await audit.audit({ repo, client:valid, confirmation:audit.CONFIRMATION, verifyRepository(){} });
  assert.equal(report.safeToPrepareFreshAcceptance, true);
});
