'use strict';
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const cp = require('node:child_process');
const acceptance = require('./e2-t6-v3');

const APPROVED_MAIN_SHA = '1db924ca2df2f5569c1e1458c65882fb3c38017b';
const OPERATION = 'e2_t6_current_state_audit_v1';
const CONFIRMATION = 'E2-T6-CURRENT-STATE-AUDIT';
const SQL = 'docs/security/sql/E2_T6_RLS_V3_PREFLIGHT.sql';
const ARTIFACTS = Object.freeze([
  SQL,
  'operator/e2-t6-current-state-audit.js',
  'operator/management-api.js',
  'scripts/e2-t6-current-state-audit.js'
]);

function checksum(repo) {
  const hash = crypto.createHash('sha256');
  for (const file of ARTIFACTS) hash.update(file).update('\0').update(fs.readFileSync(path.join(repo, file))).update('\0');
  return hash.digest('hex');
}
function verifyGit(repo, exec = cp.execFileSync) {
  const approved = exec('git', ['rev-parse', `${APPROVED_MAIN_SHA}^{commit}`], { cwd: repo, encoding: 'utf8' }).trim();
  const base = exec('git', ['merge-base', 'HEAD', APPROVED_MAIN_SHA], { cwd: repo, encoding: 'utf8' }).trim();
  if (approved !== APPROVED_MAIN_SHA || base !== APPROVED_MAIN_SHA) throw new Error('HEAD is not based on approved main SHA');
}
function buildReport(rows) {
  const expected = acceptance.PREFLIGHT_CODES;
  if (!Array.isArray(rows) || rows.length !== expected.length) throw new Error('Audit gate count mismatch');
  const codes = rows.map((row) => row && row.check_code).sort();
  if (JSON.stringify(codes) !== JSON.stringify(expected)) throw new Error('Audit gate allowlist mismatch');
  if (rows.some((row) => typeof row.passed !== 'boolean'
    || !Number.isSafeInteger(row.actual_count) || row.actual_count < 0
    || !Number.isSafeInteger(row.expected_count) || row.expected_count < 0)) throw new Error('Audit gate contract mismatch');
  const failedGateCodes = rows.filter((row) => !row.passed).map((row) => row.check_code).sort();
  return Object.freeze({
    operation: OPERATION,
    status: 'AUDIT_COMPLETE',
    checkedGates: rows.length,
    safeToPrepareFreshAcceptance: failedGateCodes.length === 0,
    failedGateCodes: Object.freeze(failedGateCodes),
    productionCountsExposed: false,
    productionIdentitiesExposed: false,
    requests: 1,
    retries: 0
  });
}
async function audit({ repo, client, confirmation, verifyRepository = verifyGit }) {
  if (confirmation !== CONFIRMATION) throw new Error('Exact human confirmation is required');
  verifyRepository(repo);
  checksum(repo);
  let result;
  try { result = await client.query(fs.readFileSync(path.join(repo, SQL), 'utf8')); }
  catch (_) { const error = new Error('Current-state audit query failed'); error.safeCode = 'AUDIT_QUERY_FAILED'; throw error; }
  try { return buildReport(result.rows); }
  catch (_) { const error = new Error('Current-state audit contract failed'); error.safeCode = 'AUDIT_CONTRACT_FAILED'; throw error; }
}
module.exports = Object.freeze({ APPROVED_MAIN_SHA, ARTIFACTS, CONFIRMATION, OPERATION, SQL, audit, buildReport, checksum, verifyGit });
