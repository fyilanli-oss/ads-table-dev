'use strict';

const cp = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const t7 = require('./e2-t7-v2');
const { readState } = require('./state-store');
const evidence = require('../scripts/e2-t7-cleanup-evidence');

const APPROVED_MAIN_SHA = 'ebd425286be80b1caf0baf0a42635e9f17857de8';
const CONFIRMATION = 'E2-T7-FINAL-DIAGNOSTIC';
const SQL = 'docs/security/sql/E2_T7_FINAL_DIAGNOSTIC.sql';

function verifyGit(repo, exec = cp.execFileSync) {
  const approved = exec('git', ['rev-parse', `${APPROVED_MAIN_SHA}^{commit}`], { cwd: repo, encoding: 'utf8' }).trim();
  const base = exec('git', ['merge-base', 'HEAD', APPROVED_MAIN_SHA], { cwd: repo, encoding: 'utf8' }).trim();
  if (approved !== APPROVED_MAIN_SHA || base !== APPROVED_MAIN_SHA) throw new Error('HEAD is not based on approved main SHA');
}

function validateRows(rows) {
  if (!Array.isArray(rows) || rows.length !== evidence.CHECK_CODES.length) throw new Error('Diagnostic check count mismatch');
  const seen = new Map();
  for (const row of rows) {
    if (!row || Object.getPrototypeOf(row) !== Object.prototype ||
      JSON.stringify(Object.keys(row).sort()) !== JSON.stringify(['check_code', 'passed']) ||
      !evidence.CHECK_CODES.includes(row.check_code) || seen.has(row.check_code) || typeof row.passed !== 'boolean') {
      throw new Error('Diagnostic contract mismatch');
    }
    seen.set(row.check_code, row.passed);
  }
  for (const code of evidence.CHECK_CODES) if (!seen.has(code)) throw new Error('Diagnostic gate missing');
  return seen;
}

async function diagnose({ repo, stateFile, client, confirmation, verifyRepository = verifyGit }) {
  if (confirmation !== CONFIRMATION) throw new Error('Exact human confirmation is required');
  verifyRepository(repo);
  const state = readState(stateFile, t7.binding(repo), repo);
  if (!state.consumed || state.phase !== 'consumed' || fs.existsSync(`${stateFile}.outcome.json`)) {
    throw new Error('Consumed failed final state is required');
  }
  const outcomeFile = `${stateFile}.diagnostic-outcome.json`;
  if (fs.existsSync(outcomeFile)) throw new Error('Diagnostic is already consumed');
  fs.writeFileSync(outcomeFile, JSON.stringify({ schemaVersion: 1, status: 'CONSUMED' }) + '\n', { flag: 'wx', mode: 0o600 });
  let result;
  try {
    const template = fs.readFileSync(path.join(repo, SQL), 'utf8');
    result = await client.query(t7.finalSql(template, state.baselines));
  } catch {
    throw Object.assign(new Error('E2-T7 diagnostic stopped fail-closed'), { safeCode: 'DIAGNOSTIC_QUERY_FAILED' });
  }
  let gates;
  try { gates = validateRows(result.rows); } catch {
    throw Object.assign(new Error('E2-T7 diagnostic stopped fail-closed'), { safeCode: 'DIAGNOSTIC_CONTRACT_FAILED' });
  }
  const failedCodes = evidence.CHECK_CODES.filter(code => !gates.get(code));
  return Object.freeze({
    operation: 'e2_t7_final_diagnostic_v1', status: failedCodes.length ? 'DIAGNOSED' : 'ALL_GATES_PASS',
    checkCount: evidence.CHECK_CODES.length, failedCodes, requests: 1, retries: 0,
    productionCountsExposed: false, productionIdentitiesExposed: false
  });
}

module.exports = Object.freeze({ APPROVED_MAIN_SHA, CONFIRMATION, SQL, diagnose, validateRows, verifyGit });
