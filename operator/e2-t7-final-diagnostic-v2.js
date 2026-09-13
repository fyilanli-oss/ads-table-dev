'use strict';

const cp = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const t7 = require('./e2-t7-v2');
const v1 = require('./e2-t7-final-diagnostic');
const { readState } = require('./state-store');
const evidence = require('../scripts/e2-t7-cleanup-evidence');

const APPROVED_MAIN_SHA = 'a31ebb61e4768c7bb149499053685404571adbc8';
const CONFIRMATION = 'E2-T7-NAMED-BASELINE-DIAGNOSTIC';
const SQL = 'docs/security/sql/E2_T7_FINAL_DIAGNOSTIC_V2.sql';
const TOKENS = Object.freeze({ connectedRows: -71001, encryptedRows: -71002, datasetRows: -71003, v1Rows: -71004, snapshotRows: -71005 });

function verifyGit(repo, exec = cp.execFileSync) {
  const approved = exec('git', ['rev-parse', `${APPROVED_MAIN_SHA}^{commit}`], { cwd: repo, encoding: 'utf8' }).trim();
  const base = exec('git', ['merge-base', 'HEAD', APPROVED_MAIN_SHA], { cwd: repo, encoding: 'utf8' }).trim();
  if (approved !== APPROVED_MAIN_SHA || base !== APPROVED_MAIN_SHA) throw new Error('HEAD is not based on approved main SHA');
}

function bindNamed(template, baselines) {
  let sql = template;
  for (const [key, token] of Object.entries(TOKENS)) {
    const marker = `(${token})::bigint`;
    if (sql.split(marker).length !== 2) throw new Error(`Named baseline marker mismatch: ${key}`);
    sql = sql.replace(marker, `(${baselines[key]})::bigint`);
  }
  if (Object.values(TOKENS).some(token => sql.includes(`(${token})::bigint`))) throw new Error('Named baseline substitution failed');
  return sql;
}

async function diagnose({ repo, stateFile, client, confirmation, verifyRepository = verifyGit }) {
  if (confirmation !== CONFIRMATION) throw new Error('Exact human confirmation is required');
  verifyRepository(repo);
  const state = readState(stateFile, t7.binding(repo), repo);
  if (!state.consumed || !fs.existsSync(`${stateFile}.diagnostic-outcome.json`) || fs.existsSync(`${stateFile}.outcome.json`)) {
    throw new Error('Consumed V1 diagnostic state is required');
  }
  const outcomeFile = `${stateFile}.diagnostic-v2-outcome.json`;
  if (fs.existsSync(outcomeFile)) throw new Error('V2 diagnostic is already consumed');
  fs.writeFileSync(outcomeFile, JSON.stringify({ schemaVersion: 1, status: 'CONSUMED' }) + '\n', { flag: 'wx', mode: 0o600 });
  let result;
  try { result = await client.query(bindNamed(fs.readFileSync(path.join(repo, SQL), 'utf8'), state.baselines)); }
  catch { throw Object.assign(new Error('E2-T7 V2 diagnostic stopped fail-closed'), { safeCode: 'DIAGNOSTIC_V2_QUERY_FAILED' }); }
  let gates;
  try { gates = v1.validateRows(result.rows); }
  catch { throw Object.assign(new Error('E2-T7 V2 diagnostic stopped fail-closed'), { safeCode: 'DIAGNOSTIC_V2_CONTRACT_FAILED' }); }
  const failedCodes = evidence.CHECK_CODES.filter(code => !gates.get(code));
  return Object.freeze({ operation: 'e2_t7_named_baseline_diagnostic_v2', status: failedCodes.length ? 'DIAGNOSED' : 'ALL_GATES_PASS', checkCount: evidence.CHECK_CODES.length, failedCodes, requests: 1, retries: 0, productionCountsExposed: false, productionIdentitiesExposed: false });
}

module.exports = Object.freeze({ APPROVED_MAIN_SHA, CONFIRMATION, SQL, TOKENS, bindNamed, diagnose, verifyGit });
