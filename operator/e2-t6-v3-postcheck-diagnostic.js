'use strict';
const fs = require('node:fs');
const { readState } = require('./state-store');
const acceptance = require('./e2-t6-v3');

const CONFIRMATION = 'E2-T6-V3-POSTCHECK-DIAGNOSTIC';
const OUTCOME_FIELDS = Object.freeze(['schemaVersion', 'operation', 'version', 'code']);
function exactKeys(value, keys) {
  return value && typeof value === 'object' && !Array.isArray(value)
    && JSON.stringify(Object.keys(value).sort()) === JSON.stringify([...keys].sort());
}
function validateOutcome(outcome) {
  if (!exactKeys(outcome, OUTCOME_FIELDS)
    || outcome.schemaVersion !== 1
    || outcome.operation !== acceptance.OPERATION
    || outcome.version !== acceptance.VERSION
    || outcome.code !== 'POSTCHECK_FAILED') throw new Error('Exact POSTCHECK_FAILED outcome is required');
  return outcome;
}
function buildReport(rows) {
  if (!Array.isArray(rows) || rows.length !== acceptance.POSTCHECK_CODES.length) throw new Error('Diagnostic gate count mismatch');
  const codes = rows.map((row) => row && row.check_code).sort();
  if (JSON.stringify(codes) !== JSON.stringify(acceptance.POSTCHECK_CODES)) throw new Error('Diagnostic gate allowlist mismatch');
  if (rows.some((row) => typeof row.passed !== 'boolean')) throw new Error('Diagnostic passed contract mismatch');
  const failedGateCodes = rows.filter((row) => !row.passed).map((row) => row.check_code).sort();
  if (!failedGateCodes.length) throw new Error('Diagnostic must identify at least one failed gate');
  return Object.freeze({ operation: acceptance.OPERATION, status: 'DIAGNOSTIC_COMPLETE', checkedGates: rows.length, failedGateCodes: Object.freeze(failedGateCodes), productionCountsExposed: false });
}
async function diagnose({ repo, stateFile, client, confirmation }) {
  if (confirmation !== CONFIRMATION) throw new Error('Exact human confirmation is required');
  acceptance.verifyGit(repo);
  const state = readState(stateFile, acceptance.binding(repo), repo);
  if (state.phase !== 'consumed' || !state.transactionSent || !state.postcheckSent || !state.consumed) throw new Error('Consumed failed acceptance state is required');
  validateOutcome(JSON.parse(fs.readFileSync(`${stateFile}.outcome.json`, 'utf8')));
  const sql = acceptance.postcheckSql(fs.readFileSync(`${repo}/${acceptance.SQL.postcheck}`, 'utf8'), state.baselines);
  const result = await client.query(sql);
  return buildReport(result.rows);
}
module.exports = Object.freeze({ CONFIRMATION, buildReport, diagnose, validateOutcome });
