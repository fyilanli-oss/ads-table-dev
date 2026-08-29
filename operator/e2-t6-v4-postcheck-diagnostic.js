'use strict';
const fs = require('node:fs');
const { readState } = require('./state-store');
const acceptance = require('./e2-t6-v4');

const CONFIRMATION = 'E2-T6-V4-POSTCHECK-DIAGNOSTIC';
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
  if (!Array.isArray(rows)) throw new Error('Diagnostic rows are required');
  const allowed = new Set(acceptance.POSTCHECK_CODES);
  const counts = new Map(acceptance.POSTCHECK_CODES.map((code) => [code, 0]));
  let unknownGatePresent = false;
  for (const row of rows) {
    if (!row || typeof row.check_code !== 'string' || !allowed.has(row.check_code)) { unknownGatePresent = true; continue; }
    counts.set(row.check_code, counts.get(row.check_code) + 1);
  }
  const missingGateCodes = acceptance.POSTCHECK_CODES.filter((code) => counts.get(code) === 0);
  const duplicateGateCodes = acceptance.POSTCHECK_CODES.filter((code) => counts.get(code) > 1);
  const malformedGateCodes = acceptance.POSTCHECK_CODES.filter((code) => rows.some((row) => row && row.check_code === code && typeof row.passed !== 'boolean'));
  const failedGateCodes = acceptance.POSTCHECK_CODES.filter((code) => rows.some((row) => row && row.check_code === code && row.passed === false));
  const shapePass = !unknownGatePresent && missingGateCodes.length === 0 && duplicateGateCodes.length === 0 && malformedGateCodes.length === 0 && rows.length === acceptance.POSTCHECK_CODES.length;
  return Object.freeze({
    operation: acceptance.OPERATION, status: 'DIAGNOSTIC_COMPLETE', checkedGates: rows.length,
    currentPostcheckPass: shapePass && failedGateCodes.length === 0,
    failedGateCodes: Object.freeze(failedGateCodes), missingGateCodes: Object.freeze(missingGateCodes),
    duplicateGateCodes: Object.freeze(duplicateGateCodes), malformedGateCodes: Object.freeze(malformedGateCodes),
    unknownGatePresent, productionCountsExposed: false, productionIdentitiesExposed: false
  });
}
async function diagnose({ repo, stateFile, client, confirmation }) {
  if (confirmation !== CONFIRMATION) throw new Error('Exact human confirmation is required');
  acceptance.verifyGit(repo);
  const state = readState(stateFile, acceptance.binding(repo), repo);
  if (state.phase !== 'consumed' || !state.transactionSent || !state.postcheckSent || !state.consumed) throw new Error('Consumed failed acceptance state is required');
  validateOutcome(JSON.parse(fs.readFileSync(`${stateFile}.outcome.json`, 'utf8')));
  const sql = acceptance.postcheckSql(fs.readFileSync(`${repo}/${acceptance.SQL.postcheck}`, 'utf8'), state.baselines);
  let result;
  try { result = await client.query(sql); } catch (_) { const error = new Error('Diagnostic query failed'); error.safeCode = 'DIAGNOSTIC_QUERY_FAILED'; throw error; }
  try { return buildReport(result.rows); } catch (_) { const error = new Error('Diagnostic contract failed'); error.safeCode = 'DIAGNOSTIC_CONTRACT_FAILED'; throw error; }
}
module.exports = Object.freeze({ CONFIRMATION, buildReport, diagnose, validateOutcome });
