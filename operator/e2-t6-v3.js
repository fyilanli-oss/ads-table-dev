'use strict';
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const cp = require('node:child_process');
const { BASELINES, readState, writeState } = require('./state-store');
const { buildEvidence } = require('../scripts/e2-t6-v3-evidence');

const APPROVED_MAIN_SHA = '4960409e3b20d697f75ae2b0d2b4d4872d62ae89';
const OPERATION = 'e2_t6_rls_v3';
const VERSION = 'e2-t6-rls-v3';
const SQL = Object.freeze({
  preflight: 'docs/security/sql/E2_T6_RLS_V3_PREFLIGHT.sql',
  transaction: 'docs/security/sql/E2_T6_RLS_V3_TRANSACTION.sql',
  postcheck: 'docs/security/sql/E2_T6_RLS_V3_POSTCHECK.sql'
});
const ARTIFACTS = Object.freeze([
  ...Object.values(SQL),
  'operator/e2-t6-v3.js',
  'operator/management-api.js',
  'operator/state-store.js',
  'scripts/e2-t6-v3-operator.js',
  'scripts/e2-t6-v3-evidence.js',
  'artifacts/dataset-v2-acceptance/e2-t6-rls/fixture-contract-v3.json',
  'artifacts/dataset-v2-acceptance/e2-t6-rls/rls-matrix.json'
]);
const PREFLIGHT_CODES = Object.freeze(['ANON_NO_PRIVILEGE','AUTHENTICATED_NO_MUTATION','AUTHENTICATED_SELECT','CONNECTED_CONNECTIONS','CONNECTION_TOKEN_PARITY','CONSTRAINT_INDEX_STATE','DATASET_V1_ROWS','DATASET_V2_EXISTS','DATASET_V2_ROWS','DISTINCT_ELIGIBLE_USERS','E2_T6_RESIDUE','ENCRYPTED_TOKEN_ROWS','EXACT_OWN_SELECT_POLICY','LEDGER_TOTAL','MISSING_ENCRYPTED','OAUTH_ROWS','ORPHAN_ENCRYPTED','PLAINTEXT_TOKENS','RLS_ENABLED_NOT_FORCED','SERVICE_ROLE_ACCESS','SNAPSHOT_ROWS']);
const POSTCHECK_CODES = Object.freeze(['ANON_NO_PRIVILEGE','AUTHENTICATED_SELECT_ONLY','CONNECTED_CONNECTIONS','CONNECTION_TOKEN_PARITY','CONSTRAINT_INDEX_STATE','DATASET_V1_BASELINE','DATASET_V2_BASELINE','E2_T6_RESIDUE','ENCRYPTED_TOKEN_ROWS','EXACT_OWN_SELECT_POLICY','LEDGER_TOTAL','MISSING_ENCRYPTED','OAUTH_ROWS','ORPHAN_ENCRYPTED','PERSISTENT_EVIDENCE_OBJECTS','PLAINTEXT_TOKENS','RLS_ENABLED_NOT_FORCED','SERVICE_ROLE_ACCESS','SNAPSHOT_BASELINE']);
function terminalError(code) { const error = new Error('E2-T6 V3 stopped fail-closed'); error.safeCode = code; return error; }
function persistOutcome(stateFile, code) {
  const file = `${stateFile}.outcome.json`;
  fs.writeFileSync(file, `${JSON.stringify({ schemaVersion:1, operation:OPERATION, version:VERSION, code })}\n`, { flag:'wx', mode:0o600 });
  return file;
}
function checksum(repo) { const hash = crypto.createHash('sha256'); for (const file of ARTIFACTS) hash.update(file).update('\0').update(fs.readFileSync(path.join(repo, file))).update('\0'); return hash.digest('hex'); }
function binding(repo) { return { operation: OPERATION, version: VERSION, approvedMainSha: APPROVED_MAIN_SHA, artifactChecksum: checksum(repo) }; }
function verifyGit(repo, exec = cp.execFileSync) {
  const approved = exec('git', ['rev-parse', `${APPROVED_MAIN_SHA}^{commit}`], { cwd: repo, encoding: 'utf8' }).trim();
  const base = exec('git', ['merge-base', 'HEAD', APPROVED_MAIN_SHA], { cwd: repo, encoding: 'utf8' }).trim();
  if (approved !== APPROVED_MAIN_SHA || base !== APPROVED_MAIN_SHA) throw new Error('HEAD is not based on approved main SHA');
}
function readSql(repo, kind) { return fs.readFileSync(path.join(repo, SQL[kind]), 'utf8'); }
function validatePreflight(rows) {
  validateGateRows(rows, PREFLIGHT_CODES, 'Preflight');
  const names = { DATASET_V2_ROWS:'datasetRows', DATASET_V1_ROWS:'v1Rows', SNAPSHOT_ROWS:'snapshotRows', CONNECTED_CONNECTIONS:'connectedRows', ENCRYPTED_TOKEN_ROWS:'encryptedRows' };
  const baselines = {};
  for (const [code, key] of Object.entries(names)) {
    const row = rows.find((item) => item.check_code === code);
    if (!row || !Number.isSafeInteger(row.actual_count) || row.actual_count < 0) throw new Error('Preflight baseline is missing');
    baselines[key] = row.actual_count;
  }
  return baselines;
}
function postcheckSql(template, baselines) {
  const placeholders = template.match(/\(-1\)::bigint/g) || [];
  if (placeholders.length !== BASELINES.length) throw new Error('Postcheck must contain exactly five baseline placeholders');
  let sql = template;
  for (const key of BASELINES) sql = sql.replace('(-1)::bigint', `(${baselines[key]})::bigint`);
  if (sql.includes('(-1)::bigint')) throw new Error('Postcheck baseline substitution failed');
  return sql;
}
function validatePostcheck(rows) {
  validateGateRows(rows, POSTCHECK_CODES, 'Postcheck');
}
function validateGateRows(rows, expectedCodes, label) {
  if (!Array.isArray(rows) || rows.length !== expectedCodes.length) throw new Error(`${label} must return exactly ${expectedCodes.length} gates`);
  const codes = rows.map((row) => row && row.check_code).sort();
  if (JSON.stringify(codes) !== JSON.stringify(expectedCodes)) throw new Error(`${label} gate allowlist mismatch`);
  if (rows.some((row) => row.passed !== true
    || !Number.isSafeInteger(row.actual_count) || row.actual_count < 0
    || !Number.isSafeInteger(row.expected_count) || row.expected_count < 0)) throw new Error(`${label} gate failed`);
}
async function preflight({ repo, stateFile, client, runTests = () => cp.execFileSync('npm', ['test'], { cwd: repo, stdio: 'inherit' }), verifyRepository = verifyGit }) {
  verifyRepository(repo);
  if (fs.existsSync(stateFile) || fs.existsSync(`${stateFile}.outcome.json`)) throw new Error('Operator state already exists and cannot be replaced');
  runTests();
  const currentBinding = binding(repo);
  let result;
  try { result = await client.query(readSql(repo, 'preflight')); }
  catch (_) { throw terminalError('PREFLIGHT_QUERY_FAILED'); }
  let baselines;
  try { baselines = validatePreflight(result.rows); }
  catch (_) { throw terminalError('PREFLIGHT_GATES_FAILED'); }
  const state = { schemaVersion:1, operation:OPERATION, version:VERSION, approvedMainSha:APPROVED_MAIN_SHA, artifactChecksum:currentBinding.artifactChecksum, baselines, phase:'approval-ready', transactionSent:false, postcheckSent:false, consumed:false };
  writeState(stateFile, state, currentBinding, repo, { create:true }); readState(stateFile, currentBinding, repo);
  return Object.freeze({ operation:OPERATION, status:'APPROVAL_READY', preflight:'21/21 PASS', baselineCount:5, transactionRequests:0 });
}
async function execute({ repo, stateFile, client, confirmation, verifyRepository = verifyGit }) {
  if (confirmation !== 'E2-T6-V3-LIVE') throw new Error('Exact human confirmation is required');
  verifyRepository(repo); const currentBinding = binding(repo); let state = readState(stateFile, currentBinding, repo);
  if (state.consumed || state.postcheckSent) throw new Error('Operator state is consumed');
  let evidence; let transactionError; let transactionRequests = 0;
  if (state.phase === 'approval-ready' && !state.transactionSent) {
    state = { ...state, phase:'transaction-sent', transactionSent:true }; writeState(stateFile, state, currentBinding, repo);
    transactionRequests = 1;
    try { const transaction = await client.query(readSql(repo, 'transaction')); const row = transaction.rows.at(-1); evidence = buildEvidence(row && Object.keys(row).length === 1 && row.evidence ? row.evidence : row); }
    catch (error) { transactionError = error; }
  } else if (state.phase === 'transaction-sent' && state.transactionSent) {
    transactionError = new Error('Transaction result unavailable after interrupted execution');
  } else throw new Error('State cannot send a transaction');
  state = readState(stateFile, currentBinding, repo);
  if (!state.transactionSent || state.postcheckSent) throw new Error('Postcheck state invariant failed');
  state = { ...state, postcheckSent:true, phase:'consumed', consumed:true }; writeState(stateFile, state, currentBinding, repo);
  let postcheckError;
  try { const postcheck = await client.query(postcheckSql(readSql(repo, 'postcheck'), state.baselines)); validatePostcheck(postcheck.rows); }
  catch (error) { postcheckError = error; }
  readState(stateFile, currentBinding, repo);
  const outcome = transactionError
    ? (postcheckError ? 'TRANSACTION_AND_POSTCHECK_FAILED' : 'TRANSACTION_EVIDENCE_FAILED_POSTCHECK_PASS')
    : (postcheckError ? 'POSTCHECK_FAILED' : 'PASS');
  persistOutcome(stateFile, outcome);
  if (transactionError || postcheckError) throw terminalError(outcome);
  return Object.freeze({ operation:OPERATION, status:'PASS', evidenceVersion:evidence.evidence_version, transactionRequests, transactionRetries:0, postcheck:'19/19 PASS', postcheckRequests:1, postcheckRetries:0, state:'CONSUMED' });
}
module.exports = Object.freeze({ APPROVED_MAIN_SHA, OPERATION, VERSION, SQL, ARTIFACTS, PREFLIGHT_CODES, POSTCHECK_CODES, binding, checksum, execute, persistOutcome, postcheckSql, preflight, validatePostcheck, validatePreflight, verifyGit });
