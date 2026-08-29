'use strict';

const crypto = require('node:crypto');
const cp = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const capture = require('../scripts/e2-t8-schema-capture');

const APPROVED_MAIN_SHA = 'a5e0917acf650e753161a97a85baf9e851d967b9';
const OPERATION = 'e2_t8_source_capture_v1';
const INVENTORY_CONFIRMATION = 'E2-T8-SOURCE-INVENTORY';
const OWNERSHIP_DIAGNOSTIC_CONFIRMATION = 'E2-T8-SOURCE-OWNERSHIP-DIAGNOSTIC';
const CAPTURE_CONFIRMATION = 'E2-T8-SCHEMA-CAPTURE';
const SQL = 'docs/security/sql/E2_T8_SOURCE_INVENTORY.sql';
const ARTIFACTS = Object.freeze([
  SQL, 'operator/e2-t8-capture.js', 'operator/management-api.js', 'operator/state-store.js',
  'package.json',
  'scripts/e2-t8-capture-operator.js', 'scripts/e2-t8-schema-capture.js',
  'security/e2-t8-captured-schema-validator.js', 'security/e2-t8-restore-contract.js',
  'artifacts/dataset-v2-acceptance/e2-t8-restore/restore-scope.json',
  'artifacts/dataset-v2-acceptance/e2-t8-restore/migration-classification.json',
  'artifacts/dataset-v2-acceptance/e2-t8-restore/restore-artifact-manifest.template.json'
]);
const OWNERSHIP = Object.freeze(['application_owned', 'managed_extension_owned', 'excluded_managed']);

function checksum(repo) {
  const hash = crypto.createHash('sha256');
  for (const file of ARTIFACTS) hash.update(file).update('\0').update(fs.readFileSync(path.join(repo, file))).update('\0');
  return hash.digest('hex');
}
function binding(repo) { return { operation: OPERATION, version: 'e2-t8-source-capture-v1', approvedMainSha: APPROVED_MAIN_SHA, artifactChecksum: checksum(repo) }; }
const STATE_FIELDS = Object.freeze(['schemaVersion','operation','version','approvedMainSha','artifactChecksum','phase','consumed','inventorySha256','inventoryRequests','captureRequests']);
function stateValue(repo, value) { return { schemaVersion: 1, ...binding(repo), ...value }; }
function validateState(value, expected) {
  if (!value || JSON.stringify(Object.keys(value).sort()) !== JSON.stringify([...STATE_FIELDS].sort()) || value.schemaVersion !== 1 ||
    value.operation !== expected.operation || value.version !== expected.version || value.approvedMainSha !== expected.approvedMainSha ||
    value.artifactChecksum !== expected.artifactChecksum || !['capture-approval-ready','consumed'].includes(value.phase) ||
    typeof value.consumed !== 'boolean' || !/^[0-9a-f]{64}$/.test(value.inventorySha256) ||
    !Number.isSafeInteger(value.inventoryRequests) || !Number.isSafeInteger(value.captureRequests)) throw new Error('Capture state contract mismatch');
  return value;
}
function assertOutsideRepo(file, repo) { const relative = path.relative(path.resolve(repo), path.resolve(file)); if (!relative.startsWith('..') || relative === '') throw new Error('Capture state must be outside repository'); }
function writeCaptureState(file, value, repo, create = false) {
  assertOutsideRepo(file, repo); const dir = path.dirname(file); fs.mkdirSync(dir, { recursive: true, mode: 0o700 }); fs.chmodSync(dir, 0o700);
  const state = validateState(stateValue(repo, value), binding(repo));
  fs.writeFileSync(file, `${JSON.stringify(state)}\n`, { flag: create ? 'wx' : 'w', mode: 0o600 }); fs.chmodSync(file, 0o600);
}
function readCaptureState(file, repo) {
  assertOutsideRepo(file, repo); const stat = fs.lstatSync(file); if (!stat.isFile() || stat.isSymbolicLink() || (stat.mode & 0o777) !== 0o600) throw new Error('Capture state is unsafe');
  return validateState(JSON.parse(fs.readFileSync(file, 'utf8')), binding(repo));
}
function verifyGit(repo, exec = cp.execFileSync) {
  const approved = exec('git', ['rev-parse', `${APPROVED_MAIN_SHA}^{commit}`], { cwd: repo, encoding: 'utf8' }).trim();
  const base = exec('git', ['merge-base', 'HEAD', APPROVED_MAIN_SHA], { cwd: repo, encoding: 'utf8' }).trim();
  if (approved !== APPROVED_MAIN_SHA || base !== APPROVED_MAIN_SHA) throw new Error('HEAD is not based on approved main SHA');
}
function validateInventory(rows) {
  if (!Array.isArray(rows) || rows.length === 0) throw safeError('SOURCE_INVENTORY_EMPTY');
  const seen = new Set();
  for (const row of rows) {
    if (!row || Object.getPrototypeOf(row) !== Object.prototype ||
      JSON.stringify(Object.keys(row).sort()) !== JSON.stringify(['fingerprint','object_class','object_key','ownership_class'])) throw safeError('SOURCE_INVENTORY_ROW_SHAPE_INVALID');
    if (typeof row.object_key !== 'string' || !/^[a-z]+:.+/.test(row.object_key) || typeof row.object_class !== 'string') throw safeError('SOURCE_INVENTORY_IDENTITY_INVALID');
    if (!OWNERSHIP.includes(row.ownership_class)) throw safeError('SOURCE_INVENTORY_OWNERSHIP_UNCLASSIFIED');
    if (!/^[0-9a-f]{64}$/.test(row.fingerprint)) throw safeError('SOURCE_INVENTORY_FINGERPRINT_INVALID');
    if (seen.has(row.object_key)) throw safeError('SOURCE_INVENTORY_DUPLICATE_IDENTITY');
    seen.add(row.object_key);
  }
  if (!rows.some(row => row.ownership_class === 'application_owned')) throw safeError('SOURCE_INVENTORY_APPLICATION_EMPTY');
  return rows;
}
function inventoryChecksum(rows) { return crypto.createHash('sha256').update(JSON.stringify(rows)).digest('hex'); }
function safeError(code) { return Object.assign(new Error('E2-T8 capture stopped fail-closed'), { safeCode: code }); }

async function preflight({ repo, stateFile, client, confirmation, verifyRepository = verifyGit }) {
  if (confirmation !== INVENTORY_CONFIRMATION) throw new Error('Exact inventory confirmation is required');
  verifyRepository(repo);
  const inventoryFile = `${stateFile}.inventory.json`;
  if (fs.existsSync(stateFile) || fs.existsSync(inventoryFile) || fs.existsSync(`${stateFile}.outcome.json`)) throw new Error('Capture state already exists');
  let result;
  try { result = await client.query(fs.readFileSync(path.join(repo, SQL), 'utf8')); }
  catch (error) {
    const allowed = new Set(['MANAGEMENT_AUTH_REJECTED','MANAGEMENT_QUERY_REJECTED','MANAGEMENT_SERVICE_UNAVAILABLE','MANAGEMENT_RESPONSE_INVALID','MANAGEMENT_TIMEOUT','MANAGEMENT_TRANSPORT_FAILED']);
    throw safeError(`SOURCE_INVENTORY_${allowed.has(error && error.safeCode) ? error.safeCode : 'QUERY_FAILED'}`);
  }
  let rows;
  try { rows = validateInventory(result.rows); }
  catch (error) {
    const allowed = new Set(['SOURCE_INVENTORY_EMPTY','SOURCE_INVENTORY_ROW_SHAPE_INVALID','SOURCE_INVENTORY_IDENTITY_INVALID','SOURCE_INVENTORY_OWNERSHIP_UNCLASSIFIED','SOURCE_INVENTORY_FINGERPRINT_INVALID','SOURCE_INVENTORY_DUPLICATE_IDENTITY','SOURCE_INVENTORY_APPLICATION_EMPTY']);
    throw safeError(allowed.has(error && error.safeCode) ? error.safeCode : 'SOURCE_INVENTORY_CONTRACT_FAILED');
  }
  fs.mkdirSync(path.dirname(stateFile), { recursive: true, mode: 0o700 });
  fs.writeFileSync(inventoryFile, JSON.stringify(rows), { flag: 'wx', mode: 0o600 });
  const state = { schemaVersion: 1, phase: 'capture-approval-ready', consumed: false, inventorySha256: inventoryChecksum(rows), inventoryRequests: 1, captureRequests: 0 };
  writeCaptureState(stateFile, state, repo, true);
  return Object.freeze({ operation: OPERATION, status: 'CAPTURE_APPROVAL_READY', inventorySha256: state.inventorySha256, inventoryRequests: 1, captureRequests: 0, productionRowsRead: false, productionIdentitiesExposed: false });
}

async function diagnoseOwnership({ repo, stateFile, client, confirmation, verifyRepository = verifyGit }) {
  if (confirmation !== OWNERSHIP_DIAGNOSTIC_CONFIRMATION) throw new Error('Exact ownership diagnostic confirmation is required');
  verifyRepository(repo);
  const diagnosticFile = `${stateFile}.ownership.json`;
  if (fs.existsSync(stateFile) || fs.existsSync(diagnosticFile)) throw new Error('Ownership diagnostic state already exists');
  let result;
  try { result = await client.query(fs.readFileSync(path.join(repo, SQL), 'utf8')); }
  catch { throw safeError('SOURCE_OWNERSHIP_DIAGNOSTIC_QUERY_FAILED'); }
  const rows = Array.isArray(result && result.rows) ? result.rows.filter(row => row && row.ownership_class === 'unclassified') : null;
  if (!rows || rows.length === 0 || rows.some(row => Object.getPrototypeOf(row) !== Object.prototype ||
    JSON.stringify(Object.keys(row).sort()) !== JSON.stringify(['fingerprint','object_class','object_key','ownership_class']) ||
    typeof row.object_key !== 'string' || typeof row.object_class !== 'string' || !/^[0-9a-f]{64}$/.test(row.fingerprint))) throw safeError('SOURCE_OWNERSHIP_DIAGNOSTIC_CONTRACT_FAILED');
  fs.mkdirSync(path.dirname(stateFile), { recursive: true, mode: 0o700 }); fs.chmodSync(path.dirname(stateFile), 0o700);
  fs.writeFileSync(diagnosticFile, JSON.stringify(rows), { flag: 'wx', mode: 0o600 }); fs.chmodSync(diagnosticFile, 0o600);
  return Object.freeze({ operation: OPERATION, status: 'OWNERSHIP_REVIEW_REQUIRED', unclassifiedCount: rows.length,
    diagnosticSha256: inventoryChecksum(rows), requests: 1, repositoryArtifactWritten: false, productionRowsRead: false });
}

function execute({ repo, stateFile, confirmation, runCapture = capture.run, verifyRepository = verifyGit, env = process.env }) {
  if (confirmation !== CAPTURE_CONFIRMATION) throw new Error('Exact capture confirmation is required');
  verifyRepository(repo);
  const state = readCaptureState(stateFile, repo);
  const inventoryFile = `${stateFile}.inventory.json`, outcomeFile = `${stateFile}.outcome.json`;
  if (state.phase !== 'capture-approval-ready' || state.consumed || fs.existsSync(outcomeFile)) throw new Error('Capture approval-ready state is required');
  let inventory;
  try { inventory = validateInventory(JSON.parse(fs.readFileSync(inventoryFile, 'utf8'))); }
  catch { throw safeError('SOURCE_INVENTORY_CAPSULE_INVALID'); }
  if (inventoryChecksum(inventory) !== state.inventorySha256) throw safeError('SOURCE_INVENTORY_CHECKSUM_MISMATCH');
  writeCaptureState(stateFile, { phase: 'consumed', consumed: true, inventorySha256: state.inventorySha256, inventoryRequests: 1, captureRequests: 1 }, repo);
  const scope = JSON.parse(fs.readFileSync(path.join(repo, 'artifacts/dataset-v2-acceptance/e2-t8-restore/restore-scope.json')));
  const manifest = JSON.parse(fs.readFileSync(path.join(repo, 'artifacts/dataset-v2-acceptance/e2-t8-restore/restore-artifact-manifest.template.json')));
  const validationInputs = { approvedSourceInventory: inventory, restoreScope: scope, captureManifest: manifest, expectedGrantContract: { grantees: ['anon','authenticated','service_role','PUBLIC'], privileges: ['SELECT','INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER'] } };
  let result;
  try { result = runCapture(['--execute','--confirm',capture.CONFIRMATION], { env, repoRoot: repo, validationInputs }); }
  catch { throw safeError('SCHEMA_CAPTURE_RUNTIME_FAILED'); }
  if (!result || result.status !== 'CAPTURE_QUARANTINED_CONTRACT_PASS' || !/^[0-9a-f]{64}$/.test(result.artifact_checksum)) throw safeError('SCHEMA_CAPTURE_CONTRACT_FAILED');
  fs.writeFileSync(outcomeFile, JSON.stringify({ schemaVersion: 1, status: 'PASS', baselineSha256: result.artifact_checksum }) + '\n', { flag: 'wx', mode: 0o600 });
  return Object.freeze({ operation: OPERATION, status: 'CAPTURE_QUARANTINED_CONTRACT_PASS', sourceInventorySha256: state.inventorySha256, baselineSha256: result.artifact_checksum, inventoryRequests: 1, captureRequests: 1, retries: 0, repositoryArtifactWritten: false, productionRowsRead: false, productionIdentitiesExposed: false });
}

module.exports = Object.freeze({ APPROVED_MAIN_SHA, ARTIFACTS, CAPTURE_CONFIRMATION, INVENTORY_CONFIRMATION, OWNERSHIP_DIAGNOSTIC_CONFIRMATION, OPERATION, SQL, binding, checksum, diagnoseOwnership, execute, inventoryChecksum, preflight, readCaptureState, validateInventory, verifyGit });
