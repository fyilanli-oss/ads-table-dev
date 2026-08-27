'use strict';
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const BASELINES = Object.freeze(['datasetRows','v1Rows','snapshotRows','connectedRows','encryptedRows']);
const FIELDS = Object.freeze(['schemaVersion','operation','version','approvedMainSha','artifactChecksum','baselines','phase','transactionSent','postcheckSent','consumed']);
function exactKeys(value, keys) { return value && typeof value === 'object' && !Array.isArray(value) && JSON.stringify(Object.keys(value).sort()) === JSON.stringify([...keys].sort()); }
function validateState(state, binding) {
  if (!exactKeys(state, FIELDS)) throw new Error('Operator state schema mismatch');
  if (state.schemaVersion !== 1 || state.operation !== binding.operation || state.version !== binding.version) throw new Error('Operator state binding mismatch');
  if (state.approvedMainSha !== binding.approvedMainSha) throw new Error('Approved main SHA changed');
  if (state.artifactChecksum !== binding.artifactChecksum) throw new Error('Artifact checksum changed');
  if (!exactKeys(state.baselines, BASELINES) || BASELINES.some((key) => !Number.isSafeInteger(state.baselines[key]) || state.baselines[key] < 0)) throw new Error('Exact five baselines are required');
  if (!['approval-ready','transaction-sent','consumed'].includes(state.phase)) throw new Error('Operator state phase is invalid');
  if (typeof state.transactionSent !== 'boolean' || typeof state.postcheckSent !== 'boolean' || typeof state.consumed !== 'boolean') throw new Error('Operator state flags are invalid');
  if (state.phase === 'approval-ready' && (state.transactionSent || state.postcheckSent || state.consumed)) throw new Error('Approval-ready flags are invalid');
  if (state.phase === 'transaction-sent' && (!state.transactionSent || state.postcheckSent || state.consumed)) throw new Error('Transaction-sent flags are invalid');
  if (state.phase === 'consumed' && (!state.transactionSent || !state.postcheckSent || !state.consumed)) throw new Error('Consumed flags are invalid');
  return state;
}
function defaultStatePath() { return process.env.ADS_TABLE_OPERATOR_STATE || path.join(os.homedir(), '.local', 'state', 'ads-table', 'e2-t5-v2.json'); }
function assertOutsideRepo(file, repo) {
  const relative = path.relative(path.resolve(repo), path.resolve(file));
  if (!relative.startsWith('..') || relative === '') throw new Error('Operator state must be outside repository');
}
function writeState(file, state, binding, repo, { create = false } = {}) {
  assertOutsideRepo(file, repo); validateState(state, binding);
  const dir = path.dirname(file); fs.mkdirSync(dir, { recursive: true, mode: 0o700 }); fs.chmodSync(dir, 0o700);
  const realDir = fs.realpathSync(dir);
  assertOutsideRepo(path.join(realDir, path.basename(file)), fs.realpathSync(repo));
  const temp = `${file}.${process.pid}.${Date.now()}.tmp`;
  try {
    fs.writeFileSync(temp, `${JSON.stringify(state)}\n`, { mode: 0o600, flag: 'wx' });
    if (create) fs.linkSync(temp, file);
    else fs.renameSync(temp, file);
    fs.chmodSync(file, 0o600);
  } finally { try { fs.unlinkSync(temp); } catch (error) { if (error.code !== 'ENOENT') throw error; } }
}
function readState(file, binding, repo) {
  assertOutsideRepo(file, repo);
  const dir = path.dirname(file);
  let realDir; try { realDir = fs.realpathSync(dir); } catch { throw new Error('Operator state is missing'); }
  assertOutsideRepo(path.join(realDir, path.basename(file)), fs.realpathSync(repo));
  if ((fs.statSync(dir).mode & 0o777) !== 0o700) throw new Error('Operator state directory mode must be 700');
  let descriptor; try { descriptor = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW); } catch { throw new Error('Operator state is missing or unsafe'); }
  let state;
  try {
    const stat = fs.fstatSync(descriptor);
    if (!stat.isFile()) throw new Error('Operator state must be a regular file');
    if ((stat.mode & 0o777) !== 0o600) throw new Error('Operator state mode must be 600');
    try { state = JSON.parse(fs.readFileSync(descriptor, 'utf8')); } catch { throw new Error('Operator state is malformed'); }
  } finally { fs.closeSync(descriptor); }
  return validateState(state, binding);
}
module.exports = Object.freeze({ BASELINES, defaultStatePath, readState, writeState, validateState });
