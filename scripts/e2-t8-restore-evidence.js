'use strict';
const crypto = require('node:crypto');
const { SENSITIVE } = require('../security/e2-t8-restore-contract');
const INVENTORY_KEYS = ['object_key','object_class','fingerprint'];
const PREFLIGHT_KEYS = ['managed_primitives_ok','target_kind_ok','target_identity_distinct','public_allowlist_only','application_relation_count','ledger_unambiguous','passed'];

const digest = (value) => crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');
function convert({ sourceInventory, targetPreflight, targetInventory, manifest, migrationClassification }) {
  const errors = [];
  if (SENSITIVE.test(JSON.stringify(arguments[0]))) errors.push('SENSITIVE_INPUT');
  if (JSON.stringify(Object.keys(targetPreflight)) !== JSON.stringify(PREFLIGHT_KEYS)) errors.push('PREFLIGHT_ALLOWLIST');
  if (!targetPreflight.managed_primitives_ok) errors.push('MANAGED_PRIMITIVE_MISSING');
  if (!targetPreflight.target_kind_ok || !targetPreflight.target_identity_distinct || !targetPreflight.public_allowlist_only || targetPreflight.application_relation_count !== 0 || !targetPreflight.ledger_unambiguous || !targetPreflight.passed) errors.push('TARGET_PREFLIGHT');
  const normalize = (rows, label) => { const map = new Map(); for (const row of rows) { if (JSON.stringify(Object.keys(row)) !== JSON.stringify(INVENTORY_KEYS)) errors.push(`${label}_ALLOWLIST`); if (map.has(row.object_key)) errors.push(`${label}_DUPLICATE`); map.set(row.object_key, row); } return map; };
  const source = normalize(sourceInventory, 'SOURCE'); const target = normalize(targetInventory, 'TARGET');
  for (const [key, row] of source) { if (!target.has(key)) errors.push('TARGET_MISSING'); else if (target.get(key).object_class !== row.object_class || target.get(key).fingerprint !== row.fingerprint) errors.push('FINGERPRINT_MISMATCH'); }
  for (const key of target.keys()) if (!source.has(key)) errors.push('TARGET_EXTRA');
  if (manifest.baseline_cutoff === null) errors.push('CUTOFF_NULL');
  if (!manifest.restore_ready) errors.push('RESTORE_NOT_READY');
  if (!migrationClassification.migrations.every((m) => m.final_classification_status === 'final' && typeof m.replay_allowed === 'boolean')) errors.push('CLASSIFICATION_INCOMPLETE');
  const status = errors.length ? 'FAIL' : 'PASS';
  return { status, errors: [...new Set(errors)], counts: { source: source.size, target: target.size }, source_checksum: digest(sourceInventory), target_checksum: digest(targetInventory) };
}
module.exports = { INVENTORY_KEYS, PREFLIGHT_KEYS, convert };
