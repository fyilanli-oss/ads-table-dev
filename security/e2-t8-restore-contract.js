'use strict';

const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const ROOT = path.join(__dirname, '..');
const MAIN_SHA = 'e3aca83d6a5ba65b601190b37b7547d97ea7a475';
const SCOPE_KEYS = ['contract_version','status','source','target','included_schemas','excluded_schemas','included_object_classes','excluded_content','historical_migration_policy','forward_migration_policy','restore_sequence','stop_gates','acceptance_required','production_effect'];
const MANIFEST_KEYS = ['manifest_version','status','provenance','source_repository_sha','source_inventory_sha256','capture_tool','capture_tool_version','capture_contract_version','baseline_sha256','baseline_cutoff','schemas_included','schemas_excluded','row_data_included','managed_schema_ddl_included','owner_restore_included','secrets_or_pii_included','migration_classification_sha256','human_review_status','restore_ready','fresh_restore_verified'];
const MIGRATION_KEYS = ['version','filename','sha256','objects','candidate_classification','final_classification_status','baseline_cutoff_dependency','replay_allowed','reason'];
const SENSITIVE = /(?:postgres(?:ql)?:\/\/|eyJ[A-Za-z0-9_-]{10,}|-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}|[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}|password\s*[=:])/i;
const exactKeys = (value, keys) => JSON.stringify(Object.keys(value)) === JSON.stringify(keys);
const sha256 = (value) => crypto.createHash('sha256').update(value).digest('hex');

function validatePreparation(scope, classification, manifest) {
  const errors = [];
  if (!exactKeys(scope, SCOPE_KEYS)) errors.push('SCOPE_ALLOWLIST');
  if (!exactKeys(manifest, MANIFEST_KEYS)) errors.push('MANIFEST_ALLOWLIST');
  if (scope.source.repository_main_sha !== MAIN_SHA || manifest.source_repository_sha !== MAIN_SHA) errors.push('SOURCE_SHA');
  if (manifest.provenance !== 'CURRENT_STATE_BASELINE') errors.push('PROVENANCE');
  for (const key of ['row_data_included','managed_schema_ddl_included','owner_restore_included','secrets_or_pii_included']) if (manifest[key] !== false) errors.push(`RISK_${key}`);
  if (manifest.baseline_cutoff === null && manifest.restore_ready !== false) errors.push('PENDING_CUTOFF_READY');
  if (classification.migrations.length !== 6) errors.push('MIGRATION_COUNT');
  const filenames = new Set(); const objects = new Set();
  for (const migration of classification.migrations) {
    if (!exactKeys(migration, MIGRATION_KEYS)) errors.push('MIGRATION_ALLOWLIST');
    if (filenames.has(migration.filename)) errors.push('DUPLICATE_MIGRATION'); filenames.add(migration.filename);
    const file = path.join(ROOT, 'supabase/migrations', migration.filename);
    if (!fs.existsSync(file) || sha256(fs.readFileSync(file)) !== migration.sha256) errors.push('MIGRATION_CHECKSUM');
    if (migration.final_classification_status !== 'pending_capture_checksum' || migration.replay_allowed !== false) errors.push('CLASSIFICATION_NOT_PENDING');
    for (const object of migration.objects) { if (objects.has(object)) errors.push('DUPLICATE_OBJECT_OWNERSHIP'); objects.add(object); }
  }
  if (scope.target.plain_generic_postgresql_target !== false) errors.push('GENERIC_TARGET');
  if (scope.target.production_target || scope.target.existing_shared_development_project_target) errors.push('FORBIDDEN_TARGET');
  if (!Array.isArray(scope.target.required_managed_primitives) || scope.target.required_managed_primitives.length !== 5) errors.push('MANAGED_PRIMITIVES');
  if (SENSITIVE.test(JSON.stringify({ scope, classification, manifest }))) errors.push('SENSITIVE_PATTERN');
  return { status: errors.length ? 'FAIL' : 'PASS', errors: [...new Set(errors)], restoreSafeDecision: false };
}

function validateTarget(target) {
  const required = ['auth_schema','auth_uid_exact_signature','anon_role','authenticated_role','service_role_role'];
  const errors = required.filter((key) => target[key] !== true).map(() => 'MISSING_MANAGED_PRIMITIVE');
  if (!['disposable_supabase','official_full_local_supabase'].includes(target.kind)) errors.push('GENERIC_POSTGRESQL_TARGET');
  if (target.production || target.shared || !target.empty || target.production_ref_match) errors.push('FORBIDDEN_TARGET');
  return { status: errors.length ? 'FAIL' : 'PASS', errors: [...new Set(errors)] };
}

module.exports = { MAIN_SHA, SCOPE_KEYS, MANIFEST_KEYS, MIGRATION_KEYS, SENSITIVE, sha256, validatePreparation, validateTarget };
