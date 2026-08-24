'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const root = path.join(__dirname, '..');
const artifactDir = path.join(root, 'artifacts/dataset-v2-acceptance/e2-t3-roundtrip');
const canonical = require(path.join(artifactDir, 'expected-canonical.json'));
const physical = require(path.join(artifactDir, 'expected-physical.json'));
const { validateCanonicalRow, RAW_METRICS } = require('../funnel-core/canonical-contract');
const { validateEntityHierarchy } = require('../funnel-core/entity-hierarchy');
const { canonicalToDbRow, dbToCanonicalRow } = require('../funnel-core/supabase-dataset-repository');
const { buildEvidence, canonicalBlocks } = require('../scripts/e2-t3-roundtrip-evidence');
const read = (relative) => fs.readFileSync(path.join(root, relative), 'utf8');
const files = [
  'artifacts/dataset-v2-acceptance/e2-t3-roundtrip/expected-canonical.json',
  'artifacts/dataset-v2-acceptance/e2-t3-roundtrip/expected-physical.json',
  'docs/security/sql/E2_T3_ROUNDTRIP_PREFLIGHT.sql',
  'docs/security/sql/E2_T3_ROUNDTRIP_TRANSACTION.sql',
  'docs/security/sql/E2_T3_ROUNDTRIP_POSTCHECK.sql',
  'scripts/e2-t3-roundtrip-evidence.js',
  'docs/security/E2_T3_ROUNDTRIP_RUNBOOK.md'
];
const transaction = read('docs/security/sql/E2_T3_ROUNDTRIP_TRANSACTION.sql');
const preflight = read('docs/security/sql/E2_T3_ROUNDTRIP_PREFLIGHT.sql');
const postcheck = read('docs/security/sql/E2_T3_ROUNDTRIP_POSTCHECK.sql');
const stripComments = (sql) => sql.replace(/--.*$/gm, '').trim();

function expectedMappedPhysical() {
  const mapped = canonicalToDbRow(canonical);
  mapped.user_id = '<runtime_user_redacted>';
  mapped.updated_at = '<runtime_timestamp>';
  return mapped;
}

function operationResult() {
  const row = { ...physical };
  delete row.user_id;
  delete row.updated_at;
  return {
    operation_code: 'E2_T3_TRANSACTION', inserted_count: 1, contract_match_count: 1,
    read_back_count: 1, v1_unchanged: true, snapshot_unchanged: true,
    oauth_unchanged: true, tokens_unchanged: true, passed: true,
    redacted_physical: [row]
  };
}

test('all E2-T3 preparation artifacts exist', () => {
  for (const file of files) assert.equal(fs.existsSync(path.join(root, file)), true, file);
});

test('canonical Meta fixture passes existing validators and is namespaced', () => {
  assert.equal(validateCanonicalRow(canonical), canonical);
  assert.equal(validateEntityHierarchy(canonical.identity, canonical.entity), canonical.entity);
  assert.match(JSON.stringify(canonical), /e2_t3_static_v1/);
  assert.equal(canonical.identity.platform, 'meta');
  assert.equal(canonical.identity.traffic_type, 'paid');
  assert.equal(canonical.identity.source_system, 'meta_ads');
  assert.equal(canonical.provenance.synthetic, false);
});

test('canonical to physical mapping is exact and deterministic after runtime fields are redacted', () => {
  assert.deepEqual(expectedMappedPhysical(), physical);
});

test('physical to canonical preserves all seven blocks exactly', () => {
  const row = { ...physical, user_id: canonical.identity.user_id };
  delete row.updated_at;
  const roundTrip = dbToCanonicalRow(row);
  assert.deepEqual(canonicalBlocks(roundTrip), canonicalBlocks(canonical));
});

test('unsupported null, measured zero, positive metrics, and ten support keys survive', () => {
  assert.deepEqual(Object.keys(canonical.metric_support).sort(), [...RAW_METRICS].sort());
  assert.equal(canonical.metric_support.session, 'unsupported');
  assert.equal(canonical.raw_metrics.session, null);
  assert.equal(canonical.metric_support.add_to_cart, 'supported');
  assert.equal(canonical.raw_metrics.add_to_cart, 0);
  assert.equal(canonical.raw_metrics.impression > 0, true);
});

test('transaction is exactly one insert/read/rollback operation with no commit', () => {
  const sql = stripComments(transaction);
  assert.equal((sql.match(/\bbegin\s*;/gi) || []).length, 1);
  assert.equal((sql.match(/\brollback\s*;/gi) || []).length, 1);
  assert.equal((sql.match(/\bcommit\b/gi) || []).length, 0);
  assert.equal((sql.match(/\binsert\s+into\s+public\.performance_dataset_rows_v2\b/gi) || []).length, 1);
  assert.match(sql, /inserted_count=1/);
  assert.match(sql, /read_back_count[\s\S]*=1/);
  assert.doesNotMatch(sql, /\b(?:update|delete\s+from|truncate|merge)\b/i);
});

test('transaction cannot mutate protected relations, ledger, schema, grants, or RLS', () => {
  for (const relation of ['performance_dataset_rows', 'dashboard_snapshots', 'oauth_transactions', 'platform_connection_tokens', 'platform_connections', 'auth.users', 'public.users', 'schema_migrations']) {
    const escaped = relation.replace('.', '\\.');
    assert.doesNotMatch(transaction, new RegExp(`(?:insert\\s+into|update|delete\\s+from|truncate)\\s+(?:public\\.)?${escaped}\\b`, 'i'));
  }
  assert.doesNotMatch(stripComments(transaction), /\b(?:create|alter|drop|grant|revoke|call|do)\b/i);
});

test('preflight and postcheck are single read-only WITH SELECT statements', () => {
  for (const sql of [preflight, postcheck]) {
    const clean = stripComments(sql);
    assert.match(clean, /^with\b/i);
    assert.equal((clean.match(/;/g) || []).length, 1);
    assert.match(clean, /select[\s\S]*;$/i);
    assert.doesNotMatch(clean, /\b(?:insert|update|delete|alter|create|drop|grant|revoke|truncate|call|do|begin|commit|rollback)\b/i);
  }
});

test('preflight and postcheck cover parity and rollback residue', () => {
  for (const code of ['V1_ROWS', 'SNAPSHOT_ROWS', 'OAUTH_ROWS', 'CONNECTED_CONNECTIONS', 'ENCRYPTED_TOKEN_ROWS', 'LEDGER_TOTAL']) {
    assert.match(preflight, new RegExp(`'${code}'`));
    assert.match(postcheck, new RegExp(`'${code}'`));
  }
  assert.match(postcheck, /'FIXTURE_ROWS'[\s\S]*,\s*0\b/);
});

test('evidence conversion is allowlisted, exact, and fail-closed', () => {
  const evidence = buildEvidence(operationResult(), canonical);
  assert.equal(evidence.operation_status, 'PASS');
  assert.equal(evidence.blocks.length, 7);
  assert.equal(evidence.rollback_required, true);
  assert.throws(() => buildEvidence({ ...operationResult(), unexpected: true }, canonical), /allowlist/);
  assert.throws(() => buildEvidence({ ...operationResult(), read_back_count: 2 }, canonical), /read-back/);
});

test('artifacts contain no production identity, credential, URI, JWT, private key, or email', () => {
  const combined = files.map(read).join('\n');
  assert.doesNotMatch(combined, /postgres(?:ql)?:\/\/|authorization\s*:|bearer\s+|-----BEGIN [A-Z ]*PRIVATE KEY-----|\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b|[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}|[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}/i);
});

test('execution plan keeps later E2 tasks open and E2-T8 in verification', () => {
  const plan = read('codex-input/AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md');
  assert.match(plan, /E2-T1 — `Done`/);
  assert.match(plan, /E2-T2 — `Done`/);
  assert.match(plan, /E2-T3 — `Verification`/);
  assert.match(plan, /E2-T4 — `Verification`/);
  assert.match(plan, /E2-T5 — `Verification`/);
  assert.match(plan, /E2-T6 — `Verification`/);
  assert.match(plan, /E2-T7 — `Verification`/);
  assert.match(plan, /E2-T8 — `Verification`/);
});
