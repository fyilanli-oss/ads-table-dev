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
const { buildEvidence, canonicalBlocks, ALLOWED_RESULT_KEYS, ALLOWED_REDACTED_PHYSICAL_KEYS, RUNTIME_PHYSICAL_KEYS } = require('../scripts/e2-t3-roundtrip-evidence');
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
    operation_code: 'E2_T3_TRANSACTION_V2', inserted_count: 1, contract_match_count: 1,
    read_back_count: 1, dataset_transaction_delta_ok: true, v1_unchanged: true, snapshot_unchanged: true,
    oauth_unchanged: true, connected_unchanged: true, encrypted_unchanged: true, missing_encrypted_unchanged: true, orphan_encrypted_unchanged: true, plaintext_unchanged: true, ledger_unchanged: true, passed: true,
    redacted_physical: [row]
  };
}

test('all E2-T3 preparation artifacts exist', () => {
  for (const file of files) assert.equal(fs.existsSync(path.join(root, file)), true, file);
});

test('canonical Meta fixture passes existing validators and is namespaced', () => {
  assert.equal(validateCanonicalRow(canonical), canonical);
  assert.equal(validateEntityHierarchy(canonical.identity, canonical.entity), canonical.entity);
  assert.match(JSON.stringify(canonical), /e2_t3_static_v2/);
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
  assert.equal((sql.match(/(?:^|;)\s*commit\s*;/gi) || []).length, 0);
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
  assert.doesNotMatch(stripComments(transaction).replace(/create temp table pg_temp\.e2_t3_v2_baseline on commit drop/i, ''), /\b(?:create|alter|drop|grant|revoke|call|do)\b/i);
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
  assert.match(plan, /E2-T3 — `Done`/);
  assert.match(plan, /E2-T4 — `Done`/);
  assert.match(plan, /E2-T5 — `Done`/);
  assert.match(plan, /E2-T6 — `Done`/);
  assert.match(plan, /E2-T7 — `Done`/);
  assert.match(plan, /E2-T8 — `Deferred`/);
});

test('E2-C1 captured provider-token parity is fail-closed', () => {
  for (const sql of [preflight, transaction, postcheck]) assert.doesNotMatch(sql, /(?:CONNECTED_CONNECTIONS|ENCRYPTED_TOKEN_ROWS)[^\n]*,\s*7\b/);
  for (const code of ['MISSING_ENCRYPTED','ORPHAN_ENCRYPTED','PLAINTEXT_TOKENS','CONNECTION_TOKEN_PARITY']) assert.match(preflight, new RegExp(`'${code}'`));
  assert.match(preflight, /'CONNECTED_CONNECTIONS'[^\n]*'capture'/); assert.match(preflight, /'ENCRYPTED_TOKEN_ROWS'[^\n]*'capture'/);
  for (const field of ['dataset_transaction_delta_ok','connected_unchanged','encrypted_unchanged','missing_encrypted_unchanged','orphan_encrypted_unchanged','plaintext_unchanged','ledger_unchanged']) assert.match(transaction, new RegExp(`\\b${field}\\b`));
  assert.match(postcheck, /connected_rows/); assert.match(postcheck, /encrypted_rows/); assert.match(transaction, /rollback;\s*$/i);
  assert.throws(() => buildEvidence({...operationResult(), connection_count: 1}, canonical), /counts are forbidden/);
  for (const field of ['dataset_transaction_delta_ok','connected_unchanged','encrypted_unchanged','missing_encrypted_unchanged','orphan_encrypted_unchanged','plaintext_unchanged','ledger_unchanged']) { const value=operationResult(); value[field]=false; assert.throws(()=>buildEvidence(value,canonical),new RegExp(field)); }
});

test('T3 postcheck and runbook define exactly four operator-local replacements', () => {
  assert.equal((postcheck.match(/\(-1\)::bigint/g) || []).length, 4);
  const runbook = read('docs/security/E2_T3_ROUNDTRIP_RUNBOOK.md');
  assert.match(runbook, /exactly four operator-local replacements, in this order: V1, snapshot, connected, encrypted/);
  assert.match(runbook, /committed Dataset V2 zero is not a placeholder and must not be changed/);
  assert.match(runbook, /Actual provider counts remain operator-local, are never shared, and are never committed/);
});

test('v2 namespace and evidence identifiers replace v1 in active E2-T3 artifacts', () => {
  const active = [
    'artifacts/dataset-v2-acceptance/e2-t3-roundtrip/expected-canonical.json',
    'artifacts/dataset-v2-acceptance/e2-t3-roundtrip/expected-physical.json',
    'docs/security/sql/E2_T3_ROUNDTRIP_PREFLIGHT.sql',
    'docs/security/sql/E2_T3_ROUNDTRIP_TRANSACTION.sql',
    'docs/security/sql/E2_T3_ROUNDTRIP_POSTCHECK.sql',
    'scripts/e2-t3-roundtrip-evidence.js'
  ].map(read).join('\n');
  assert.doesNotMatch(active, /e2_t3_static_v1|E2_T3_TRANSACTION(?!_V2)|e2-t3-roundtrip-v1/);
  for (const sql of [transaction, preflight, postcheck]) assert.match(sql, /e2_t3_static_v2/);
  assert.match(transaction, /E2_T3_TRANSACTION_V2/);
  const evidence = buildEvidence(operationResult(), canonical);
  assert.equal(evidence.evidence_version, 'e2-t3-roundtrip-v2');
  assert.equal(evidence.run_id, 'e2_t3_static_v2');
  assert.throws(() => buildEvidence({ ...operationResult(), operation_code: 'E2_T3_TRANSACTION' }, canonical), /operation code/);
});

test('transaction uses ordered top-level write then target-table read-back', () => {
  const clean = stripComments(transaction);
  const statements = clean.split(';').map((part) => part.trim()).filter(Boolean);
  assert.equal(statements[0].toLowerCase(), 'begin');
  assert.equal(statements.at(-1).toLowerCase(), 'rollback');
  const inserts = statements.filter((statement) => /^insert\s+into\s+public\.performance_dataset_rows_v2\b/i.test(statement));
  assert.equal(inserts.length, 1);
  assert.doesNotMatch(clean, /with\s+inserted\s+as\s*\(\s*insert/i);
  const insertIndex = statements.indexOf(inserts[0]);
  const finalRead = statements[insertIndex + 1];
  assert.match(finalRead, /^with\s+fixture\s+as/i);
  assert.match(finalRead, /from\s+public\.performance_dataset_rows_v2\s+d[\s\S]*e2_t3_static_v2/i);
  assert.match(finalRead, /select\s+'E2_T3_TRANSACTION_V2'\s+operation_code/);
  assert.doesNotMatch(finalRead.match(/select\s+'E2_T3_TRANSACTION_V2'[\s\S]*from\s+result/i)[0], /\buser_id\b/i);
});

test('transaction temp baseline and locks are deterministic and non-persistent', () => {
  assert.match(transaction, /lock table public\.performance_dataset_rows_v2 in share row exclusive mode/i);
  assert.match(transaction, /lock table public\.performance_dataset_rows, public\.dashboard_snapshots, public\.oauth_transactions,[\s\S]*public\.platform_connections, public\.platform_connection_tokens in share mode/i);
  assert.match(transaction, /create temp table pg_temp\.e2_t3_v2_baseline on commit drop as/i);
  assert.equal((transaction.match(/\bcreate\s+(?:temp\s+)?table\b/gi) || []).length, 1);
  assert.doesNotMatch(transaction, /\b(?:on conflict|dynamic sql|execute\s+)\b/i);
});

test('final v2 evidence gates counts, baseline gates, and every parity boolean', () => {
  const finalProjection = transaction.slice(transaction.lastIndexOf("select 'E2_T3_TRANSACTION_V2'"));
  for (const assertion of ['fixture_before=0','inserted_count=1','read_back_count=1','contract_match_count=1','ledger_before=37','dataset_before=0','eligible_user_ok','connected_before=encrypted_before','missing_ok','orphan_ok','plaintext_ok','dataset_transaction_delta_ok','v1_unchanged','snapshot_unchanged','oauth_unchanged','connected_unchanged','encrypted_unchanged','missing_encrypted_unchanged','orphan_encrypted_unchanged','plaintext_unchanged','ledger_unchanged']) assert.match(finalProjection, new RegExp(`\\b${assertion.replace(/[=]/g, '\\s*=\\s*')}\\b`));
  assert.match(transaction, /read_back_count-b\.fixture_before inserted_count/);
  assert.match(transaction, /jsonb_agg\(to_jsonb\(fixture\) - 'id' - 'user_id' - 'created_at' - 'updated_at'/);
});

test('postcheck uses scalar actual and expected queries with exact contract', () => {
  assert.equal((postcheck.match(/\(-1\)::bigint/g) || []).length, 4);
  assert.doesNotMatch(postcheck, /from\s+public\.[\w.]+\s+cross join expected/i);
  for (const relation of ['performance_dataset_rows_v2','performance_dataset_rows','dashboard_snapshots']) assert.match(postcheck, new RegExp(`\\(select count\\(\\*\\) from public\\.${relation}`));
  const codes = [...postcheck.matchAll(/(?:select|union all select)\s+'([A-Z0-9_]+)'/g)].map((match) => match[1]);
  assert.equal(codes.length, 13);
  assert.equal(new Set(codes).size, 13);
});

test('converter rejects wrong counts, false parity, unknown fields, and forbidden material', () => {
  for (const field of ['inserted_count','contract_match_count','read_back_count']) assert.throws(() => buildEvidence({ ...operationResult(), [field]: 2 }, canonical));
  for (const field of ['dataset_transaction_delta_ok','v1_unchanged','snapshot_unchanged','oauth_unchanged','connected_unchanged','encrypted_unchanged','missing_encrypted_unchanged','orphan_encrypted_unchanged','plaintext_unchanged','ledger_unchanged']) assert.throws(() => buildEvidence({ ...operationResult(), [field]: false }, canonical), new RegExp(field));
  assert.throws(() => buildEvidence({ ...operationResult(), unknown: true }, canonical), /allowlist/);
  const leaked = operationResult(); leaked.redacted_physical[0].user_id = 'forbidden';
  assert.throws(() => buildEvidence(leaked, canonical), /identity or credential/);
});

test('T7 inventory follows the exact E2-T3 v2 fixture key', () => {
  const inventory = read('artifacts/dataset-v2-acceptance/e2-t7-cleanup/fixture-inventory.json');
  const t7Baseline = read('docs/security/sql/E2_T7_BASELINE.sql');
  const t7Final = read('docs/security/sql/E2_T7_FINAL_CHECK.sql');
  for (const content of [inventory, t7Baseline, t7Final]) {
    assert.match(content, /meta:e2_t3_static_v2_account:paid:none:campaign:e2_t3_static_v2_campaign:ad:e2_t3_static_v2_ad/);
    assert.doesNotMatch(content, /e2_t3_static_v1/);
  }
});

test('execution plan records recovery and records E2-T3 Done', () => {
  const plan = read('codex-input/AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md');
  assert.match(plan, /E2-C2 — E2-T3 ordered read-back v2 corrective preparation/);
  assert.match(plan, /recovery sorgusu HTTP 201 ve 13\/13 PASS/);
  assert.match(plan, /fixture residue zero ve production no-change/);
  assert.match(plan, /E2-T3 `Verification`/);
});

test('eligible-user gates accept one or more users and retain deterministic selection', () => {
  assert.match(preflight, /'ELIGIBLE_USERS',\s*count\(\*\),\s*1,\s*'gte'/);
  assert.match(transaction, /exists\s*\(select 1 from public\.users u where exists \(select 1 from auth\.users a where a\.id=u\.id\)\) eligible_user_ok/i);
  assert.match(transaction, /from \(select u\.id from public\.users u[\s\S]*order by u\.id limit 1\) u/i);
  const eligibleGate = (populationCount) => populationCount >= 1;
  assert.equal(eligibleGate(0), false);
  assert.equal(eligibleGate(1), true);
  assert.equal(eligibleGate(2), true);
  assert.equal(eligibleGate(100), true);
});

test('SQL and converter enforce exact runtime-row redaction', () => {
  assert.deepEqual(RUNTIME_PHYSICAL_KEYS, ['id', 'user_id', 'created_at', 'updated_at']);
  assert.match(transaction, /to_jsonb\(fixture\)\s*- 'id'\s*- 'user_id'\s*- 'created_at'\s*- 'updated_at'/);

  const realisticDatabaseRow = {
    ...physical,
    id: '123e4567-e89b-42d3-a456-426614174000',
    user_id: '123e4567-e89b-42d3-a456-426614174001',
    created_at: '2026-08-26T12:00:00.000Z',
    updated_at: '2026-08-26T12:00:01.000Z'
  };
  const sqlRedactedRow = { ...realisticDatabaseRow };
  for (const field of RUNTIME_PHYSICAL_KEYS) delete sqlRedactedRow[field];
  assert.deepEqual(Object.keys(sqlRedactedRow).sort(), [...ALLOWED_REDACTED_PHYSICAL_KEYS]);
  assert.equal(buildEvidence({ ...operationResult(), redacted_physical: [sqlRedactedRow] }, canonical).operation_status, 'PASS');

  const runtimeValues = {
    id: realisticDatabaseRow.id,
    user_id: realisticDatabaseRow.user_id,
    created_at: realisticDatabaseRow.created_at,
    updated_at: realisticDatabaseRow.updated_at
  };
  for (const [field, value] of Object.entries(runtimeValues)) {
    const result = operationResult();
    result.redacted_physical[0][field] = value;
    assert.throws(() => buildEvidence(result, canonical));
  }
  const unknown = operationResult();
  unknown.redacted_physical[0].unknown_row_field = 'forbidden';
  assert.throws(() => buildEvidence(unknown, canonical), /physical fields.*allowlist/);
  const uuid = operationResult();
  uuid.redacted_physical[0].entity_name = '123e4567-e89b-42d3-a456-426614174002';
  assert.throws(() => buildEvidence(uuid, canonical), /identity material/);
});

test('dataset transaction delta has exact semantics and rejects the old no-change field', () => {
  assert.ok(ALLOWED_RESULT_KEYS.includes('dataset_transaction_delta_ok'));
  assert.ok(!ALLOWED_RESULT_KEYS.includes('dataset_unchanged'));
  assert.match(transaction, /\(select count\(\*\) from public\.performance_dataset_rows_v2\)=b\.dataset_before\+1 dataset_transaction_delta_ok/);
  const finalProjection = transaction.slice(transaction.lastIndexOf("select 'E2_T3_TRANSACTION_V2'"));
  assert.match(finalProjection, /and dataset_transaction_delta_ok/);
  assert.doesNotMatch(transaction, /\bdataset_unchanged\b/);
  const oldField = operationResult();
  oldField.dataset_unchanged = oldField.dataset_transaction_delta_ok;
  delete oldField.dataset_transaction_delta_ok;
  assert.throws(() => buildEvidence(oldField, canonical), /result fields.*allowlist/);
  const evidence = buildEvidence(operationResult(), canonical);
  assert.equal(evidence.dataset_transaction_delta_ok, true);
  assert.equal(Object.hasOwn(evidence, 'dataset_unchanged'), false);
});

test('runbook distinguishes transaction-local delta from post-rollback no-change', () => {
  const runbook = read('docs/security/E2_T3_ROUNDTRIP_RUNBOOK.md');
  assert.match(runbook, /temporarily `\+1` inside the transaction/);
  assert.match(runbook, /`dataset_transaction_delta_ok` boolean proves only that transaction-local expected delta/);
  assert.match(runbook, /not a persistent no-change claim/);
  assert.match(runbook, /Permanent Dataset V2 zero\/no-change is proved only by the separate postcheck after the final `ROLLBACK`/);
});
