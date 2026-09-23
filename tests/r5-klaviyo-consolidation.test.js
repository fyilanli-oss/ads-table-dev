'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.join(__dirname, '..');
const read = file => fs.readFileSync(path.join(root, file), 'utf8');

test('R5 contract requires explicit binding, provider verification and non-destructive consolidation', () => {
  const contract = JSON.parse(read('contracts/r5-klaviyo-consolidation-v1.json'));
  assert.equal(contract.authority.tenant_key, 'workspace_id');
  assert.deepEqual(contract.gates.map(gate => gate.id), ['R5-A', 'R5-B', 'R5-C']);
  for (const forbidden of [
    'derive_workspace_from_email',
    'derive_workspace_from_provider_account_id',
    'auto_bind_single_remaining_workspace',
    'provider_revoke_before_acceptance',
    'delete_legacy_token_envelopes'
  ]) assert.ok(contract.forbidden.includes(forbidden));
});

test('R5-A migration is additive, empty and server-only', () => {
  const sql = read('supabase/migrations/20260923132407_create_legacy_user_workspace_bindings.sql');
  assert.match(sql, /create table public\.legacy_user_workspace_bindings/i);
  assert.match(sql, /evidence_type text not null check \(evidence_type = 'human_attested'\)/i);
  assert.match(sql, /create unique index legacy_user_workspace_one_active_idx[\s\S]*where status = 'active'/i);
  assert.match(sql, /enable row level security/i);
  assert.match(sql, /force row level security/i);
  assert.match(sql, /revoke all[\s\S]*anon, authenticated/i);
  assert.match(sql, /grant select, insert, update, delete[\s\S]*service_role/i);
  assert.doesNotMatch(sql, /insert into|update public\.|delete from|oauth\/revoke|account_id\s*=/i);
});

test('R5 evidence and runbook do not claim account match is tenant authority', () => {
  const doc = read('docs/R5_KLAVIYO_CONNECTION_CONSOLIDATION.md');
  const evidence = JSON.parse(read('docs/security/evidence/R5A_KLAVIYO_CONSOLIDATION_PREFLIGHT_2026-09-23.json'));
  assert.match(doc, /tek başına[\s\S]*kanıtlamaz/i);
  assert.match(doc, /Provider revoke yok/i);
  assert.equal(evidence.observations.provider_contact_performed, false);
  assert.equal(evidence.observations.automatic_binding_performed, false);
  assert.equal(evidence.counts.canonical_klaviyo_rows, 0);
});
