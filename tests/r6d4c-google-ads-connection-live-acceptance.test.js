'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const read = relative => fs.readFileSync(path.join(root, relative), 'utf8');

test('R6-D4-C records a redacted three-account production acceptance without data activation', () => {
  const evidenceText = read('docs/security/evidence/R6D4C_GOOGLE_ADS_CONNECTION_LIVE_ACCEPTANCE_2026-09-26.json');
  const evidence = JSON.parse(evidenceText);
  assert.equal(evidence.production_release.pull_request, 276);
  assert.equal(evidence.production_release.deployment_status, 'READY');
  assert.equal(evidence.merchant_acceptance.selected_account_count, 3);
  assert.equal(evidence.merchant_acceptance.connected_ui, 'PASS');
  assert.equal(evidence.merchant_acceptance.reload_persistence, 'PASS');
  assert.equal(evidence.runtime_evidence.accounts_request_status, 200);
  assert.equal(evidence.runtime_evidence.selection_request_status, 200);
  assert.equal(evidence.supabase_read_only_postcheck.result, 'PASS');
  assert.equal(evidence.supabase_read_only_postcheck.connected_rows, 1);
  assert.equal(evidence.supabase_read_only_postcheck.selected_accounts_shape_valid, true);
  assert.equal(evidence.supabase_read_only_postcheck.credential_envelopes_present, true);
  assert.equal(evidence.supabase_read_only_postcheck.dataset_v2_google_ads_rows, 0);
  assert.equal(evidence.scope.dataset_v2_write, false);
  assert.equal(evidence.scope.existing_e5_runtime_redeveloped, false);
  assert.equal(evidence.next_gate, 'R6-D4-D_GOOGLE_ADS_READ_ONLY_PREFLIGHT_ANALYST_BRIEF');
  assert.doesNotMatch(evidenceText, /customer[_ -]?id|workspace[_ -]?id|user[_ -]?id|ciphertext|refresh[_ -]?token|access[_ -]?token/i);
});

test('Execution Plan and runtime record R6-D4-C as connection-only PASS with R6-D4-D next', () => {
  const plan = read('codex-input/AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md');
  const runtime = read('docs/R6_WORKSPACE_PROVIDER_RUNTIME.md');
  const decision = read('docs/R6D4A_GOOGLE_ADS_CONNECTION_LIFECYCLE_DECISION.md');
  assert.match(plan, /R6-D4-C Google Ads bağlantı ve hesap seçimi — Production merchant acceptance PASS/);
  assert.match(plan, /Connected · 3 accounts/);
  assert.match(plan, /Dataset V2 Google Ads satırı `0`/);
  assert.match(plan, /R6-D4-D salt-okunur Google Ads preflight analist brief/);
  assert.match(runtime, /R6-D4-C Google Ads bağlantı ve üç hesap seçimi — production PASS/);
  assert.match(decision, /R6-D4-C production merchant acceptance PASS/);
});
