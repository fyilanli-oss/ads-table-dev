'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const read = relative => fs.readFileSync(path.join(root, relative), 'utf8');
const contract = JSON.parse(read('contracts/r6d3b-meta-token-validation-v1.json'));

test('R6-D3-B requires the complete server-side Meta token validation sequence', () => {
  assert.deepEqual(contract.token_sequence, [
    'authorization_code_to_short_lived_user_token',
    'short_lived_to_long_lived_user_token',
    'debug_long_lived_token',
    'validate_app_scope_and_expiry',
    'persist_encrypted_pending_connection',
  ]);
  assert.deepEqual(contract.required_validation.required_scopes, ['ads_read']);
  assert.equal(contract.required_validation.configured_app_id_match, true);
  assert.equal(contract.required_validation.known_future_expiry, true);
});

test('R6-D3-B preserves pending selection and forbids invented refresh authority', () => {
  assert.equal(contract.persistence.refresh_token_expected, false);
  assert.equal(contract.persistence.status_after_callback, 'pending_account_selection');
  assert.equal(contract.account_selection.minimum, 1);
  assert.equal(contract.account_selection.maximum, 3);
  assert.equal(contract.account_selection.connected_before_selection, false);
});

test('R6-D3-B is repository-only and cannot activate Meta data movement', () => {
  assert.equal(contract.production_mutation, false);
  assert.equal(contract.provider_contact, false);
  assert.equal(contract.dataset_v2_write, false);
  for (const item of ['production_deployment', 'production_meta_oauth', 'provider_performance_read', 'schedule_or_backfill']) {
    assert.ok(contract.excluded.includes(item));
  }
});
