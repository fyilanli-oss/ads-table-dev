const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const contract = JSON.parse(
  fs.readFileSync(
    path.join(__dirname, '..', 'contracts', 'r6d3a-meta-connection-lifecycle-v1.json'),
    'utf8',
  ),
);

test('R6-D3-A preserves legacy Meta history without reusing its expired authority', () => {
  assert.equal(contract.legacy_disposition.migrate_to_workspace, false);
  assert.equal(contract.legacy_disposition.delete_legacy_connection, false);
  assert.equal(contract.legacy_disposition.reuse_legacy_access_token, false);
  assert.equal(contract.legacy_disposition.legacy_history_preserved, true);
  assert.equal(contract.legacy_disposition.legacy_schedule_must_remain_inactive, true);
  assert.equal(contract.legacy_disposition.automatic_legacy_job_allowed, false);
});

test('R6-D3-A requires validated long-lived Meta authority and no invented refresh token', () => {
  assert.equal(contract.token_lifecycle.server_side_long_lived_exchange_required, true);
  assert.equal(contract.token_lifecycle.refresh_token_expected, false);
  assert.equal(contract.token_lifecycle.invented_refresh_token_logic_forbidden, true);
  assert.deepEqual(contract.token_lifecycle.validation_before_save, [
    'token_is_valid',
    'token_belongs_to_configured_meta_app',
    'required_scopes_are_present',
    'expiry_is_known_and_in_the_future',
  ]);
  assert.equal(contract.token_lifecycle.merchant_action_label, 'Reconnect Meta');
});

test('R6-D3-A cannot mark Meta connected before 1 to 3 verified accounts are selected', () => {
  assert.equal(contract.oauth_entry.callback_state_after_token_validation, 'pending_account_selection');
  assert.equal(contract.oauth_entry.callback_may_mark_connected, false);
  assert.equal(contract.account_selection.minimum, 1);
  assert.equal(contract.account_selection.maximum, 3);
  assert.equal(contract.account_selection.server_side_refetch_required, true);
  assert.equal(
    contract.account_selection.connected_transition,
    'only_after_successful_verified_account_selection',
  );
});

test('R6-D3-A remains contract-only and leaves data activation to later gates', () => {
  assert.equal(contract.production_mutation, false);
  assert.equal(contract.provider_contact, false);
  assert.equal(contract.dataset_v2_write, false);
  assert.ok(contract.excluded_until_later_gate.includes('schedule_or_backfill'));
  assert.ok(contract.excluded_until_later_gate.includes('production_meta_oauth'));
  assert.equal(
    Object.values(contract.acceptance_questions).every(Boolean),
    true,
  );
});
