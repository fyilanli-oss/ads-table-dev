const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const contract = JSON.parse(
  fs.readFileSync(
    path.join(__dirname, '..', 'contracts', 'r6d4a-google-ads-connection-lifecycle-v1.json'),
    'utf8',
  ),
);

test('R6-D4-A reuses completed E5 work and keeps non-active Google products parked', () => {
  assert.ok(contract.existing_completed_work.reuse_without_redevelopment.some(item => item.includes('E5 Google Ads Standard')));
  assert.equal(contract.existing_completed_work.google_sheets_status, 'parked');
  assert.equal(contract.existing_completed_work.ga4_organic_status, 'parked');
  assert.equal(contract.legacy_disposition.reuse_standalone_token_for_embedded_connection, false);
  assert.equal(contract.legacy_disposition.delete_legacy_history, false);
});

test('R6-D4-A requires durable server-side Google token renewal', () => {
  assert.equal(contract.oauth_entry.offline_access_required, true);
  assert.equal(contract.oauth_entry.refresh_token_required_before_canonical_save, true);
  assert.equal(contract.oauth_entry.callback_may_mark_connected, false);
  assert.equal(contract.token_lifecycle.server_side_refresh_required, true);
  assert.equal(contract.token_lifecycle.refresh_token_rotation_must_be_persisted_when_returned, true);
  assert.equal(contract.token_lifecycle.invalid_grant_or_missing_refresh_token_result, 'reauthorization_required');
  assert.equal(contract.token_lifecycle.browser_token_exposure_allowed, false);
});

test('R6-D4-A preserves verified manager context for 1 to 3 selected ad accounts', () => {
  assert.equal(contract.account_selection.minimum, 1);
  assert.equal(contract.account_selection.maximum, 3);
  assert.equal(contract.account_selection.manager_accounts_selectable, false);
  assert.equal(contract.account_selection.server_side_refetch_required_on_save, true);
  assert.deepEqual(contract.account_selection.required_canonical_fields_per_account, [
    'id',
    'name',
    'currency',
    'login_customer_id',
  ]);
  assert.ok(contract.fail_closed_rules.includes('selected_account_without_verified_login_customer_id_does_not_mark_connected'));
});

test('R6-D4-A separates workspace reporting currency from Google account currency', () => {
  assert.equal(contract.currency_policy.workspace_reporting_currency_source, 'merchant_selected_workspace_setting');
  assert.equal(contract.currency_policy.provider_source_currency_source, 'verified Google Ads customer metadata');
  assert.equal(contract.currency_policy.shopify_currency_allowed, false);
});

test('R6-D4-A defines independent local-only Google Ads disconnect', () => {
  assert.equal(contract.disconnect_policy.independent_google_ads_disconnect_required, true);
  assert.equal(contract.disconnect_policy.provider_global_revoke_allowed, false);
  assert.equal(contract.disconnect_policy.local_credentials_removed, true);
  assert.equal(contract.disconnect_policy.selected_accounts_removed, true);
  assert.equal(contract.disconnect_policy.historical_analytics_preserved, true);
  assert.equal(contract.disconnect_policy.clean_reconnect_required, true);
});

test('R6-D4-A remains contract-only and cannot activate production data paths', () => {
  assert.equal(contract.production_mutation, false);
  assert.equal(contract.provider_contact, false);
  assert.equal(contract.dataset_v2_write, false);
  assert.ok(contract.excluded_from_r6_d4_a.includes('schedule_or_backfill_activation'));
  assert.ok(contract.excluded_from_r6_d4_a.includes('E5_adapter_mapper_time_fx_or_writer_redevelopment'));
});

