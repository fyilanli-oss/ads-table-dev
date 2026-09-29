'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const migration = fs.readFileSync(path.resolve(
  __dirname,
  '../supabase/migrations/20260929122743_create_klaviyo_estimated_email_spend_history.sql',
), 'utf8');

test('R7 B5 spend history is workspace/account scoped and stores effective changes, not materialized windows', () => {
  assert.match(migration, /create table public\.workspace_provider_email_spend_history/);
  assert.match(migration, /primary key \(workspace_id, provider, provider_account_id, effective_from\)/);
  assert.match(migration, /estimated_30_day_email_spend numeric\(10,2\)/);
  assert.match(migration, /provenance text not null default 'user_estimated'/);
  assert.doesNotMatch(migration, /window_end|billing_period_end|sent_email_count/i);
});

test('R7 B5 spend history remains server-only under forced RLS', () => {
  assert.match(migration, /enable row level security/);
  assert.match(migration, /force row level security/);
  assert.match(migration, /revoke all on table public\.workspace_provider_email_spend_history from public, anon, authenticated/);
  assert.match(migration, /grant select, insert, update, delete on table public\.workspace_provider_email_spend_history to service_role/);
  assert.match(migration, /revoke all on function public\.complete_klaviyo_connection_with_spend_history[\s\S]*from public, anon, authenticated/);
  assert.match(migration, /grant execute on function public\.correct_klaviyo_estimated_30_day_email_spend[\s\S]*to service_role/);
});

test('R7 B5 functions preserve optimistic connection versioning and explicit correction semantics', () => {
  assert.match(migration, /connection_version = p_expected_version \+ 1/);
  assert.match(migration, /raise exception 'CONNECTION_CHANGED'/);
  assert.match(migration, /raise exception 'DUPLICATE_EFFECTIVE_START'/);
  assert.match(migration, /raise exception 'SPEND_HISTORY_ENTRY_NOT_FOUND'/);
  assert.match(migration, /correction_version = correction_version \+ 1/);
  assert.match(migration, /when v_latest = p_effective_from then p_estimated_30_day_email_spend/);
});
