"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const migration = fs.readFileSync(path.join(__dirname, "..", "supabase/migrations/20260911130000_add_embedded_oauth_authority.sql"), "utf8");

test("embedded OAuth authority is an exclusive alternative to standalone auth user", () => {
  assert.match(migration, /alter column user_id drop not null/i);
  assert.match(migration, /surface = 'standalone' and user_id is not null[\s\S]*surface = 'shopify_embedded' and user_id is null/i);
  for (const field of ["shop_id", "workspace_id", "shopify_user_id", "return_target"]) {
    assert.match(migration, new RegExp(`add column ${field}\\b`, "i"));
  }
});

test("embedded shop and workspace must match one persisted installation", () => {
  assert.match(migration, /unique \(shop_id, workspace_id\)/i);
  assert.match(migration, /foreign key \(shop_id, workspace_id\)[\s\S]*references public\.shopify_installations \(shop_id, workspace_id\)/i);
  assert.match(migration, /return_target = '\/shopify\/app\/platforms'/i);
});

test("atomic consume returns the complete authority and retains server-only execution", () => {
  assert.match(migration, /delete from public\.oauth_transactions[\s\S]*returning[\s\S]*oauth_transactions\.surface[\s\S]*oauth_transactions\.return_target/i);
  assert.match(migration, /revoke all on function public\.consume_oauth_transaction\(text, text, text\) from public, anon, authenticated/i);
  assert.match(migration, /grant execute on function public\.consume_oauth_transaction\(text, text, text\) to service_role/i);
  assert.doesNotMatch(migration, /\bcommit\b|\btruncate\b/i);
});
