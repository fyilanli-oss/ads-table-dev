"use strict";

const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const assert = require("node:assert/strict");
const {createShopifyInstallService, createShopifyTenantResolver} = require("../src/shopify/install-service");

const migration = fs.readFileSync(path.join(__dirname, "../supabase/migrations/20260910100000_create_shopify_managed_installations.sql"), "utf8");

test("Shopify installation schema is server-only and stores envelope columns", () => {
  assert.match(migration, /create table if not exists public\.shopify_installations/i);
  assert.match(migration, /workspace_id uuid not null unique default gen_random_uuid\(\)/i);
  assert.match(migration, /access_token_envelope jsonb not null/i);
  assert.doesNotMatch(migration, /\baccess_token\s+text\b|\brefresh_token\s+text\b/i);
  for (const role of ["public", "anon", "authenticated"])
    assert.match(migration, new RegExp(`revoke all on table public\\.shopify_installations from[^;]*${role}`, "i"));
  assert.match(migration, /grant select, insert, update, delete on table public\.shopify_installations to service_role/i);
  assert.match(migration, /force row level security/i);
});

test("managed install RPC is one idempotent atomic upsert and service-role only", () => {
  assert.match(migration, /create or replace function public\.complete_shopify_managed_install/i);
  assert.match(migration, /on conflict on constraint shopify_installations_pkey do update/i);
  assert.match(migration, /returning installation\.shop_id, installation\.shop_domain, installation\.workspace_id, installation\.status/i);
  assert.match(migration, /revoke all on function public\.complete_shopify_managed_install[^;]+from public, anon, authenticated/i);
  assert.match(migration, /grant execute on function public\.complete_shopify_managed_install[^;]+to service_role/i);
});

test("install service encrypts before RPC and never sends plaintext tokens", async () => {
  let parameters;
  const client = {rpc: async (name, input) => {
    assert.equal(name, "complete_shopify_managed_install");
    parameters = input;
    return {data: {shop_id: "gid://shopify/Shop/1", workspace_id: "workspace-1", status: "active"}, error: null};
  }};
  const vault = {encrypt: (token, context) => ({version: "v1", keyId: "key", iv: "iv", tag: "tag", ciphertext: `${context.tokenType}:${token.length}`})};
  const service = createShopifyInstallService({client, vault});
  assert.deepEqual(await service.complete({
    shop: {authority: "shopify_verified", shop_id: "gid://shopify/Shop/1", shop_domain: "store.myshopify.com"},
    shopify_user_id: "user-1",
    token_material: {access_token: "sensitive-access", refresh_token: "sensitive-refresh", expires_at: 2000, refresh_token_expires_at: 3000, scope: "read_reports"},
  }), {status: "active", shop_id: "gid://shopify/Shop/1", workspace_id: "workspace-1", token_encrypted: true});
  assert.equal(parameters.p_access_token_envelope.ciphertext, "access:16");
  assert.equal(parameters.p_refresh_token_envelope.ciphertext, "refresh:17");
  assert.doesNotMatch(JSON.stringify(parameters), /sensitive/);
});

test("tenant resolver accepts only the canonical stored domain", async () => {
  const chain = {select: () => chain, eq: (_field, domain) => { assert.equal(domain, "store.myshopify.com"); return chain; }, maybeSingle: async () => ({data: {shop_id: "shop-1", shop_domain: "store.myshopify.com", workspace_id: "workspace-1", status: "active"}, error: null})};
  const resolver = createShopifyTenantResolver({client: {from: (table) => { assert.equal(table, "shopify_installations"); return chain; }}});
  assert.equal((await resolver({shop_domain: "STORE.myshopify.com"})).workspace_id, "workspace-1");
});

test("server composition registers managed Shopify runtime dependencies", () => {
  const server = fs.readFileSync(path.join(__dirname, "../server.js"), "utf8");
  const runtime = fs.readFileSync(path.join(__dirname, "../src/shopify/runtime.js"), "utf8");
  assert.match(server, /registerShopifyRuntime\(\{app,env:process\.env,supabaseAdmin\}\)/);
  assert.match(runtime, /const config = loadShopifyConfig\(env\)/);
  assert.match(runtime, /createShopifyInstallService\(\{client: supabaseAdmin, vault\}\)/);
  assert.match(runtime, /registerShopifyAuthRoutes\(app,/);
});
