"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const {
  createShopWorkspaceBinding,
  reconcileVerifiedInstall,
  validateOneShopPerWorkspace,
  deriveTenantContext,
} = require("../src/shopify/tenant-model");

const verifiedShop = (shop_id, shop_domain) => ({authority: "shopify_verified", shop_id, shop_domain});
const create = (shop_id = "shop-stable-1", workspace_id = "workspace-1") => createShopWorkspaceBinding({
  shop: verifiedShop(shop_id, "store-one.myshopify.com"),
  workspace_id,
  installed_at: "2026-09-08T12:00:00.000Z",
});

test("E10 T2 creates an immutable one-shop workspace binding from verified identity", () => {
  const binding = create();
  assert.deepEqual(binding, {
    contract_version: "e10-t2-v1", shop_id: "shop-stable-1", shop_domain: "store-one.myshopify.com",
    workspace_id: "workspace-1", status: "active", install_generation: 1,
    installed_at: "2026-09-08T12:00:00.000Z", last_verified_at: "2026-09-08T12:00:00.000Z", domain_history: [],
  });
  assert.equal(Object.isFrozen(binding), true);
  assert.equal(Object.isFrozen(binding.domain_history), true);
  assert.throws(() => createShopWorkspaceBinding({shop: {shop_id: "forged", shop_domain: "x.myshopify.com"}, workspace_id: "w", installed_at: "2026-09-08T12:00:00.000Z"}), /Shopify verification/);
});

test("stable shop identity survives a verified domain change and reinstall", () => {
  const original = create();
  const reinstalled = reconcileVerifiedInstall(original, {
    shop: verifiedShop("shop-stable-1", "renamed-store.myshopify.com"),
    verified_at: "2026-09-09T12:00:00.000Z",
  });
  assert.equal(reinstalled.shop_id, original.shop_id);
  assert.equal(reinstalled.workspace_id, original.workspace_id);
  assert.equal(reinstalled.shop_domain, "renamed-store.myshopify.com");
  assert.equal(reinstalled.install_generation, 2);
  assert.deepEqual(reinstalled.domain_history, [{shop_domain: "store-one.myshopify.com", replaced_at: "2026-09-09T12:00:00.000Z"}]);
  assert.throws(() => reconcileVerifiedInstall(original, {shop: verifiedShop("other-shop", "other.myshopify.com"), verified_at: "2026-09-09T12:00:00.000Z"}), /SHOP_IDENTITY_CONFLICT/);
});

test("registry enforces one shop per workspace in the first release", () => {
  assert.equal(validateOneShopPerWorkspace([create(), create("shop-stable-2", "workspace-2")]), true);
  assert.throws(() => validateOneShopPerWorkspace([create(), create("shop-stable-2", "workspace-1")]), /WORKSPACE_ALREADY_BOUND/);
  assert.throws(() => validateOneShopPerWorkspace([create(), create("shop-stable-1", "workspace-2")]), /DUPLICATE_SHOP_BINDING/);
});

test("browser tenant claims never override verified server-side authority", () => {
  const binding = create();
  assert.deepEqual(deriveTenantContext({binding, shop: verifiedShop("shop-stable-1", "store-one.myshopify.com"), caller_claims: {workspace_id: "workspace-1"}}), {
    authority: "shopify_verified", shop_id: "shop-stable-1", workspace_id: "workspace-1",
  });
  assert.throws(() => deriveTenantContext({binding, shop: verifiedShop("shop-stable-1", "store-one.myshopify.com"), caller_claims: {workspace_id: "attacker-workspace"}}), /CALLER_TENANT_CLAIM_REJECTED/);
  assert.throws(() => deriveTenantContext({binding, shop: verifiedShop("other-shop", "other.myshopify.com")}), /SHOP_TENANT_MISMATCH/);
});

test("domains and temporal reconciliation fail closed", () => {
  assert.throws(() => createShopWorkspaceBinding({shop: verifiedShop("s", "merchant.example.com"), workspace_id: "w", installed_at: "2026-09-08T12:00:00.000Z"}), /myshopify.com/);
  assert.throws(() => reconcileVerifiedInstall(create(), {shop: verifiedShop("shop-stable-1", "store-one.myshopify.com"), verified_at: "2026-09-07T12:00:00.000Z"}), /STALE_SHOP_VERIFICATION/);
});
