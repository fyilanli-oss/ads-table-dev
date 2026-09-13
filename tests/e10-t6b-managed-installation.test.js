"use strict";

const crypto = require("node:crypto");
const test = require("node:test");
const assert = require("node:assert/strict");
const {bootstrapManagedInstallation} = require("../src/shopify/managed-installation");

const secret = "development-secret";
const client = "development-client";

function sessionToken() {
  const header = {alg: "HS256", typ: "JWT"};
  const payload = {aud: client, dest: "https://store.myshopify.com/", iss: "https://store.myshopify.com/admin", sub: "user-1", sid: "session-1", nbf: 990, exp: 1100};
  const parts = [header, payload].map((value) => Buffer.from(JSON.stringify(value)).toString("base64url"));
  return `${parts.join(".")}.${crypto.createHmac("sha256", secret).update(parts.join(".")).digest("base64url")}`;
}

function dependencies(overrides = {}) {
  return {
    session_token: sessionToken(),
    auth_config: {client_id: client, client_secret: secret, now_seconds: 1000},
    exchange_client: {exchange: async (request) => {
      assert.equal(request.subject_token_type, "urn:ietf:params:oauth:token-type:id_token");
      assert.equal(request.requested_token_type, "urn:shopify:params:oauth:token-type:offline-access-token");
      return {access_token: "sensitive-access-token", expires_at: 2000, refresh_token: "sensitive-refresh-token", refresh_token_expires_at: 3000, scope: ""};
    }},
    admin_client: {getShopIdentity: async ({shop_domain, access_token}) => {
      assert.equal(access_token, "sensitive-access-token");
      return {authority: "shopify_verified_admin", shop_id: "gid://shopify/Shop/1", shop_domain};
    }},
    install_service: {complete: async ({shop, token_material}) => {
      assert.equal(token_material.access_token, "sensitive-access-token");
      return {status: "active", shop_id: shop.shop_id, workspace_id: "workspace-1", token_encrypted: true};
    }},
    ...overrides,
  };
}

test("managed installation bootstraps from a verified ID token without an OAuth callback", async () => {
  const result = await bootstrapManagedInstallation(dependencies());
  assert.deepEqual(result, {contract_version: "e10-t6b-managed-install-v1", status: "active", workspace_ready: true, reauthorization_required: false});
  assert.doesNotMatch(JSON.stringify(result), /token|shop_id|workspace_id|scope/i);
});

test("managed installation rejects an unverified Admin shop identity", async () => {
  await assert.rejects(() => bootstrapManagedInstallation(dependencies({
    admin_client: {getShopIdentity: async () => ({authority: "browser_query", shop_id: "forged", shop_domain: "store.myshopify.com"})},
  })), /SHOP_IDENTITY_VERIFICATION_FAILED/);
});

test("managed installation activates a binding only after atomic encrypted persistence", async () => {
  await assert.rejects(() => bootstrapManagedInstallation(dependencies({
    install_service: {complete: async () => ({status: "active", shop_id: "gid://shopify/Shop/1", workspace_id: "workspace-1", token_encrypted: false})},
  })), /MANAGED_INSTALL_COMPLETION_FAILED/);
});
