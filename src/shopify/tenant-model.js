"use strict";

const CONTRACT_VERSION = "e10-t2-v1";
const VERIFIED_AUTHORITY = "shopify_verified";
const ACTIVE_STATUS = "active";

function requiredOpaqueId(value, field) {
  if (typeof value !== "string" || value.trim() !== value || value.length === 0 || value.length > 255) {
    throw new TypeError(`${field} must be a non-empty canonical string`);
  }
  return value;
}

function normalizeShopDomain(value) {
  if (typeof value !== "string") throw new TypeError("shop domain must be a string");
  const domain = value.trim().toLowerCase().replace(/\.$/, "");
  if (!/^[a-z0-9][a-z0-9-]*\.myshopify\.com$/.test(domain)) {
    throw new TypeError("shop domain must be a canonical myshopify.com domain");
  }
  return domain;
}

function verifiedShopIdentity(input) {
  if (!input || input.authority !== VERIFIED_AUTHORITY) {
    throw new TypeError("shop identity must come from Shopify verification");
  }
  return Object.freeze({
    shop_id: requiredOpaqueId(input.shop_id, "shop_id"),
    shop_domain: normalizeShopDomain(input.shop_domain),
  });
}

function timestamp(value, field) {
  const date = new Date(value);
  if (typeof value !== "string" || Number.isNaN(date.valueOf()) || date.toISOString() !== value) {
    throw new TypeError(`${field} must be an ISO timestamp`);
  }
  return value;
}

function freezeBinding(binding) {
  return Object.freeze({
    ...binding,
    domain_history: Object.freeze(binding.domain_history.map((entry) => Object.freeze({...entry}))),
  });
}

function createShopWorkspaceBinding({shop, workspace_id, installed_at}) {
  const identity = verifiedShopIdentity(shop);
  const installedAt = timestamp(installed_at, "installed_at");
  return freezeBinding({
    contract_version: CONTRACT_VERSION,
    shop_id: identity.shop_id,
    shop_domain: identity.shop_domain,
    workspace_id: requiredOpaqueId(workspace_id, "workspace_id"),
    status: ACTIVE_STATUS,
    install_generation: 1,
    installed_at: installedAt,
    last_verified_at: installedAt,
    domain_history: [],
  });
}

function assertBinding(binding) {
  if (!binding || binding.contract_version !== CONTRACT_VERSION || binding.status !== ACTIVE_STATUS) {
    throw new TypeError("active E10-T2 binding required");
  }
  requiredOpaqueId(binding.shop_id, "binding.shop_id");
  requiredOpaqueId(binding.workspace_id, "binding.workspace_id");
  normalizeShopDomain(binding.shop_domain);
  timestamp(binding.installed_at, "binding.installed_at");
  timestamp(binding.last_verified_at, "binding.last_verified_at");
  if (!Number.isSafeInteger(binding.install_generation) || binding.install_generation < 1) {
    throw new TypeError("binding install_generation is invalid");
  }
  if (!Array.isArray(binding.domain_history)) throw new TypeError("binding domain_history is invalid");
}

function reconcileVerifiedInstall(binding, {shop, verified_at}) {
  assertBinding(binding);
  const identity = verifiedShopIdentity(shop);
  const verifiedAt = timestamp(verified_at, "verified_at");
  if (identity.shop_id !== binding.shop_id) throw new Error("SHOP_IDENTITY_CONFLICT");
  if (Date.parse(verifiedAt) < Date.parse(binding.last_verified_at)) throw new Error("STALE_SHOP_VERIFICATION");
  const domainChanged = identity.shop_domain !== binding.shop_domain;
  return freezeBinding({
    ...binding,
    shop_domain: identity.shop_domain,
    install_generation: binding.install_generation + 1,
    last_verified_at: verifiedAt,
    domain_history: domainChanged
      ? [...binding.domain_history, {shop_domain: binding.shop_domain, replaced_at: verifiedAt}]
      : [...binding.domain_history],
  });
}

function validateOneShopPerWorkspace(bindings) {
  if (!Array.isArray(bindings)) throw new TypeError("bindings must be an array");
  const shops = new Set();
  const workspaces = new Set();
  for (const binding of bindings) {
    assertBinding(binding);
    if (shops.has(binding.shop_id)) throw new Error("DUPLICATE_SHOP_BINDING");
    if (workspaces.has(binding.workspace_id)) throw new Error("WORKSPACE_ALREADY_BOUND");
    shops.add(binding.shop_id);
    workspaces.add(binding.workspace_id);
  }
  return true;
}

function deriveTenantContext({binding, shop, caller_claims = {}}) {
  assertBinding(binding);
  const identity = verifiedShopIdentity(shop);
  if (identity.shop_id !== binding.shop_id) throw new Error("SHOP_TENANT_MISMATCH");
  if (caller_claims && typeof caller_claims !== "object") throw new TypeError("caller_claims must be an object");
  for (const [field, authoritative] of [["shop_id", binding.shop_id], ["workspace_id", binding.workspace_id]]) {
    if (caller_claims[field] !== undefined && caller_claims[field] !== authoritative) {
      throw new Error("CALLER_TENANT_CLAIM_REJECTED");
    }
  }
  return Object.freeze({
    authority: VERIFIED_AUTHORITY,
    shop_id: binding.shop_id,
    workspace_id: binding.workspace_id,
  });
}

module.exports = Object.freeze({
  CONTRACT_VERSION,
  VERIFIED_AUTHORITY,
  normalizeShopDomain,
  createShopWorkspaceBinding,
  reconcileVerifiedInstall,
  validateOneShopPerWorkspace,
  deriveTenantContext,
});
