"use strict";

const CONTRACT_VERSION = "e10-t4-v1";
const EVENTS = Object.freeze(["uninstall", "access_revoked", "customers_data_request", "customers_redact", "shop_redact"]);
const ACTIONS = Object.freeze({
  uninstall: Object.freeze(["disable_access", "remove_shop_tokens", "record_retention_decision"]),
  access_revoked: Object.freeze(["disable_access", "remove_shop_tokens", "record_retention_decision"]),
  customers_data_request: Object.freeze(["prepare_customer_data_report"]),
  customers_redact: Object.freeze(["delete_customer_personal_data", "record_deletion_evidence"]),
  shop_redact: Object.freeze(["disable_access", "remove_shop_tokens", "delete_shop_personal_data", "record_deletion_evidence"]),
});

function required(value, field) {
  if (typeof value !== "string" || value.trim() !== value || value.length === 0 || value.length > 255) throw new TypeError(`${field} is invalid`);
  return value;
}

function planVerifiedLifecycleEvent({authority, event, shop_id, event_id, occurred_at, subject_ref = null}) {
  if (authority !== "shopify_verified_webhook" && authority !== "shopify_verified_admin") throw new Error("UNVERIFIED_SHOPIFY_LIFECYCLE_EVENT");
  if (!EVENTS.includes(event)) throw new Error("UNKNOWN_SHOPIFY_LIFECYCLE_EVENT");
  const occurred = new Date(occurred_at);
  if (typeof occurred_at !== "string" || Number.isNaN(occurred.valueOf()) || occurred.toISOString() !== occurred_at) throw new TypeError("occurred_at is invalid");
  if ((event === "customers_data_request" || event === "customers_redact") && !subject_ref) throw new Error("CUSTOMER_SUBJECT_REQUIRED");
  if (!(event === "customers_data_request" || event === "customers_redact") && subject_ref !== null) throw new Error("CUSTOMER_SUBJECT_FORBIDDEN");
  return Object.freeze({
    contract_version: CONTRACT_VERSION,
    event,
    shop_id: required(shop_id, "shop_id"),
    event_id: required(event_id, "event_id"),
    occurred_at,
    subject_ref: subject_ref === null ? null : required(subject_ref, "subject_ref"),
    actions: ACTIONS[event],
  });
}

async function executeLifecyclePlan(plan, {event_store, operations}) {
  if (!plan || plan.contract_version !== CONTRACT_VERSION || !ACTIONS[plan.event] || plan.actions !== ACTIONS[plan.event]) throw new Error("INVALID_LIFECYCLE_PLAN");
  if (!event_store || typeof event_store.claim !== "function" || typeof event_store.complete !== "function") throw new TypeError("event_store is invalid");
  if (!operations || typeof operations !== "object") throw new TypeError("operations is invalid");
  const claimed = await event_store.claim({shop_id: plan.shop_id, event_id: plan.event_id});
  if (claimed !== true) throw new Error("LIFECYCLE_EVENT_REPLAYED");
  const completed = [];
  for (const action of plan.actions) {
    if (typeof operations[action] !== "function") throw new TypeError(`operations.${action} is required`);
    await operations[action]({shop_id: plan.shop_id, subject_ref: plan.subject_ref, event: plan.event});
    completed.push(action);
  }
  await event_store.complete({shop_id: plan.shop_id, event_id: plan.event_id, action_count: completed.length});
  return Object.freeze({contract_version: CONTRACT_VERSION, event: plan.event, status: "completed", action_count: completed.length, access_disabled: completed.includes("disable_access"), tokens_removed: completed.includes("remove_shop_tokens"), production_values_exposed: false});
}

module.exports = Object.freeze({CONTRACT_VERSION, EVENTS, ACTIONS, planVerifiedLifecycleEvent, executeLifecyclePlan});
