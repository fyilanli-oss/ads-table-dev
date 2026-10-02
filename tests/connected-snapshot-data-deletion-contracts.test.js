"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const root = path.resolve(__dirname, "..");
const bootstrap = JSON.parse(
  fs.readFileSync(path.join(root, "contracts", "e9-t8-connected-data-bootstrap-hourly-snapshot-v1.json"), "utf8"),
);
const deletion = JSON.parse(
  fs.readFileSync(
    path.join(root, "contracts", "shopify", "e10-t4b-workspace-data-deletion-clean-reinstall-v1.json"),
    "utf8",
  ),
);
const plan = fs.readFileSync(
  path.join(root, "codex-input", "AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md"),
  "utf8",
);

test("first connected bootstrap is exactly yesterday then today and trial length is not lookback", () => {
  assert.equal(bootstrap.status, "decision_frozen_implementation_pending");
  assert.deepEqual(
    bootstrap.initial_bootstrap.ordered_business_dates.map(({relative_date}) => relative_date),
    ["yesterday", "today"],
  );
  assert.equal(bootstrap.initial_bootstrap.automatic_lookback_days, 2);
  assert.equal(bootstrap.initial_bootstrap.older_history_automatic_fetch, false);
  assert.equal(bootstrap.initial_bootstrap.trial_days, 14);
  assert.equal(bootstrap.initial_bootstrap.trial_days_are_not_lookback_days, true);
  assert.equal(bootstrap.scope.excluded.includes("historical backfill"), true);
});

test("SnapshotJob is server-owned hourly sharded and cannot be multiplied by Shopify users", () => {
  assert.equal(bootstrap.scheduler.owner, "AdsTable server");
  assert.equal(bootstrap.scheduler.cadence, "hourly");
  assert.equal(bootstrap.scheduler.merchant_refresh_control, false);
  assert.equal(bootstrap.scheduler.page_view_trigger, false);
  assert.equal(bootstrap.scheduler.top_of_hour_fanout_forbidden, true);
  assert.equal(bootstrap.scheduler.single_flight, true);
  assert.equal(bootstrap.scheduler.expiring_worker_lease, true);
  assert.equal(bootstrap.failure_and_product_behavior.merchant_concurrency_cannot_create_jobs, true);
  assert.equal(bootstrap.scope.excluded.includes("production schedule activation"), true);
});

test("closed date does not invent provider finality", () => {
  assert.equal(bootstrap.maturity.day_closed_does_not_guarantee_metric_finality, true);
  assert.deepEqual(bootstrap.maturity.states, ["provisional", "settling", "finalized"]);
  assert.equal(bootstrap.maturity.provider_revalidation_required_before_runtime, true);
  assert.equal(bootstrap.maturity.no_synthetic_finality, true);
});

test("Shopify mandatory webhook and uninstall boundaries are explicit", () => {
  assert.deepEqual(
    deletion.mandatory_webhooks.compliance_topics,
    ["customers/data_request", "customers/redact", "shop/redact"],
  );
  assert.equal(deletion.mandatory_webhooks.app_uninstalled_operational_topic, "app/uninstalled");
  assert.equal(deletion.mandatory_webhooks.invalid_hmac_response, 401);
  assert.equal(deletion.mandatory_webhooks.compliance_completion_deadline_days, 30);
  assert.equal(deletion.mandatory_webhooks.shop_redact_after_uninstall_hours, 48);
  assert.equal(deletion.mandatory_webhooks.shop_redact_not_emitted_if_reinstalled_before_delivery, true);
  assert.equal(deletion.uninstall.analytics_deleted_immediately, false);
  assert.equal(deletion.uninstall.provider_calls_after_uninstall, false);
});

test("Delete my data and clean reinstall are destructive explicit and non-overlapping", () => {
  assert.equal(deletion.delete_my_data.available_while_installed, true);
  assert.equal(deletion.delete_my_data.separate_from_uninstall, true);
  assert.equal(deletion.delete_my_data.confirmation.length, 2);
  assert.equal(deletion.reinstall.within_shop_redact_delay.duplicate_workspace_forbidden, true);
  assert.equal(deletion.reinstall.within_shop_redact_delay.schedules_resume_automatically, false);
  assert.equal(deletion.reinstall.after_completed_deletion.create_new_workspace_generation, true);
  assert.equal(deletion.reinstall.after_completed_deletion.old_dataset_token_binding_restore, false);
  assert.equal(deletion.reinstall.clean_reinstall.old_workspace_must_reach_terminal_deletion_before_new_authority, true);
  assert.ok(deletion.excluded_from_this_contract_only_package.includes("token or data deletion"));
});

test("Execution Plan carries both decisions without claiming runtime completion", () => {
  for (const required of [
    "E9-T8 — Connected bootstrap ve system-owned hourly SnapshotJob",
    "yalnız \`yesterday\` ve \`today\`",
    "14 günlük trial lookback değildir",
    "merchant Refresh kontrolü yoktur",
    "E10-T4-B — Workspace Data Deletion ve Clean Reinstall",
    "customers/data_request",
    "customers/redact",
    "shop/redact",
    "Delete my data",
    "uninstall veri silindi anlamına gelmez",
    "implementation pending",
  ]) {
    assert.ok(plan.includes(required), `Execution Plan is missing: ${required}`);
  }
});
