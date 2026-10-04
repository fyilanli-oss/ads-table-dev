const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const contract = JSON.parse(fs.readFileSync(
  path.join(root, 'contracts', 'a6-eo-f5-product-route-map-v1.json'), 'utf8'
));
const doc = fs.readFileSync(path.join(root, 'docs', 'A6_EO_F5_PRODUCT_ROUTE_MAP.md'), 'utf8');
const master = JSON.parse(fs.readFileSync(
  path.join(root, 'contracts', 'a6-eo-00-embedded-only-reestablishment-v1.json'), 'utf8'
));
const plan = fs.readFileSync(
  path.join(root, 'codex-input', 'AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md'), 'utf8'
);

test('EO-F5 freezes exactly three top-level product surfaces', () => {
  assert.equal(contract.status, 'eo_f5_complete_eo_f6_pending');
  assert.equal(contract.product.surface, 'shopify_embedded_only');
  assert.deepEqual(contract.product.canonical_home, {
    route: '/',
    surface: 'funnel',
    duplicate_app_nav_link: false
  });
  assert.deepEqual(contract.product.global_logical_order, ['funnel', 'ad_analysis', 'settings']);
  assert.deepEqual(contract.product.rendered_app_nav, ['ad_analysis', 'settings']);
  assert.equal(contract.acceptance.canonical_ui_route_count, 3);
  assert.equal(contract.acceptance.top_level_product_surface_count, 3);
  assert.deepEqual(contract.ui_routes.map((route) => route.path), ['/', '/ad-analysis', '/settings']);
});

test('EO-F5 nests Dashboard, Platforms and Attribution Differences', () => {
  assert.equal(contract.dashboard.top_level_route, false);
  assert.deepEqual(contract.dashboard.host_surfaces, ['/', '/ad-analysis']);
  assert.equal(contract.dashboard.behavior, 'contextual_chart_panels');
  assert.equal(contract.platforms.top_level_route, false);
  assert.equal(contract.platforms.host_surface, '/settings');
  assert.equal(contract.attribution_differences.top_level_route, false);
  assert.equal(contract.attribution_differences.host_surface, '/ad-analysis');
  assert.equal(
    contract.attribution_differences.activation_after,
    'E10-T5-C2-C_cross_platform_deepest_grain_discovery'
  );
  for (const route of ['/dashboard', '/platforms', '/attribution-differences']) {
    assert.ok(contract.retired_or_non_target_routes.includes(route));
  }
});

test('EO-F5 binds onboarding and provider setup to Settings', () => {
  assert.deepEqual(contract.onboarding_resolver, [
    'verified_shopify_session',
    'managed_installation_bootstrap',
    'active_trial_or_subscription_entitlement',
    'merchant_selected_reporting_currency_in_settings',
    'at_least_one_verified_active_provider_account_in_settings',
    'klaviyo_email_monthly_plan_cost_in_settings_when_klaviyo_selected',
    'funnel_home'
  ]);
  assert.equal(contract.route_guard.tenant_source, 'server_verified_shopify_session');
  assert.equal(contract.route_guard.caller_supplied_workspace, false);
  assert.equal(contract.route_guard.caller_supplied_shop_authority, false);
  assert.deepEqual(contract.oauth.callback_return_allowlist, ['/settings']);
});

test('EO-F5 exposes only allowlisted provider OAuth and signed Shopify webhooks', () => {
  assert.deepEqual(contract.oauth.provider_allowlist, ['meta', 'google-ads', 'klaviyo']);
  assert.equal(contract.shopify_webhook.route, 'POST /webhooks/shopify');
  assert.equal(contract.shopify_webhook.raw_body_hmac_before_parse, true);
  assert.equal(contract.shopify_webhook.invalid_hmac_status, 401);
  for (const topic of ['customers/data_request', 'customers/redact', 'shop/redact', 'app/uninstalled']) {
    assert.ok(contract.shopify_webhook.topics.includes(topic));
  }
});

test('EO-F5 keeps refresh server-owned and truth states explicit', () => {
  assert.equal(contract.provider_data_rules.missing_becomes_zero, false);
  assert.equal(contract.provider_data_rules.hourly_scheduler_authority, 'adstable_owned');
  assert.equal(contract.provider_data_rules.page_open_starts_refresh, false);
  assert.deepEqual(contract.provider_data_rules.first_bootstrap_dates, ['yesterday', 'today']);
  for (const state of ['partial', 'stale', 'unsupported', 'unknown', 'reauthorization_required', 'capability_unavailable']) {
    assert.ok(contract.truth_states.includes(state));
  }
  assert.equal(contract.internal_routes[0].browser_allowed, false);
});

test('EO-F5 excludes legacy, standalone and operator routes from production', () => {
  for (const required of [
    'legacy_shopify_app_tree',
    'standalone_login_signup_auth_dashboard',
    'api_e10_acceptance_preflight_probe',
    'runtime_preflight_or_acceptance',
    'raw_provider_response_or_metric_discovery',
    'browser_triggered_manual_refresh'
  ]) {
    assert.ok(contract.forbidden_production_route_classes.includes(required));
  }
  assert.equal(contract.acceptance.standalone_ui_or_auth_route_count, 0);
  assert.equal(contract.acceptance.production_debug_operator_acceptance_route_count, 0);
  assert.equal(contract.acceptance.browser_triggered_refresh_route_count, 0);
  assert.equal(contract.acceptance.direct_browser_database_access_count, 0);
});

test('EO-F5 preserves UI Constitution and advances only to EO-F6', () => {
  assert.equal(contract.ui_implementation_gate.code_written_in_this_package, false);
  assert.equal(contract.ui_implementation_gate.exact_official_component_mapping_required, true);
  assert.equal(contract.ui_implementation_gate.raw_html_action_controls, false);
  assert.equal(contract.ui_implementation_gate.desktop_shopify_admin_evidence, true);
  assert.equal(contract.ui_implementation_gate.real_mobile_320px_shopify_admin_evidence, true);
  assert.equal(contract.scope.production_mutation, false);
  assert.equal(contract.project_creation_authorized, false);
  assert.equal(contract.cutover_authorized, false);
  assert.equal(contract.legacy_deletion_authorized, false);
  assert.equal(contract.acceptance.next_gate, 'EO-F6');
  assert.match(master.status, /f5_complete/);
  assert.equal(master.completed_gates.at(-1).id, 'EO-F5');
  assert.match(plan, /\*\*EO-F5 — Complete \/ embedded product and route map frozen:\*\*/);
  assert.match(doc, /yalnız \*\*üç ana yüzeyden\*\*/);
});
