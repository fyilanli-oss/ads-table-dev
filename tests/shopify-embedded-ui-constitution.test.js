"use strict";

const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const assert = require("node:assert/strict");

const root = path.join(__dirname, "..");
const read = (file) => fs.readFileSync(path.join(root, file), "utf8");
const contract = JSON.parse(
  read("contracts/shopify/shopify-embedded-ui-constitution-v1.json"),
);
const constitution = read("docs/SHOPIFY_EMBEDDED_UI_CONSTITUTION.md");
const plan = read("codex-input/AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md");
const agentRules = read("AGENTS.md");
const taskTemplate = read("docs/templates/SHOPIFY_EMBEDDED_UI_TASK_TEMPLATE.md");
const settingsSource = read("src/shopify/embedded-app-home.js");
const adAccountSource = read("src/shopify/ad-account-ui.js");
const klaviyoAccountSource = read("src/shopify/klaviyo-account-ui.js");

test("the Shopify embedded UI constitution is binding across plan and task instructions", () => {
  assert.equal(contract.status, "binding");
  assert.match(plan, /SHOPIFY_EMBEDDED_UI_CONSTITUTION\.md/);
  assert.match(agentRules, /SHOPIFY_EMBEDDED_UI_CONSTITUTION\.md/);
  assert.match(agentRules, /Kanıtlanmamış aşama tamamlanmış kabul edilmez/);
  assert.match(taskTemplate, /Exact Shopify component/);
  assert.match(constitution, /yalnız Shopify App Bridge ve ürün sahibi tarafından açıkça onaylanmış güncel App Home Polaris web component sürümü/);
  assert.equal(
    contract.approved_runtime.polaris_script,
    "https://cdn.shopify.com/shopifycloud/polaris-2.0-rc.js",
  );
  assert.match(settingsSource, /shopifycloud\/polaris-2\.0-rc\.js/);
  assert.doesNotMatch(settingsSource, /shopifycloud\/polaris-(?:1|1\.\d+)\.js/);
});

test("every frozen Shopify UI contract points to the constitution", () => {
  const governedContracts = [
    "contracts/shopify/e10-t5c1-funnel-ui.json",
    "contracts/shopify/e10-t5c2a-ad-analysis-ui.json",
    "contracts/shopify/e10-t5c3-dashboard-ui.json",
    "contracts/shopify/e10-t5c4-platforms-settings-ui.json",
    "contracts/shopify/e10-t5c5a-attribution-differences-ui.json",
    "contracts/shopify/e10-t5c7-integrated-navigation.json",
  ];

  for (const file of governedContracts) {
    const value = JSON.parse(read(file));
    assert.equal(
      value.governance_contract,
      "contracts/shopify/shopify-embedded-ui-constitution-v1.json",
      `${file} must inherit the central UI constitution`,
    );
  }
});

test("standard controls cannot be replaced by raw HTML or custom visual CSS", () => {
  for (const pattern of [
    /<button\b/i,
    /<input\b/i,
    /<select\b/i,
    /<form\b/i,
    /<dialog\b/i,
    /<div\b/i,
    /<style\b/i,
    /class(?:Name)?\s*=/i,
  ]) {
    assert.doesNotMatch(settingsSource, pattern);
  }
});

test("Settings corrective implementation has zero custom visual debt", () => {
  const inlineStyles = settingsSource.match(/style\s*=/gi) || [];
  const colors = settingsSource.match(/#[0-9a-f]{3,8}\b/gi) || [];
  const clickableActions = settingsSource.match(/<s-clickable\b/gi) || [];

  assert.deepEqual(contract.known_noncompliance, []);
  assert.equal(inlineStyles.length, 0, "inline style debt must be fully removed");
  assert.equal(colors.length, 0, "literal color debt must be fully removed");
  assert.equal(clickableActions.length, 0, "s-clickable button imitation must be fully removed");
});

test("Shopify layout components use the supported display property for conditional visibility", () => {
  assert.doesNotMatch(settingsSource, /<s-(?:stack|box)[^>]*\shidden(?:\s|>)/i);
  assert.doesNotMatch(settingsSource, /(?:currencySetup|currencySummary|providerSections|selectionStep|confirmationStep|acceptancePanel|metricStep|metaAcceptancePanel|googleAcceptancePanel)\.hidden\s*=/);
  assert.doesNotMatch(adAccountSource, /\.hidden\s*=/);
  assert.doesNotMatch(klaviyoAccountSource, /\.hidden\s*=/);
  assert.match(settingsSource, /display="none"/);
  assert.match(adAccountSource, /element\.display = visible \? 'auto' : 'none'/);
  assert.match(klaviyoAccountSource, /element\.display = visible \? "auto" : "none"/);
});

test("the constitution requires both desktop and real mobile merchant acceptance", () => {
  assert.ok(contract.required_before_merge.includes("desktop_real_shopify_admin_pass"));
  assert.ok(contract.required_before_merge.includes("mobile_320px_real_shopify_admin_pass"));
  assert.ok(contract.required_before_merge.includes("explicit_product_owner_acceptance"));
  assert.match(constitution, /kullanıcı\/ürün sahibi açık kabul/i);
});
