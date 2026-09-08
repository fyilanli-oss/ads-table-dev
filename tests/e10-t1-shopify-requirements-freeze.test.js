"use strict";

const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const assert = require("node:assert/strict");

const root = path.join(__dirname, "..");
const plan = fs.readFileSync(
  path.join(root, "codex-input", "AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md"),
  "utf8",
);
const freeze = fs.readFileSync(
  path.join(root, "docs", "E10_T1_SHOPIFY_OFFICIAL_REQUIREMENTS_FREEZE.md"),
  "utf8",
);

const officialLinks = [
  "https://shopify.dev/docs/apps/launch/distribution/select-distribution-method",
  "https://shopify.dev/docs/apps/build/authentication-authorization/session-tokens",
  "https://shopify.dev/docs/apps/build/authentication-authorization/access-tokens/token-exchange",
  "https://shopify.dev/docs/api/app-bridge-library",
  "https://shopify.dev/docs/apps/launch/billing",
  "https://shopify.dev/docs/apps/build/privacy-law-compliance",
  "https://shopify.dev/docs/apps/launch/protected-customer-data",
  "https://shopify.dev/docs/apps/launch/app-requirements-checklist",
  "https://shopify.dev/docs/apps/launch/app-store-review/review-process",
];

test("E10-T1 freezes every required domain against official Shopify sources", () => {
  for (const link of officialLinks) {
    assert.match(freeze, new RegExp(link.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
  }
  assert.doesNotMatch(freeze, /https?:\/\/(?!shopify\.dev)/);
});

test("the freeze keeps identity, secrets, scope and provenance fail closed", () => {
  assert.match(freeze, /doğrulanmamış shop, user veya workspace kimliği backend yetkisi üretmez/);
  assert.match(freeze, /Access token, webhook secret, session material.*browser response'una.*log'a yazılmaz/);
  assert.match(freeze, /"İleride gerekebilir" gerekçe değildir/);
  assert.match(freeze, /Shopify-reported platform attribution.*provider-reported conversion.*ayrı provenance/);
});

test("unknown implementation details remain explicit revalidation gates", () => {
  for (const gate of [
    "exact Admin API sürümü",
    "exact access-token tipi",
    "minimum commerce scope/field listesi",
    "Managed app pricing ile Billing API seçimi",
    "zorunlu webhook topic/URI seti",
    "submission anındaki checklist",
  ]) {
    assert.match(freeze, new RegExp(gate));
  }
  assert.match(freeze, /deprecation bildirimi.*freeze güncellenmeden implementation devam etmez/);
});

test("the plan advances only E10-T1 and names E10-T2 as next", () => {
  assert.match(plan, /E10-T1 — Done — Official requirements freeze/);
  assert.match(plan, /E10-T1 `Done`; parent E10 `In progress`/);
  assert.match(plan, /Sıradaki uygulanabilir repository işi E10-T2 shop\/workspace tenant modelidir/);
  assert.match(freeze, /Partner Dashboard.*production credential.*App Store gönderimi yapmaz/);
});
