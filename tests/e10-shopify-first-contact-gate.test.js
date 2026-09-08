"use strict";

const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const assert = require("node:assert/strict");

const plan = fs.readFileSync(
  path.join(__dirname, "..", "codex-input", "AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md"),
  "utf8",
);
const start = plan.indexOf("### Shopify ile ilk temas kapısı");
const end = plan.indexOf("### Kabul kriterleri", start);
const gate = plan.slice(start, end);

test("first real Shopify contact is exactly E10-T6-B", () => {
  assert.ok(start > -1);
  assert.ok(end > start);
  assert.match(gate, /ilk gerçek işlem \*\*E10-T6-B — Development App Bootstrap\*\*/);
  assert.match(gate, /E10-T5-C `Done` ve E10-T6-A `PASS` olmadan başlayamaz/);
  assert.match(gate, /açık development-environment insan onayı/);
});

test("E10-T6-A is offline readiness and cannot touch Shopify", () => {
  assert.match(gate, /E10-T6-A — Official capability ve development-readiness — Shopify teması yok/);
  assert.match(gate, /Partner Dashboard, store, credential, scope, redirect, webhook veya API query değişikliği yapılmaz/);
});

test("development and production gates remain separate", () => {
  assert.match(gate, /Production onayı yerine geçmez/);
  assert.match(gate, /Production store\/credential, billing activation, App Store submission ve production veri işlemi kesinlikle yapılmaz/);
  assert.match(gate, /production aktivasyonu ayrı açık production onayı gerektirir/);
});

test("the post-contact sequence cannot expand Shopify intake silently", () => {
  assert.match(gate, /T6-B ilk Shopify teması → E10-T6-C embedded OAuth smoke → E10-T6-D attribution feasibility/);
  assert.match(gate, /Order\/Customer ingestion'a veya yeni scope'a otomatik geçilmez/);
  assert.match(gate, /overlap diagnostic'i tek başına order webhook, commerce storage veya Dataset V2 yazımını meşrulaştırmaz/);
});
