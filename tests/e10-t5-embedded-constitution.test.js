"use strict";

const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const assert = require("node:assert/strict");

const plan = fs.readFileSync(
  path.join(__dirname, "..", "codex-input", "AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md"),
  "utf8",
);
const start = plan.indexOf("### Shopify Embedded Uygulama Anayasası");
const end = plan.indexOf("### Planlanan işler", start);
const constitution = plan.slice(start, end);

test("Execution Plan contains the complete numbered Shopify constitution", () => {
  assert.ok(start > -1);
  assert.ok(end > start);
  const headings = [
    "1. Ürün amacı ve sınırı",
    "2. Authority, güvenlik ve production sınırı",
    "3. Shopify Embedded Uygulama Görsel Yaklaşımı",
    "4. Üst menü, tarih ve comparison",
    "5. Filters",
    "6. Funnel/Table veri akışı ve switch",
    "7. Provider hierarchy",
    "8. Metrik sözlüğü ve provenance",
    "9. E10-T5 İçin Revize Edilmiş Ürün Sözleşmesi",
  ];
  for (const heading of headings) assert.match(constitution, new RegExp(heading.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
});

test("visual constitution is Shopify-native and rejects iframe imitation", () => {
  assert.match(constitution, /embedded shell ve App Bridge \*\*zorunludur\*\*/);
  assert.match(constitution, /resmi Shopify UI componentleri \*\*zorunludur\*\*/);
  assert.match(constitution, /CSS component framework.*statik shell \*\*yasaktır\*\*/);
  assert.match(constitution, /yabancı-site\/iframe hissi \*\*acceptance failure\*\*/);
});

test("T5 A B C and the absolute sequencing gate are explicit", () => {
  assert.match(constitution, /E10-T5-A — Shopify Kuralları — `Done`/);
  assert.match(constitution, /E10-T5-B — Shopify'dan ne alınacak, nasıl gösterilecek\? — `Done`/);
  assert.match(constitution, /E10-T5-C — Shopify'a ne verilecek, nasıl gösterilecek\? — `Product decision required`/);
  assert.match(constitution, /Mutlak sıra kapısı:[\s\S]*E10-T6–T10, E11 veya E12[\s\S]*branch, kod ya da PR açılamaz/);
});
