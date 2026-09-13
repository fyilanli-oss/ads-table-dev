"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const {
  buildAttributionOverlapIntake,
} = require("../src/shopify/attribution-overlap-intake");

test("accepts only Shopify-reported platform purchase count and sales value", () => {
  const result = buildAttributionOverlapIntake([
    { platform: "meta", platform_purchase_count: 12, platform_sales_value: 840.5 },
    { platform: "google", platform_purchase_count: 7, platform_sales_value: 415 },
  ]);

  assert.equal(result.purpose, "attribution_overlap_input_only");
  assert.deepEqual(result.rows[0], {
    platform: "meta",
    platform_purchase_count: 12,
    platform_sales_value: 840.5,
    provenance: "shopify_reported_attribution",
  });
  assert.ok(Object.isFrozen(result));
  assert.ok(Object.isFrozen(result.rows));
  assert.ok(Object.isFrozen(result.rows[0]));
});

test("rejects totals, refunds and every other unapproved Shopify field", () => {
  for (const extra of ["total_sales", "refund_value", "shop_currency", "shop_timezone"]) {
    assert.throws(
      () => buildAttributionOverlapIntake([{
        platform: "meta",
        platform_purchase_count: 1,
        platform_sales_value: 10,
        [extra]: 1,
      }]),
      /unapproved field/,
    );
  }
});

test("fails closed for duplicate platforms and invalid metric values", () => {
  const row = { platform: "meta", platform_purchase_count: 1, platform_sales_value: 10 };
  assert.throws(() => buildAttributionOverlapIntake([row, row]), /duplicated/);
  assert.throws(
    () => buildAttributionOverlapIntake([{ ...row, platform_sales_value: -1 }]),
    /platform_sales_value/,
  );
});
