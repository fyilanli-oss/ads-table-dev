"use strict";

const CONTRACT_VERSION = "e10-t5d-v1";
const ROW_KEYS = Object.freeze([
  "platform",
  "platform_purchase_count",
  "platform_sales_value",
]);

function finiteNonNegative(value, name) {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    throw new TypeError(`${name} is invalid`);
  }
  return value;
}

function normalizePlatform(value) {
  if (typeof value !== "string" || !/^[a-z0-9][a-z0-9_-]{0,63}$/.test(value)) {
    throw new TypeError("platform is invalid");
  }
  return value;
}

function normalizeRow(row) {
  if (!row || typeof row !== "object" || Array.isArray(row)) {
    throw new TypeError("Shopify attribution row is invalid");
  }

  const keys = Object.keys(row).sort();
  const expected = [...ROW_KEYS].sort();
  if (keys.length !== expected.length || keys.some((key, index) => key !== expected[index])) {
    throw new TypeError("Shopify attribution row contains an unapproved field");
  }

  return Object.freeze({
    platform: normalizePlatform(row.platform),
    platform_purchase_count: finiteNonNegative(
      row.platform_purchase_count,
      "platform_purchase_count",
    ),
    platform_sales_value: finiteNonNegative(
      row.platform_sales_value,
      "platform_sales_value",
    ),
    provenance: "shopify_reported_attribution",
  });
}

function buildAttributionOverlapIntake(rows) {
  if (!Array.isArray(rows)) {
    throw new TypeError("Shopify attribution rows must be an array");
  }

  const seen = new Set();
  const normalized = rows.map((row) => {
    const item = normalizeRow(row);
    if (seen.has(item.platform)) {
      throw new TypeError("Shopify attribution platform is duplicated");
    }
    seen.add(item.platform);
    return item;
  });

  return Object.freeze({
    contract_version: CONTRACT_VERSION,
    purpose: "attribution_overlap_input_only",
    rows: Object.freeze(normalized),
  });
}

module.exports = Object.freeze({ CONTRACT_VERSION, buildAttributionOverlapIntake });
