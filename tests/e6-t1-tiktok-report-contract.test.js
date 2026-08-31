'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const { assertNoGenericPurchaseFallback, buildReportSelection, reportContract } = require('../src/providers/tiktok/report-contract');
const fixture = JSON.parse(fs.readFileSync(path.join(__dirname, '../artifacts/e6-tiktok/e6-t1-report-contract-fixture.json'), 'utf8'));

test('TikTok production contract freezes the official integrated report surface', () => {
  const contract = reportContract();
  const selection = buildReportSelection(fixture.report_level);
  assert.equal(contract.version, 'tiktok-report-v1');
  assert.equal(contract.endpoint, '/v1.3/report/integrated/get/');
  assert.match(contract.official_reference, /^https:\/\/business-api\.tiktok\.com\/portal\/docs/);
  assert.equal(selection.data_level, fixture.expected_data_level);
  assert.deepEqual(selection.dimensions, fixture.expected_dimensions);
  assert.deepEqual(selection.metrics, fixture.expected_metrics);
});

test('contract keeps report levels separate and declares Ad as the production leaf', () => {
  const contract = reportContract();
  assert.equal(contract.hard_rules.production_leaf_level, 'ad');
  assert.equal(contract.hard_rules.hierarchy_levels_are_not_additive, true);
  assert.equal(buildReportSelection('campaign').data_level, 'AUCTION_CAMPAIGN');
  assert.equal(buildReportSelection('adgroup').data_level, 'AUCTION_ADGROUP');
  assert.throws(() => buildReportSelection('asset_group'), /Unsupported/);
});

test('generic conversion fields cannot become purchase evidence', () => {
  assert.throws(() => assertNoGenericPurchaseFallback(fixture.generic_conversion_row), /cannot be treated as purchase/);
  assert.equal(assertNoGenericPurchaseFallback({ metrics: { complete_payment: '0' } }), true);
  assert.equal(reportContract().hard_rules.missing_is_not_zero, true);
});

test('fixture is synthetic and contains no credential material', () => {
  assert.equal(fixture.synthetic, true);
  assert.doesNotMatch(JSON.stringify(fixture), /access[_-]?token|client[_-]?secret|refresh[_-]?token/i);
});
