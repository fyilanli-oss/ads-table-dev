'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const { OFFICIAL_SDK_COMMIT, TIKTOK_REPORT_CONTRACT, validateTikTokReportContract } = require('../src/providers/tiktok/report-contract');

const fixture = JSON.parse(fs.readFileSync(path.join(__dirname, '../artifacts/e6-tiktok/e6-t1-report-contract-fixture.json'), 'utf8'));

test('E6-T1 pins the official TikTok synchronous reporting surface', () => {
  assert.equal(OFFICIAL_SDK_COMMIT, 'f809c396520df2d7b201a9ccc5378d822b728ed3');
  assert.equal(TIKTOK_REPORT_CONTRACT.request.endpoint, '/open_api/v1.3/report/integrated/get/');
  assert.deepEqual(TIKTOK_REPORT_CONTRACT.request, {
    method: 'GET', endpoint: '/open_api/v1.3/report/integrated/get/', report_type: 'BASIC', service_type: 'AUCTION',
    data_level: 'AUCTION_AD', dimensions: ['ad_id'], delivery_metrics: ['spend', 'impressions', 'clicks']
  });
  assert.equal(validateTikTokReportContract(), true);
});

test('production facts are additive only at Ad leaf level', () => {
  assert.deepEqual(TIKTOK_REPORT_CONTRACT.production_grain, { root: 'campaign', parent: 'adgroup', leaf: 'ad', additive_level: 'AUCTION_AD' });
  assert.equal(TIKTOK_REPORT_CONTRACT.hard_rules.campaign_and_adgroup_are_lineage_not_additive_rows, true);
});

test('generic conversion and missing values cannot become purchases', () => {
  assert.equal(TIKTOK_REPORT_CONTRACT.hard_rules.generic_conversion_is_not_purchase, true);
  assert.equal(TIKTOK_REPORT_CONTRACT.hard_rules.missing_metric_is_not_zero, true);
  assert.equal(TIKTOK_REPORT_CONTRACT.canonical_support.purchase, 'unknown');
  assert.throws(() => validateTikTokReportContract({ ...TIKTOK_REPORT_CONTRACT, request: { ...TIKTOK_REPORT_CONTRACT.request, delivery_metrics: ['conversion'] } }), /forbidden/);
});

test('fixture is synthetic, secret-free and matches the frozen request', () => {
  assert.equal(fixture.synthetic, true);
  assert.equal(fixture.contract_version, TIKTOK_REPORT_CONTRACT.version);
  assert.deepEqual(fixture.request.metrics, TIKTOK_REPORT_CONTRACT.request.delivery_metrics);
  assert.doesNotMatch(JSON.stringify(fixture), /access.?token|authorization|bearer|secret|@|https?:\/\//i);
});
