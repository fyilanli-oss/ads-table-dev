'use strict';

const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');

const contract = require('../canonical-contract');
const time = require('../time-service');
const fx = require('../fx-service');
const hierarchy = require('../entity-hierarchy');
const scope = require('../analysis-scope');
const formula = require('../formula-engine');
const { InMemoryDatasetRepository } = require('../dataset-repository');
const { FunnelQueryService } = require('../funnel-query-service');
const fixtures = require('../fixtures');

let passed = 0;

async function test(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log(`PASS ${String(passed).padStart(2, '0')} - ${name}`);
  } catch (error) {
    console.error(`FAIL - ${name}`);
    throw error;
  }
}

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function assertThrowsMessage(fn, pattern) {
  assert.throws(fn, pattern);
}

function sha256(file) {
  return crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
}

(async () => {
  // Canonical contract
  await test('Canonical contract accepts Meta paid fixture', () => contract.validateCanonicalRow(fixtures.metaPaid()));
  await test('Canonical contract accepts Google Standard fixture', () => contract.validateCanonicalRow(fixtures.googleStandard()));
  await test('Canonical contract accepts PMax shape fixture', () => contract.validateCanonicalRow(fixtures.googlePmax()));
  await test('Canonical contract accepts TikTok fixture', () => contract.validateCanonicalRow(fixtures.tiktokPaid()));
  await test('Canonical contract accepts Klaviyo Email fixture', () => contract.validateCanonicalRow(fixtures.klaviyoCampaignEmail()));
  await test('Canonical contract accepts Klaviyo SMS fixture', () => contract.validateCanonicalRow(fixtures.klaviyoFlowSms()));
  await test('Canonical contract accepts deterministic GA4-sourced Organic fixture', () => contract.validateCanonicalRow(fixtures.metaOrganic()));

  await test('Unknown platform is rejected', () => {
    const row = fixtures.metaPaid();
    row.identity.platform = 'ga4';
    assertThrowsMessage(() => contract.validateCanonicalRow(row), /Unsupported platform/);
  });

  await test('GA4 cannot be modeled as a paid platform source', () => {
    const row = fixtures.metaPaid();
    row.identity.source_system = 'ga4';
    row.provenance.source_system = 'ga4';
    assertThrowsMessage(() => contract.validateCanonicalRow(row), /Paid meta row must use source_system=meta_ads/);
  });

  await test('Non-Klaviyo email channel is rejected', () => {
    const row = fixtures.metaPaid();
    row.identity.channel = 'email';
    assertThrowsMessage(() => contract.validateCanonicalRow(row), /must have channel=null/);
  });

  await test('Unsupported metric must be null, not zero', () => {
    const row = fixtures.metaPaid();
    row.metric_support.checkout_value = 'unsupported';
    row.raw_metrics.checkout_value = 0;
    assertThrowsMessage(() => contract.validateCanonicalRow(row), /unsupported metric checkout_value must be null/);
  });

  await test('Measured real zero remains supported zero', () => {
    const row = fixtures.metaPaid({ metrics: { purchase: 0 } });
    assert.equal(row.raw_metrics.purchase, 0);
    assert.equal(row.metric_support.purchase, 'supported');
    contract.validateCanonicalRow(row);
  });

  await test('Synthetic row is rejected from production canonical validation', () => {
    const row = fixtures.metaPaid({ synthetic: true, sourceConfidence: 'fallback' });
    assertThrowsMessage(() => contract.validateCanonicalRow(row), /Synthetic rows cannot/);
  });

  await test('Organic row requires GA4 property provenance', () => {
    const row = fixtures.metaOrganic();
    row.provenance.ga4_property_id = null;
    assertThrowsMessage(() => contract.validateCanonicalRow(row), /ga4_property_id/);
  });

  // Hierarchy
  await test('Meta Campaign -> AdSet -> Ad hierarchy validates', () => hierarchy.validateEntityHierarchy(fixtures.metaPaid().identity, fixtures.metaPaid().entity));
  await test('Google Campaign -> AdGroup -> Ad hierarchy validates', () => hierarchy.validateEntityHierarchy(fixtures.googleStandard().identity, fixtures.googleStandard().entity));
  await test('PMax Campaign -> Asset Group hierarchy validates without fake AdGroup/Ad', () => hierarchy.validateEntityHierarchy(fixtures.googlePmax().identity, fixtures.googlePmax().entity));
  await test('TikTok Campaign -> AdGroup -> Ad hierarchy validates', () => hierarchy.validateEntityHierarchy(fixtures.tiktokPaid().identity, fixtures.tiktokPaid().entity));
  await test('Klaviyo Campaign -> Campaign Message validates', () => hierarchy.validateEntityHierarchy(fixtures.klaviyoCampaignEmail().identity, fixtures.klaviyoCampaignEmail().entity));
  await test('Klaviyo Flow -> Flow Message validates as sibling root', () => hierarchy.validateEntityHierarchy(fixtures.klaviyoFlowSms().identity, fixtures.klaviyoFlowSms().entity));
  await test('Organic platform-level entity validates without paid hierarchy', () => hierarchy.validateEntityHierarchy(fixtures.metaOrganic().identity, fixtures.metaOrganic().entity));

  await test('Fake PMax Ad is rejected', () => {
    const row = fixtures.googlePmax();
    row.entity.entity_type = 'ad';
    assertThrowsMessage(() => hierarchy.validateEntityHierarchy(row.identity, row.entity), /Performance Max leaf must be Asset Group/);
  });

  await test('Klaviyo fake AdGroup parent is rejected', () => {
    const row = fixtures.klaviyoCampaignEmail();
    row.entity.parent_entity_type = 'adgroup';
    row.entity.parent_entity_id = 'fake';
    row.entity.parent_entity_name = 'Fake';
    assertThrowsMessage(() => hierarchy.validateEntityHierarchy(row.identity, row.entity), /must not invent a parent level/);
  });

  await test('Entity keys are deterministic and distinguish Klaviyo Campaign vs Flow messages', () => {
    const campaign = fixtures.klaviyoCampaignEmail();
    const flow = fixtures.klaviyoFlowSms();
    const a = hierarchy.buildEntityKey(campaign.identity, campaign.entity);
    const b = hierarchy.buildEntityKey(campaign.identity, campaign.entity);
    const c = hierarchy.buildEntityKey(flow.identity, flow.entity);
    assert.equal(a, b);
    assert.notEqual(a, c);
  });

  // Time
  await test('Time service moves UTC timestamp into next Istanbul business date', () => {
    assert.equal(time.businessDateFromTimestamp('2026-08-15T22:30:00Z', 'Europe/Istanbul'), '2026-08-16');
  });

  await test('Time service moves UTC timestamp into previous US business date', () => {
    assert.equal(time.businessDateFromTimestamp('2026-08-16T02:00:00Z', 'America/New_York'), '2026-08-15');
  });

  await test('Time service accepts provider daily date deterministically', () => {
    const result = time.normalizeBusinessDate({ providerDate: '2026-08-15', sourceTimezone: 'Europe/Istanbul' });
    assert.deepEqual(result, { source_timezone: 'Europe/Istanbul', business_date: '2026-08-15', time_engine_version: 'v1' });
  });

  await test('Time service rejects missing timezone instead of UTC fallback', () => {
    assertThrowsMessage(() => time.normalizeBusinessDate({ providerTimestamp: '2026-08-15T10:00:00Z' }), /server UTC is not a silent fallback/);
  });

  await test('Time service rejects invalid timezone', () => {
    assertThrowsMessage(() => time.normalizeBusinessDate({ providerTimestamp: '2026-08-15T10:00:00Z', sourceTimezone: 'Mars/Olympus' }), /Invalid IANA timezone/);
  });

  // FX
  await test('FX converts all four monetary raw facts before formulas', () => {
    const row = fixtures.metaPaid({ sourceCurrency: 'USD', targetCurrency: 'TRY', fxRate: 2 });
    const result = fx.normalizeMonetaryRawFields(row);
    assert.equal(result.raw_metrics.spend_value, 200);
    assert.equal(result.raw_metrics.add_to_cart_value, 800);
    assert.equal(result.raw_metrics.checkout_value, 600);
    assert.equal(result.raw_metrics.purchase_value, 500);
    assert.equal(result.raw_metrics.purchase, 5);
    assert.equal(result.currency.fx_engine_version, 'v1');
  });

  await test('FX same currency uses rate=1', () => {
    const row = fixtures.metaPaid();
    const result = fx.normalizeMonetaryRawFields(row, { sourceCurrency: 'TRY', targetCurrency: 'TRY', fxRate: null });
    assert.equal(result.currency.fx_rate, 1);
    assert.equal(result.raw_metrics.purchase_value, 250);
  });

  await test('FX preserves unsupported null monetary metric', () => {
    const row = fixtures.metaPaid({ metricSupport: { checkout_value: 'unsupported' } });
    const result = fx.normalizeMonetaryRawFields(row);
    assert.equal(result.raw_metrics.checkout_value, null);
    assert.equal(result.metric_support.checkout_value, 'unsupported');
  });

  await test('FX preserves measured zero', () => {
    const row = fixtures.metaPaid({ metrics: { spend_value: 0 } });
    const result = fx.normalizeMonetaryRawFields(row);
    assert.equal(result.raw_metrics.spend_value, 0);
  });

  await test('FX rejects missing cross-currency rate', () => {
    const row = fixtures.metaPaid({ sourceCurrency: 'USD', targetCurrency: 'TRY', fxRate: null });
    assertThrowsMessage(() => fx.normalizeMonetaryRawFields(row), /requires a positive fx_rate/);
  });

  await test('FX rejects invalid negative rate', () => {
    const row = fixtures.metaPaid({ sourceCurrency: 'USD', targetCurrency: 'TRY', fxRate: -1 });
    assertThrowsMessage(() => fx.normalizeMonetaryRawFields(row), /requires a positive fx_rate/);
  });

  // Analysis scope
  await test('Paid scope uses ad_click as funnel_click', () => {
    const result = scope.aggregateScope([fixtures.metaPaid(), fixtures.metaOrganic()], 'paid');
    assert.equal(result.funnel_click, 100);
    assert.equal(result.funnel_sales, 250);
  });

  await test('Organic scope uses session as funnel_click', () => {
    const result = scope.aggregateScope([fixtures.metaPaid(), fixtures.metaOrganic()], 'organic');
    assert.equal(result.funnel_click, 50);
    assert.equal(result.funnel_sales, 120);
  });

  await test('Blend sums paid ad_click + organic session before formulas', () => {
    const result = scope.aggregateScope([fixtures.metaPaid(), fixtures.metaOrganic()], 'blend');
    assert.equal(result.funnel_click, 150);
    assert.equal(result.funnel_spend, 100);
    assert.equal(result.funnel_purchase, 8);
    assert.equal(result.funnel_sales, 370);
  });

  await test('Metric support merging refuses partial additive total', () => {
    const paid = fixtures.metaPaid();
    const organic = fixtures.metaOrganic({ metricSupport: { checkout_value: 'unsupported' } });
    const result = scope.aggregateScope([paid, organic], 'blend');
    assert.equal(result.funnel_checkout_value, null);
    assert.equal(result.metric_support.funnel_checkout_value, 'unknown');
  });

  // Formula engine
  await test('Formula Engine computes core derived metrics from aggregate facts', () => {
    const aggregate = scope.aggregateScope([fixtures.metaPaid()], 'paid');
    const result = formula.calculateFunnelMetrics(aggregate);
    assert.equal(result.sales, 250);
    assert.equal(result.abandoned, 5);
    assert.equal(result.abandoned_value, 50);
    assert.equal(result.profit, 150);
    assert.equal(result.margin, 60);
    assert.equal(result.ctr, 10);
    assert.equal(result.cpc, 1);
    assert.equal(result.roas, 2.5);
    assert.equal(result.cps, 20);
    assert.equal(result.formula_engine_version, 'v1');
    assert.equal(Object.prototype.hasOwnProperty.call(result, 'cpm'), false);
    assert.equal(Object.prototype.hasOwnProperty.call(result, 'acos'), false);
    assert.equal(Object.prototype.hasOwnProperty.call(result, 'cvr'), false);
    assert.equal(Object.prototype.hasOwnProperty.call(result, 'aov'), false);
  });

  await test('Formula Engine floors abandoned and abandoned value at zero', () => {
    const row = fixtures.metaPaid({ metrics: { checkout: 2, purchase: 5, checkout_value: 100, purchase_value: 250 } });
    const result = formula.calculateFunnelMetrics(scope.aggregateScope([row], 'paid'));
    assert.equal(result.abandoned, 0);
    assert.equal(result.abandoned_value, 0);
  });

  await test('Formula Engine returns null when denominator is measured zero', () => {
    const row = fixtures.metaPaid({ metrics: { ad_click: 0, spend_value: 0, purchase: 0, purchase_value: 0 } });
    const result = formula.calculateFunnelMetrics(scope.aggregateScope([row], 'paid'));
    assert.equal(result.cpc, null);
    assert.equal(result.roas, null);
    assert.equal(result.cps, null);
    assert.equal(result.margin, null);
  });

  await test('Formula Engine propagates unsupported required input as null', () => {
    const row = fixtures.metaPaid({ metricSupport: { checkout_value: 'unsupported' } });
    const result = formula.calculateFunnelMetrics(scope.aggregateScope([row], 'paid'));
    assert.equal(result.abandoned_value, null);
  });

  await test('Intent formulas are Paid-only and deterministic', () => {
    const paid = scope.aggregateIntentPaid([fixtures.metaPaid(), fixtures.metaOrganic()]);
    const result = formula.calculateIntentMetrics(paid);
    assert.equal(result.add_to_cart_rate, 20);
    assert.equal(result.checkout_rate, 50);
    assert.equal(result.abandoned_rate, 50);
    assert.equal(result.purchase_rate, 50);
  });

  await test('Blend CTR is recomputed from aggregate facts, not averaged row CTR', () => {
    const aggregate = scope.aggregateScope([fixtures.metaPaid(), fixtures.metaOrganic()], 'blend');
    const result = formula.calculateFunnelMetrics(aggregate);
    assert.equal(result.ctr, 12.5); // 150 / 1200 * 100
    assert.notEqual(result.ctr, 17.5); // not AVG(10%,25%)
  });

  // Repository
  await test('In-memory repository UPSERT replaces same canonical identity instead of duplicating', async () => {
    const repo = new InMemoryDatasetRepository();
    const first = fixtures.metaPaid();
    const second = fixtures.metaPaid({ metrics: { purchase_value: 999 } });
    await repo.upsertCanonicalRawFacts([first, second]);
    const rows = await repo.readCanonicalRawFacts({ user_id: 'user-1', from: '2026-08-15', to: '2026-08-15' });
    assert.equal(rows.length, 1);
    assert.equal(rows[0].raw_metrics.purchase_value, 999);
  });

  await test('Repository keeps Klaviyo Campaign Message and Flow Message separate even with same leaf ID', async () => {
    const repo = new InMemoryDatasetRepository([fixtures.klaviyoCampaignEmail(), fixtures.klaviyoFlowSms()]);
    const rows = await repo.readCanonicalRawFacts({ user_id: 'user-1', from: '2026-08-15', to: '2026-08-15', platform: 'klaviyo' });
    assert.equal(rows.length, 2);
    assert.notEqual(rows[0].entity_key, rows[1].entity_key);
  });

  // Query service
  await test('Query Service returns Funnel-ready paid result through repository -> scope -> formula chain', async () => {
    const repo = new InMemoryDatasetRepository([fixtures.metaPaid(), fixtures.metaOrganic()]);
    const service = new FunnelQueryService({ repository: repo });
    const result = await service.query({
      user_id: 'user-1', from: '2026-08-15', to: '2026-08-15', platform: 'meta', analysis_scope: 'paid'
    });
    assert.equal(result.totals.funnel_click, 100);
    assert.equal(result.totals.ctr, 10);
    assert.equal(result.totals.roas, 2.5);
    assert.equal(result.meta.currency, 'TRY');
    assert.equal(result.meta.formula_engine_version, 'v1');
    assert.equal(result.meta.canonical_contract_version, 'v1');
    assert.equal(result.rows.length, 1);
  });

  await test('Query Service returns Organic result through session click basis', async () => {
    const repo = new InMemoryDatasetRepository([fixtures.metaPaid(), fixtures.metaOrganic()]);
    const service = new FunnelQueryService({ repository: repo });
    const result = await service.query({ user_id: 'user-1', from: '2026-08-15', to: '2026-08-15', platform: 'meta', analysis_scope: 'organic' });
    assert.equal(result.totals.funnel_click, 50);
    assert.equal(result.totals.sales, 120);
  });

  await test('Query Service returns Blend result from additive facts then formulas', async () => {
    const repo = new InMemoryDatasetRepository([fixtures.metaPaid(), fixtures.metaOrganic()]);
    const service = new FunnelQueryService({ repository: repo });
    const result = await service.query({ user_id: 'user-1', from: '2026-08-15', to: '2026-08-15', platform: 'meta', analysis_scope: 'blend' });
    assert.equal(result.totals.funnel_click, 150);
    assert.equal(result.totals.sales, 370);
    assert.equal(result.totals.roas, 3.7);
  });

  await test('Query Service preserves unsupported/null in Funnel-ready output', async () => {
    const unsupported = fixtures.metaPaid({ metricSupport: { checkout_value: 'unsupported' } });
    const repo = new InMemoryDatasetRepository([unsupported]);
    const service = new FunnelQueryService({ repository: repo });
    const result = await service.query({ user_id: 'user-1', from: '2026-08-15', to: '2026-08-15', platform: 'meta', analysis_scope: 'paid' });
    assert.equal(result.totals.funnel_checkout_value, null);
    assert.equal(result.totals.metric_support.funnel_checkout_value, 'unsupported');
    assert.equal(result.totals.abandoned_value, null);
  });

  await test('Same fixture query produces deterministic identical output', async () => {
    const repo = new InMemoryDatasetRepository([fixtures.metaPaid(), fixtures.metaOrganic()]);
    const service = new FunnelQueryService({ repository: repo });
    const query = { user_id: 'user-1', from: '2026-08-15', to: '2026-08-15', platform: 'meta', analysis_scope: 'blend' };
    const a = JSON.stringify(await service.query(query));
    const b = JSON.stringify(await service.query(query));
    assert.equal(a, b);
  });

  await test('Query Service rejects mixed target currencies before aggregate', async () => {
    const tryRow = fixtures.metaPaid();
    const usdRow = fixtures.metaOrganic({ targetCurrency: 'USD', sourceCurrency: 'USD', fxRate: 1 });
    const repo = new InMemoryDatasetRepository([tryRow, usdRow]);
    const service = new FunnelQueryService({ repository: repo });
    await assert.rejects(
      service.query({ user_id: 'user-1', from: '2026-08-15', to: '2026-08-15', platform: 'meta', analysis_scope: 'blend' }),
      /must share one target currency/
    );
  });

  console.log(`\nPHASE 1 TEST RESULT: ${passed} tests passed.`);
})().catch((error) => {
  console.error(error.stack || error);
  process.exitCode = 1;
});
