'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { renderEmbeddedPlatforms } = require('../src/shopify/embedded-app-home');

const contract = require('../contracts/r6d5k4-klaviyo-provisional-reconciliation-v1.json');

test('R6-D5-K4 contract removes the 48-hour gate and requires hourly provisional reconciliation', () => {
  assert.equal(contract.read_eligibility.today, true);
  assert.equal(contract.read_eligibility.yesterday, true);
  assert.equal(contract.read_eligibility.fixed_48_hour_gate, false);
  assert.equal(contract.finality.refresh_interval_minutes, 60);
  assert.equal(contract.finality.finalize_only_after_configured_attribution_window, true);
  assert.equal(contract.finality.default_is_configurable_not_universal, true);
  assert.equal(contract.writes.production_snapshot_schedule_activation, false);
});

test('R6-D5-K4 Shopify surface uses official components, allows today and exposes finality', () => {
  const html = renderEmbeddedPlatforms({
    clientId: 'client-id',
    providerOAuthEnabled: true,
    providerAvailability: { meta: true, google_ads: true, klaviyo: true },
  });
  assert.match(html, /<s-date-field id="r6d2-klaviyo-provider-date"/);
  assert.match(html, /new Date\(\)\.toISOString\(\)\.slice\(0, 10\)/);
  assert.match(html, /provider_date_finality\.toUpperCase\(\)/);
  assert.doesNotMatch(html, /Date\.now\(\) - \(48 \* 60 \* 60 \* 1000\)/);
  assert.doesNotMatch(html, /<style|style=|<button|<input|<select/i);
});

test('R6-D5-K4 analyst brief records desktop and real mobile acceptance gates', () => {
  const brief = fs.readFileSync(path.join(__dirname, '..', 'docs', 'R6D5K4_KLAVIYO_PROVISIONAL_RECONCILIATION.md'), 'utf8');
  assert.match(brief, /Desktop ve gerçek mobil Shopify Admin/);
  assert.match(brief, /açıkça kabul edilmeden paket `Done`, `PASS` veya merge sayılamaz/);
});
