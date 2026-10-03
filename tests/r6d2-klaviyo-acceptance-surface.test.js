'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { renderEmbeddedPlatforms } = require('../src/shopify/embedded-app-home');

test('R6-D2 acceptance control is hidden from the normal Data Sources experience', () => {
  const html = renderEmbeddedPlatforms({
    clientId: 'client-id',
    providerOAuthEnabled: true,
    providerAvailability: { meta: true, google_ads: true, klaviyo: true },
  });
  assert.match(html, /id="r6d2-klaviyo-acceptance" display="none"/);
  assert.match(html, /params\.get\("acceptance"\) === "r6d2-klaviyo"/);
  assert.match(html, /\/api\/shopify\/providers\/klaviyo\/runtime\/preflight/);
  assert.match(html, /\/api\/shopify\/providers\/klaviyo\/runtime\/metrics/);
  assert.match(html, /Confirm metric and continue/);
  assert.match(html, /<s-date-field id="r6d2-klaviyo-provider-date"/);
  assert.match(html, /through today/);
  assert.match(html, /provider_date_finality\.toUpperCase/);
  assert.doesNotMatch(html, /Date\.now\(\) - \(48 \* 60 \* 60 \* 1000\)/);
  assert.match(html, /provider_date: String\(acceptanceProviderDate\.value/);
  assert.match(html, /campaign_row_count/);
  assert.match(html, /flow_row_count/);
  assert.match(html, /VERIFIED EMPTY/);
  assert.match(html, /Dataset V2 writes: 0/);
  const acceptanceMarkup = html.slice(
    html.indexOf('<s-stack id="r6d2-klaviyo-acceptance"'),
    html.indexOf('<s-stack id="currency-setup"'),
  );
  assert.doesNotMatch(acceptanceMarkup, /row\.identity|active_account_id|accessToken|account-1/);
});

test('R6-D2 C6 controlled write stays inside the acceptance surface and is confirmation-bound', () => {
  const html = renderEmbeddedPlatforms({
    clientId: 'client-id',
    providerOAuthEnabled: true,
    providerAvailability: { meta: true, google_ads: true, klaviyo: true },
  });
  assert.match(html, /id="r6d2-klaviyo-c6-run"/);
  assert.match(html, /\/api\/shopify\/providers\/klaviyo\/runtime\/acceptance/);
  assert.match(html, /RUN_R6_D2_C6_KLAVIYO_WRITE/);
  assert.match(html, /Do not retry; review runtime evidence/);
  assert.match(html, /id="r6d2-klaviyo-c6-run" tone="critical" disabled/);
  const acceptanceMarkup = html.slice(
    html.indexOf('<s-stack id="r6d2-klaviyo-acceptance"'),
    html.indexOf('<s-stack id="currency-setup"'),
  );
  assert.match(acceptanceMarkup, /may write real verified Klaviyo rows to Dataset V2/);
  assert.doesNotMatch(acceptanceMarkup, /row\.identity|active_account_id|accessToken|account-1/);
});

test('R6-D5 journey diagnostic stays hidden, date-selectable and exposes performance plus journey aggregates', () => {
  const html = renderEmbeddedPlatforms({
    clientId: 'client-id',
    providerOAuthEnabled: true,
    providerAvailability: { meta: true, google_ads: true, klaviyo: true },
  });
  assert.match(html, /id="r6d5-klaviyo-historical-inventory" display="none"/);
  assert.match(html, /id="r6d5-klaviyo-journey-diagnostic-date"/);
  assert.match(html, /Run journey diagnostic/);
  assert.match(html, /\/api\/shopify\/providers\/klaviyo\/runtime\/journey-diagnostic/);
  assert.match(html, /PASS_R6_D5_KLAVIYO_JOURNEY_DIAGNOSTIC/);
  assert.match(html, /diagnostics\.performance/);
  assert.match(html, /unique opens/);
  assert.match(html, /unique clicks/);
  assert.match(html, /unmatched_key_count/);
  assert.match(html, /Dataset V2 writes: 0/);
  const diagnosticMarkup = html.slice(
    html.indexOf('This read-only diagnostic shows Campaign and Flow'),
    html.indexOf('<s-stack gap="large">'),
  );
  assert.match(diagnosticMarkup, /s-date-field/);
  assert.match(diagnosticMarkup, /Open attribution dates are provisional/);
  assert.doesNotMatch(diagnosticMarkup, /workspace_id|account_id|campaign_id|flow_id|message_id|accessToken/);
});
