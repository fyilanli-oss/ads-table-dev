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
  assert.match(html, /id="r6d2-klaviyo-acceptance" hidden/);
  assert.match(html, /params\.get\("acceptance"\) === "r6d2-klaviyo"/);
  assert.match(html, /\/api\/shopify\/providers\/klaviyo\/runtime\/preflight/);
  assert.match(html, /\/api\/shopify\/providers\/klaviyo\/runtime\/metrics/);
  assert.match(html, /Confirm metric and continue/);
  assert.match(html, /Dataset V2 writes: 0/);
  const acceptanceMarkup = html.slice(
    html.indexOf('<div id="r6d2-klaviyo-acceptance"'),
    html.indexOf('<div id="currency-setup"'),
  );
  assert.doesNotMatch(acceptanceMarkup, /row\.identity|active_account_id|accessToken|account-1/);
});
