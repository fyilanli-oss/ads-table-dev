'use strict';

const { normalizeCurrencyCode } = require('../../../funnel-core/fx-service');
const { TIME_ENGINE_VERSION, businessDateFromTimestamp, validateTimeZone } = require('../../../funnel-core/time-service');

const GOOGLE_CUSTOMER_METADATA_QUERY = 'SELECT customer.id, customer.currency_code, customer.time_zone FROM customer LIMIT 1';

function customerId(value, field = 'customer.id') {
  const normalized = String(value ?? '').replace(/-/g, '').trim();
  if (!/^\d+$/.test(normalized)) throw new Error(`${field} must be a Google customer id`);
  return normalized;
}

function readCustomer(row) {
  if (!row || typeof row !== 'object' || Array.isArray(row)) throw new TypeError('Google customer result is required');
  const customer = row.customer;
  if (!customer || typeof customer !== 'object' || Array.isArray(customer)) throw new TypeError('Google customer metadata is required');
  return customer;
}

function normalizeGoogleCustomerMetadata(response, { requestedCustomerId, observedAt = new Date() } = {}) {
  if (!response || !Array.isArray(response.results) || response.results.length !== 1) throw new Error('Google customer metadata must return exactly one row');
  const source = readCustomer(response.results[0]);
  const id = customerId(source.id);
  if (id !== customerId(requestedCustomerId, 'requestedCustomerId')) throw new Error('Google customer identity mismatch');
  const currency = normalizeCurrencyCode(source.currencyCode ?? source.currency_code, 'customer.currency_code');
  const timeZone = validateTimeZone(source.timeZone ?? source.time_zone);
  return Object.freeze({ id, source_currency: currency, source_timezone: timeZone, business_date: businessDateFromTimestamp(observedAt, timeZone), time_engine_version: TIME_ENGINE_VERSION });
}

function googleCustomerMetadataEvidence(metadata) {
  if (!metadata || typeof metadata !== 'object') throw new TypeError('normalized Google customer metadata is required');
  return Object.freeze({ currency_present: /^[A-Z]{3}$/.test(metadata.source_currency), timezone_present: typeof metadata.source_timezone === 'string', business_date: metadata.business_date, time_engine_version: metadata.time_engine_version, identity_verified: true });
}

module.exports = Object.freeze({ GOOGLE_CUSTOMER_METADATA_QUERY, customerId, googleCustomerMetadataEvidence, normalizeGoogleCustomerMetadata });
