'use strict';

const CONVERSION_MAPPING_VERSION = 'google-conversions-v1';
const CONVERSION_RULES = Object.freeze({
  add_to_cart: Object.freeze({ categories: Object.freeze(['ADD_TO_CART']), names: Object.freeze(['add_to_cart', 'add to cart']) }),
  checkout: Object.freeze({ categories: Object.freeze(['BEGIN_CHECKOUT']), names: Object.freeze(['begin_checkout', 'begin checkout', 'start_checkout', 'start checkout']) }),
  purchase: Object.freeze({ categories: Object.freeze(['PURCHASE']), names: Object.freeze(['purchase', 'placed_order', 'placed order']) })
});

function nonNegative(value, field) {
  if (value === null || value === undefined || value === '') return 0;
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) throw new Error(`${field} must be a non-negative number`);
  return parsed;
}

function normalizedName(value) { return String(value || '').trim().toLowerCase(); }
function normalizedCategory(value) { return String(value || '').trim().toUpperCase(); }

function selectConversion(actions, kind) {
  if (!Array.isArray(actions)) throw new TypeError('Google conversion actions must be an array');
  const rule = CONVERSION_RULES[kind];
  if (!rule) throw new Error('Unknown Google conversion kind');
  const categoryMatches = actions.filter(action => rule.categories.includes(normalizedCategory(action?.category)));
  const selected = categoryMatches.length ? categoryMatches : actions.filter(action => rule.names.includes(normalizedName(action?.name)));
  if (!selected.length) return Object.freeze({ count: null, value: null, provenance: Object.freeze({ mapping_version: CONVERSION_MAPPING_VERSION, match_basis: null, matched_action_count: 0, categories: Object.freeze([]), fallback_used: false }) });
  const categories = [...new Set(selected.map(action => normalizedCategory(action?.category)).filter(Boolean))].sort();
  return Object.freeze({
    count: selected.reduce((sum, action) => sum + nonNegative(action?.conversions, `${kind}.conversions`), 0),
    value: selected.reduce((sum, action) => sum + nonNegative(action?.conversions_value, `${kind}.conversions_value`), 0),
    provenance: Object.freeze({ mapping_version: CONVERSION_MAPPING_VERSION, match_basis: categoryMatches.length ? 'category' : 'exact_name_fallback', matched_action_count: selected.length, categories: Object.freeze(categories), fallback_used: categoryMatches.length === 0 })
  });
}

function mapGoogleConversions(actions) {
  return Object.freeze(Object.fromEntries(Object.keys(CONVERSION_RULES).map(kind => [kind, selectConversion(actions, kind)])));
}

module.exports = Object.freeze({ CONVERSION_MAPPING_VERSION, CONVERSION_RULES, mapGoogleConversions, selectConversion });
