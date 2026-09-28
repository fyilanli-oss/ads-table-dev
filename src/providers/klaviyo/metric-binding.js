'use strict';

function codedError(code, status = 409) {
  return Object.assign(new Error(code), { code, status });
}

function sameIntegration(left, right) {
  return String(left?.integration_name || '').trim().toLowerCase() ===
    String(right?.integration_name || '').trim().toLowerCase();
}

function relatedMetric(candidates, purchase) {
  const matches = candidates.filter(candidate => sameIntegration(candidate, purchase));
  if (matches.length > 1) throw codedError('KLAVIYO_JOURNEY_METRIC_AMBIGUOUS');
  return matches[0] || null;
}

function createKlaviyoMetricBinding({ connectionStore, providerClient, tokenLifecycle = null } = {}) {
  if (!connectionStore || typeof connectionStore.resolveConnected !== 'function' ||
    typeof connectionStore.bindKlaviyoJourneyMetrics !== 'function') {
    throw new TypeError('canonical connection store with metric binding is required');
  }
  if (!providerClient || typeof providerClient.fetchJourneyMetricCandidates !== 'function') {
    throw new TypeError('Klaviyo metric discovery client is required');
  }

  async function discover(authority) {
    const connection = await connectionStore.resolveConnected({ authority, provider: 'klaviyo' });
    if (!connection) throw codedError('KLAVIYO_PREFLIGHT_CONNECTION_REQUIRED');
    let journeyCandidates;
    try {
      const operation = active => providerClient.fetchJourneyMetricCandidates({ accessToken: active.accessToken });
      journeyCandidates = tokenLifecycle
        ? (await tokenLifecycle.run({ authority, connection, operation })).value
        : await operation(connection);
    } catch (error) {
      if (error?.message === 'KLAVIYO_REAUTHORIZE') throw codedError('KLAVIYO_REAUTHORIZE');
      throw codedError('KLAVIYO_METRIC_DISCOVERY_FAILED', 503);
    }
    if (journeyCandidates.purchase.length === 0) throw codedError('KLAVIYO_CONVERSION_METRIC_NOT_FOUND');
    return Object.freeze({
      status: 'KLAVIYO_CONVERSION_METRIC_SELECTION_REQUIRED',
      candidate_count: journeyCandidates.purchase.length,
      candidates: journeyCandidates.purchase,
      journey_candidate_counts: Object.freeze({
        add_to_cart: journeyCandidates.addToCart.length,
        checkout: journeyCandidates.checkout.length,
        purchase: journeyCandidates.purchase.length,
      }),
      dataset_v2_write: false,
      connection_write: false,
    });
  }

  async function select(authority, body = {}) {
    const requestedId = typeof body.metric_id === 'string' ? body.metric_id.trim() : '';
    if (!requestedId) throw codedError('KLAVIYO_CONVERSION_METRIC_SELECTION_INVALID');
    let connection = await connectionStore.resolveConnected({ authority, provider: 'klaviyo' });
    if (!connection) throw codedError('KLAVIYO_PREFLIGHT_CONNECTION_REQUIRED');
    let candidates;
    try {
      const operation = active => providerClient.fetchJourneyMetricCandidates({ accessToken: active.accessToken });
      if (tokenLifecycle) {
        const execution = await tokenLifecycle.run({ authority, connection, operation });
        candidates = execution.value;
        connection = execution.connection;
      } else candidates = await operation(connection);
    } catch (error) {
      if (error?.message === 'KLAVIYO_REAUTHORIZE') throw codedError('KLAVIYO_REAUTHORIZE');
      throw codedError('KLAVIYO_METRIC_DISCOVERY_FAILED', 503);
    }
    const selected = candidates.purchase.find(candidate => candidate.id === requestedId);
    if (!selected) throw codedError('KLAVIYO_CONVERSION_METRIC_SELECTION_INVALID');
    const metrics = Object.freeze({
      addToCart: relatedMetric(candidates.addToCart, selected),
      checkout: relatedMetric(candidates.checkout, selected),
      purchase: selected,
    });
    await connectionStore.bindKlaviyoJourneyMetrics({
      authority,
      version: connection.version,
      accountId: connection.activeAccountId,
      metrics,
    });
    return Object.freeze({
      status: 'KLAVIYO_CONVERSION_METRIC_BOUND',
      dataset_v2_write: false,
      connection_write: true,
      journey_bindings: Object.freeze({
        add_to_cart: metrics.addToCart?.name || null,
        checkout: metrics.checkout?.name || null,
        purchase: metrics.purchase.name,
      }),
    });
  }

  return Object.freeze({ discover, select });
}

module.exports = Object.freeze({ createKlaviyoMetricBinding });



