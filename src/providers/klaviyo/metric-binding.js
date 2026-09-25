'use strict';

function codedError(code, status = 409) {
  return Object.assign(new Error(code), { code, status });
}

function createKlaviyoMetricBinding({ connectionStore, providerClient } = {}) {
  if (!connectionStore || typeof connectionStore.resolveConnected !== 'function' ||
    typeof connectionStore.bindKlaviyoConversionMetric !== 'function') {
    throw new TypeError('canonical connection store with metric binding is required');
  }
  if (!providerClient || typeof providerClient.fetchPlacedOrderMetricCandidates !== 'function') {
    throw new TypeError('Klaviyo metric discovery client is required');
  }

  async function discover(authority) {
    const connection = await connectionStore.resolveConnected({ authority, provider: 'klaviyo' });
    if (!connection) throw codedError('KLAVIYO_PREFLIGHT_CONNECTION_REQUIRED');
    let candidates;
    try {
      candidates = await providerClient.fetchPlacedOrderMetricCandidates({ accessToken: connection.accessToken });
    } catch (error) {
      if (error?.message === 'KLAVIYO_REAUTHORIZE') throw codedError('KLAVIYO_REAUTHORIZE');
      throw codedError('KLAVIYO_METRIC_DISCOVERY_FAILED', 503);
    }
    if (candidates.length === 0) throw codedError('KLAVIYO_CONVERSION_METRIC_NOT_FOUND');
    return Object.freeze({
      status: 'KLAVIYO_CONVERSION_METRIC_SELECTION_REQUIRED',
      candidate_count: candidates.length,
      candidates,
      dataset_v2_write: false,
      connection_write: false,
    });
  }

  async function select(authority, body = {}) {
    const requestedId = typeof body.metric_id === 'string' ? body.metric_id.trim() : '';
    if (!requestedId) throw codedError('KLAVIYO_CONVERSION_METRIC_SELECTION_INVALID');
    const connection = await connectionStore.resolveConnected({ authority, provider: 'klaviyo' });
    if (!connection) throw codedError('KLAVIYO_PREFLIGHT_CONNECTION_REQUIRED');
    let candidates;
    try {
      candidates = await providerClient.fetchPlacedOrderMetricCandidates({ accessToken: connection.accessToken });
    } catch (error) {
      if (error?.message === 'KLAVIYO_REAUTHORIZE') throw codedError('KLAVIYO_REAUTHORIZE');
      throw codedError('KLAVIYO_METRIC_DISCOVERY_FAILED', 503);
    }
    const selected = candidates.find(candidate => candidate.id === requestedId);
    if (!selected) throw codedError('KLAVIYO_CONVERSION_METRIC_SELECTION_INVALID');
    await connectionStore.bindKlaviyoConversionMetric({
      authority,
      version: connection.version,
      accountId: connection.activeAccountId,
      metric: selected,
    });
    return Object.freeze({
      status: 'KLAVIYO_CONVERSION_METRIC_BOUND',
      dataset_v2_write: false,
      connection_write: true,
    });
  }

  return Object.freeze({ discover, select });
}

module.exports = Object.freeze({ createKlaviyoMetricBinding });
