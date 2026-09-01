'use strict';

const CANDIDATE_METRICS = Object.freeze([
  'add_to_cart', 'add_to_cart_value', 'checkout', 'checkout_value',
  'initiate_checkout', 'initiate_checkout_value', 'complete_payment',
  'complete_payment_count', 'complete_payment_value'
]);

function safeProbe(metric, data) {
  const list = Array.isArray(data?.data?.list) ? data.data.list : [];
  return Object.freeze({ metric, accepted: true, field_present: list.some(row => Object.hasOwn(row?.metrics || {}, metric)), result_shape: list.length ? 'non_empty' : 'zero_row' });
}

function registerTikTokCharacterizationRoute({app, requireUser, fetchReport, enabled, sandboxBase, accessToken, advertiserId, now = () => new Date()} = {}) {
  if (!app || typeof app.get !== 'function') throw new TypeError('app is required');
  for (const [name, fn] of Object.entries({ requireUser, fetchReport, now })) if (typeof fn !== 'function') throw new TypeError(`${name} must be a function`);
  app.get('/api/tiktok/sandbox/characterize', async (req, res) => {
    const user = await requireUser(req, res); if (!user) return;
    if (!enabled) return res.status(404).json({ error: 'TikTok characterization is disabled' });
    if (!accessToken || !advertiserId) return res.status(503).json({ error: 'TikTok characterization is not ready' });
    const date = now().toISOString().slice(0, 10), probes = [];
    for (const metric of CANDIDATE_METRICS) {
      try {
        const data = await fetchReport({ base: sandboxBase, endpoint: '/v1.3/report/integrated/get/', headers: { 'Access-Token': accessToken }, params: { report_type: 'BASIC', service_type: 'AUCTION', data_level: 'AUCTION_AD', advertiser_id: advertiserId, start_date: date, end_date: date, dimensions: ['ad_id'], metrics: ['spend', 'impressions', 'clicks', metric], page: 1, page_size: 1 } });
        probes.push(safeProbe(metric, data));
      } catch { probes.push(Object.freeze({ metric, accepted: false, field_present: false, result_shape: 'rejected' })); }
    }
    return res.json({ platform: 'tiktok', mode: 'sandbox_read_only_characterization', date_scope: 'single_day', writes_performed: false, raw_response_included: false, probes });
  });
}

module.exports = Object.freeze({ CANDIDATE_METRICS, registerTikTokCharacterizationRoute, safeProbe });
