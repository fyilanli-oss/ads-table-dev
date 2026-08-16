'use strict';

const { CANONICAL_CONTRACT_VERSION } = require('./canonical-contract');
const { ANALYSIS_SCOPES, normalizeScope, aggregateScope, aggregateIntentPaid } = require('./analysis-scope');
const { calculateFunnelMetrics, calculateIntentMetrics, FORMULA_ENGINE_VERSION } = require('./formula-engine');

function ensureDateRange(from, to) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(from || '') || !/^\d{4}-\d{2}-\d{2}$/.test(to || '')) {
    throw new Error('from/to must be YYYY-MM-DD');
  }
  if (from > to) throw new Error('from must be <= to');
}

function inferTargetCurrency(rows) {
  const currencies = new Set(rows.map((row) => row.currency.target_currency));
  if (currencies.size > 1) throw new Error('Query rows must share one target currency before aggregation');
  return currencies.size === 1 ? [...currencies][0] : null;
}

function entityGroupKey(row) {
  return row.entity_key || [row.identity.platform, row.identity.platform_account_id, row.entity.entity_type, row.entity.entity_id].join('|');
}

function groupByEntity(rows) {
  const groups = new Map();
  for (const row of rows) {
    const key = entityGroupKey(row);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  }
  return groups;
}

class FunnelQueryService {
  constructor({ repository }) {
    if (!repository || typeof repository.readCanonicalRawFacts !== 'function') {
      throw new Error('FunnelQueryService requires a dataset repository');
    }
    this.repository = repository;
  }

  async query({ user_id, from, to, platform = null, platform_account_id = null, entity_key = null, analysis_scope = ANALYSIS_SCOPES.PAID } = {}) {
    if (!user_id) throw new Error('user_id is required');
    ensureDateRange(from, to);
    const scope = normalizeScope(analysis_scope);

    const rawRows = await this.repository.readCanonicalRawFacts({
      user_id,
      from,
      to,
      platform,
      platform_account_id,
      entity_key
    });

    const currency = inferTargetCurrency(rawRows);
    const totalAggregate = aggregateScope(rawRows, scope);
    const totals = { ...totalAggregate, ...calculateFunnelMetrics(totalAggregate) };

    const paidIntentAggregate = aggregateIntentPaid(rawRows);
    const intent = calculateIntentMetrics(paidIntentAggregate);

    const rows = [];
    for (const [groupKey, groupRows] of groupByEntity(rawRows)) {
      const aggregate = aggregateScope(groupRows, scope);
      if (aggregate.row_count === 0) continue;
      rows.push({
        entity_key: groupKey,
        platform: groupRows[0].identity.platform,
        platform_account_id: groupRows[0].identity.platform_account_id,
        entity: groupRows[0].entity,
        ...aggregate,
        ...calculateFunnelMetrics(aggregate)
      });
    }

    rows.sort((a, b) => a.entity_key.localeCompare(b.entity_key));

    return {
      period: { from, to },
      rows,
      totals,
      intent,
      meta: {
        analysis_scope: scope,
        formula_engine_version: FORMULA_ENGINE_VERSION,
        canonical_contract_version: CANONICAL_CONTRACT_VERSION,
        currency,
        metric_support: totals.metric_support
      }
    };
  }
}

module.exports = Object.freeze({
  FunnelQueryService,
  inferTargetCurrency,
  groupByEntity
});
