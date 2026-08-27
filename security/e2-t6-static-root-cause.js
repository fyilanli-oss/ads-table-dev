'use strict';

const { buildEntityKey } = require('../funnel-core/entity-hierarchy');

const FIXTURES = Object.freeze([
  Object.freeze({
    actor: 'user_a',
    identity: Object.freeze({ platform: 'meta', platform_account_id: 'e2_t6_rls_v2_account_a', traffic_type: 'paid', channel: null }),
    entity: Object.freeze({ campaign_type: null, root_entity_type: 'campaign', root_entity_id: 'e2_t6_rls_v2_campaign_a', root_entity_name: 'E2 T6 Campaign', parent_entity_type: 'adset', parent_entity_id: 'e2_t6_rls_v2_adset_a', parent_entity_name: 'E2 T6 AdSet', entity_type: 'ad', entity_id: 'e2_t6_rls_v2_ad_a', entity_name: 'E2 T6 Ad' }),
    transactionEntityKey: 'e2_t6_rls_v2:a'
  }),
  Object.freeze({
    actor: 'user_b',
    identity: Object.freeze({ platform: 'meta', platform_account_id: 'e2_t6_rls_v2_account_b', traffic_type: 'paid', channel: null }),
    entity: Object.freeze({ campaign_type: null, root_entity_type: 'campaign', root_entity_id: 'e2_t6_rls_v2_campaign_b', root_entity_name: 'E2 T6 Campaign', parent_entity_type: 'adset', parent_entity_id: 'e2_t6_rls_v2_adset_b', parent_entity_name: 'E2 T6 AdSet', entity_type: 'ad', entity_id: 'e2_t6_rls_v2_ad_b', entity_name: 'E2 T6 Ad' }),
    transactionEntityKey: 'e2_t6_rls_v2:b'
  })
]);

function auditCanonicalFixtureKeys(fixtures = FIXTURES) {
  const findings = fixtures.map(({ actor, identity, entity, transactionEntityKey }) => {
    const canonicalEntityKey = buildEntityKey(identity, entity);
    return Object.freeze({ actor, checkCode: 'CANONICAL_ENTITY_KEY', passed: transactionEntityKey === canonicalEntityKey });
  });
  return Object.freeze({
    operation: 'e2_t6_rls_v2',
    auditVersion: 'e2-t6-static-root-cause-v1',
    fixtureCount: findings.length,
    canonicalKeyMismatchCount: findings.filter(({ passed }) => !passed).length,
    findings: Object.freeze(findings)
  });
}

module.exports = Object.freeze({ FIXTURES, auditCanonicalFixtureKeys });
