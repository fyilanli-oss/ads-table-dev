'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const { FIXTURES, auditCanonicalFixtureKeys } = require('../security/e2-t6-static-root-cause');
const { buildEntityKey } = require('../funnel-core/entity-hierarchy');

const repo = path.join(__dirname, '..');
const transaction = fs.readFileSync(path.join(repo, 'docs/security/sql/E2_T6_RLS_V2_TRANSACTION.sql'), 'utf8');
const report = require('../artifacts/dataset-v2-acceptance/e2-t6-rls/static-root-cause-v1.json');

test('E2-T6 static audit binds both historical V2 fixtures to their SQL identity fields', () => {
  assert.equal(FIXTURES.length, 2);
  for (const fixture of FIXTURES) {
    for (const value of [fixture.identity.platform_account_id, fixture.entity.root_entity_id, fixture.entity.parent_entity_id, fixture.entity.entity_id, fixture.transactionEntityKey]) {
      assert.match(transaction, new RegExp(`'${value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}'`));
    }
  }
});

test('E2-T6 V2 historical fixture keys violate the frozen canonical key contract', () => {
  const audit = auditCanonicalFixtureKeys();
  assert.equal(audit.fixtureCount, 2);
  assert.equal(audit.canonicalKeyMismatchCount, 2);
  assert(audit.findings.every(({ checkCode, passed }) => checkCode === 'CANONICAL_ENTITY_KEY' && passed === false));
  for (const fixture of FIXTURES) assert.notEqual(fixture.transactionEntityKey, buildEntityKey(fixture.identity, fixture.entity));
});

test('E2-T6 root-cause record is redacted, fail-closed, and does not overclaim causality', () => {
  assert.equal(report.production_operation_run, false);
  assert.equal(report.confirmed_findings[0].affected_fixture_count, 2);
  assert.deepEqual(report.unresolved_findings.map(({ check_code }) => check_code), ['TERMINAL_FAILURE_CAUSE_UNCLASSIFIED', 'POSTCHECK_FAILURE_CAUSE_UNCLASSIFIED']);
  assert.match(report.decision, /no production retry/);
  assert.doesNotMatch(JSON.stringify(report), /https?:\/\/|postgres(?:ql)?:\/\/|bearer\s|[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/i);
});
