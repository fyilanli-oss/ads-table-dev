'use strict';
const files = Object.freeze([
  'tests/oauth-security-baseline.test.js', 'tests/oauth-transaction-store.test.js',
  'tests/oauth-authorization-contract.test.js', 'tests/production-config.test.js',
  'tests/provider-token-vault.test.js', 'tests/provider-token-store.test.js',
  'tests/provider-token-backfill.test.js', 'tests/provider-token-backfill-operator.test.js',
  'tests/provider-token-backfill-write-operator.test.js', 'tests/provider-token-backfill-workflow.test.js',
  'tests/provider-token-backfill-write-workflow.test.js', 'tests/provider-token-schema.test.js',
  'tests/e1-t6d-auth-orphan-cleanup.test.js', 'tests/e1-t6e-plaintext-token-nulling.test.js',
  'tests/security-regression-contract.test.js', 'tests/security-regression-workflow.test.js',
  'tests/db-ledger-t2-artifacts.test.js', 'tests/e2-t3-roundtrip-artifacts.test.js', 'tests/e2-t4-upsert-artifacts.test.js',
  'tests/e2-t5-rejection-artifacts.test.js', 'tests/e2-t5-operator.test.js', 'tests/e2-t6-rls-artifacts.test.js', 'tests/e2-t6-operator.test.js', 'tests/e2-t6-v2-operator.test.js', 'tests/e2-t6-v3-operator.test.js', 'tests/e2-t6-v3-postcheck-diagnostic.test.js', 'tests/e2-t6-current-state-audit.test.js', 'tests/e2-t6-v4-operator.test.js', 'tests/e2-t7-cleanup-artifacts.test.js',
  'tests/e2-t8-restore-readiness-artifacts.test.js', 'tests/e3-t1-critical-route-characterization.test.js', 'tests/e3-t2-composition-root.test.js', 'tests/e3-t3-runtime-config.test.js', 'tests/e3-t4-shared-clients.test.js', 'tests/e3-t5a-http-middleware-boundary.test.js', 'tests/e3-t5b-access-boundary.test.js', 'tests/e3-t6a-public-route-registration.test.js', 'tests/e3-t6b-authenticated-route-registration.test.js', 'tests/e3-t7a-oauth-transaction-boundary.test.js', 'tests/e3-t7b1-oauth-route-registration.test.js', 'tests/e3-t7b2-meta-oauth-handlers.test.js', 'tests/e3-t7b3a-google-oauth-handlers.test.js', 'tests/e3-t7b3b1-google-sheets-oauth-handlers.test.js', 'tests/e3-t7b3b2-provider-oauth-handlers.test.js', 'tests/e3-t8a-refresh-job-boundary.test.js', 'tests/e3-t8b1-manual-snapshot-orchestrator.test.js', 'tests/e3-t8b2a-automation-snapshot-orchestrator.test.js', 'tests/e3-t8b2b2a-google-job-evidence.test.js', 'tests/e3-t8b2b2b-snapshot-job-evidence.test.js', 'tests/e3-t9-architecture-guard.test.js', 'tests/e3-t10-canonical-boundary-guard.test.js', 'tests/vercel-static-entrypoints.test.js'
]);
const groups = Object.freeze({
  auth: ['tests/oauth-security-baseline.test.js', 'tests/e3-t1-critical-route-characterization.test.js', 'tests/e3-t2-composition-root.test.js', 'tests/e3-t5a-http-middleware-boundary.test.js', 'tests/e3-t5b-access-boundary.test.js', 'tests/e3-t6a-public-route-registration.test.js', 'tests/e3-t6b-authenticated-route-registration.test.js', 'tests/e3-t7a-oauth-transaction-boundary.test.js', 'tests/e3-t7b1-oauth-route-registration.test.js', 'tests/e3-t7b2-meta-oauth-handlers.test.js', 'tests/e3-t7b3a-google-oauth-handlers.test.js', 'tests/e3-t7b3b1-google-sheets-oauth-handlers.test.js', 'tests/e3-t7b3b2-provider-oauth-handlers.test.js', 'tests/e3-t8a-refresh-job-boundary.test.js', 'tests/e3-t8b1-manual-snapshot-orchestrator.test.js', 'tests/e3-t8b2a-automation-snapshot-orchestrator.test.js', 'tests/e3-t8b2b2a-google-job-evidence.test.js', 'tests/e3-t8b2b2b-snapshot-job-evidence.test.js', 'tests/e3-t9-architecture-guard.test.js', 'tests/e3-t10-canonical-boundary-guard.test.js', 'tests/vercel-static-entrypoints.test.js'], idor: ['tests/oauth-authorization-contract.test.js', 'tests/e3-t5b-access-boundary.test.js'],
  tamper: ['tests/oauth-transaction-store.test.js', 'tests/e3-t7a-oauth-transaction-boundary.test.js', 'tests/provider-token-vault.test.js'],
  replay: ['tests/oauth-transaction-store.test.js', 'tests/e3-t7a-oauth-transaction-boundary.test.js'], expiry: ['tests/oauth-transaction-store.test.js'],
  'production-config': ['tests/production-config.test.js', 'tests/e3-t3-runtime-config.test.js'],
  'provider-token': ['tests/provider-token-vault.test.js', 'tests/provider-token-store.test.js', 'tests/e3-t4-shared-clients.test.js'],
  redaction: ['tests/production-config.test.js', 'tests/provider-token-backfill.test.js', 'tests/e2-t5-operator.test.js', 'tests/e2-t6-operator.test.js', 'tests/e2-t6-v2-operator.test.js', 'tests/e2-t6-v3-operator.test.js', 'tests/e2-t6-v3-postcheck-diagnostic.test.js', 'tests/e2-t6-current-state-audit.test.js', 'tests/e2-t6-v4-operator.test.js', 'tests/e3-t5a-http-middleware-boundary.test.js']
});
module.exports = Object.freeze({files, groups});
