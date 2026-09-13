'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const test = require('node:test');
const workflow = fs.readFileSync('.github/workflows/e10-t6c-production-readiness.yml', 'utf8');
const operator = fs.readFileSync('scripts/e10-t6c-vercel-redeploy.sh', 'utf8');

test('readiness workflow is manual, main-only, serialized, and Production-gated', () => {
  const trigger = workflow.match(/^on:\n([\s\S]*?)\npermissions:/m)?.[1] || '';
  assert.match(trigger, /^  workflow_dispatch:\n/);
  assert.doesNotMatch(trigger, /^  (push|pull_request|schedule|deployment|workflow_run|repository_dispatch):/m);
  assert.match(trigger, /redeploy_confirmation:[\s\S]*?required: true/);
  assert.doesNotMatch(trigger, /^\s+default:/m);
  assert.match(workflow, /^permissions:\n  contents: read\n  id-token: write\n/m);
  assert.match(workflow, /if: github\.ref == 'refs\/heads\/main'/);
  assert.match(workflow, /^    environment: Production$/m);
  assert.match(workflow, /^  group: e10-t6c-production-readiness\n  cancel-in-progress: false$/m);
});

test('readiness workflow uses immutable checkout and scopes Vercel token to redeploy', () => {
  assert.match(workflow, /actions\/checkout@[0-9a-f]{40}[\s\S]*?persist-credentials: false/);
  assert.equal((workflow.match(/VERCEL_TOKEN:/g) || []).length, 1);
  assert.ok(workflow.indexOf('bash scripts/e10-t6c-vercel-redeploy.sh') < workflow.indexOf('node scripts/e10-t6c-remote-preflight.js'));
});

test('readiness operator cannot activate the feature flag or contact providers', () => {
  assert.match(operator, /E10-T6C-REDEPLOY-WITHOUT-ACTIVATION/);
  assert.match(operator, /vercel@59\.16\.0 deploy --prod --yes/);
  assert.doesNotMatch(operator, /env add|SHOPIFY_EMBEDDED_PROVIDER_OAUTH_ENABLED|curl|fetch|gh workflow run/);
  assert.doesNotMatch(workflow, /e10-t6c-vercel-activation|ACTIVATE-DEVELOPMENT-OAUTH|APPROVE-PRODUCTION-DEPLOY/);
});
