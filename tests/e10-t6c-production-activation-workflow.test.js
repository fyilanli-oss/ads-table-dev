'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const test = require('node:test');
const workflow = fs.readFileSync('.github/workflows/e10-t6c-production-activation.yml', 'utf8');

test('workflow has only manual trigger and two non-defaulted approvals', () => {
  const trigger = workflow.match(/^on:\n([\s\S]*?)\npermissions:/m)?.[1] || '';
  assert.match(trigger, /^  workflow_dispatch:\n/);
  assert.doesNotMatch(trigger, /^  (push|pull_request|schedule|deployment|workflow_run|repository_dispatch):/m);
  assert.equal((trigger.match(/^      [a-z_]+:\n/gm) || []).length, 2);
  assert.match(trigger, /activation_confirmation:[\s\S]*?required: true/);
  assert.match(trigger, /production_deploy_approval:[\s\S]*?required: true/);
  assert.doesNotMatch(trigger, /^\s+default:/m);
});

test('execution is main-only, least privilege, serialized, and Production-gated', () => {
  assert.match(workflow, /^permissions:\n  contents: read\n/m);
  assert.match(workflow, /^concurrency:\n  group: e10-t6c-production-activation\n  cancel-in-progress: false$/m);
  assert.match(workflow, /if: github\.ref == 'refs\/heads\/main'/);
  assert.match(workflow, /^    environment: Production$/m);
});

test('checkout is immutable and does not persist credentials', () => {
  const sha = workflow.match(/uses: actions\/checkout@([^\n]+)[\s\S]*?persist-credentials: false/)?.[1];
  assert.match(sha || '', /^[0-9a-f]{40}$/);
});

test('approval constants match the existing operator exactly', () => {
  const operator = fs.readFileSync('scripts/e10-t6c-vercel-activation.sh', 'utf8');
  for (const value of ['E10-T6C-ACTIVATE-DEVELOPMENT-OAUTH', 'E10-T6C-APPROVE-PRODUCTION-DEPLOY']) {
    assert.match(workflow, new RegExp(value));
    assert.match(operator, new RegExp(value));
  }
  assert.match(workflow, /run: bash scripts\/e10-t6c-vercel-activation\.sh/);
});

test('VERCEL_TOKEN is scoped only to the final operator step', () => {
  assert.equal((workflow.match(/VERCEL_TOKEN:/g) || []).length, 1);
  const finalStep = workflow.slice(workflow.indexOf('- name: Activate Production'));
  assert.match(finalStep, /VERCEL_TOKEN: \$\{\{ secrets\.VERCEL_TOKEN \}\}/);
  assert.match(finalStep, /run: bash scripts\/e10-t6c-vercel-activation\.sh/);
});

test('workflow never dispatches itself', () => {
  assert.doesNotMatch(workflow, /(?:gh workflow run|workflow_dispatch\s*\/dispatches|actions\/github-script|repository_dispatch)/);
});
