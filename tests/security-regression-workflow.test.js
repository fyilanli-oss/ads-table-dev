'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const workflow = fs.readFileSync(path.join(__dirname, '..', '.github/workflows/security-regression.yml'), 'utf8');
test('security workflow has safe triggers, permissions, concurrency, and pinned actions', () => {
  assert.match(workflow, /^on:\n  pull_request:\n  push:\n    branches: \[main\]/m);
  assert.match(workflow, /^permissions:\n  contents: read$/m);
  assert.match(workflow, /^concurrency:/m);
  const actions = [...workflow.matchAll(/uses: (actions\/[\w-]+)@([^\s]+)/g)].map(match => `${match[1]}@${match[2]}`);
  assert.deepEqual(actions, [
    'actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd',
    'actions/setup-node@2028fbc5c25fe9cf00d9f06a71cc4710d4507903'
  ]);
  assert.match(workflow, /persist-credentials: false/);
  assert.match(workflow, /node-version: "22\.18\.0"/);
});
test('security workflow uses locked tests without production capabilities or credentials', () => {
  assert.match(workflow, /npm ci --ignore-scripts --no-audit --no-fund/);
  assert.match(workflow, /run: npm run test:security/); assert.match(workflow, /run: npm test/);
  assert.doesNotMatch(workflow, /\b(?:environment|secrets|vars):|\$\{\{\s*(?:secrets|vars)\./i);
  assert.doesNotMatch(workflow, /supabase|vercel|deploy|migration|workflow[_ -]dispatch|provider/i);
  assert.doesNotMatch(workflow, /github\.event\.pull_request|github\.event\.issue|github\.event\.comment/);
});
