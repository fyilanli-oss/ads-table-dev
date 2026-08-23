'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const manifest = require('./security-regression-manifest');
test('security manifest retains every required control group and executable file', () => {
  const required = ['auth', 'idor', 'tamper', 'replay', 'expiry', 'production-config', 'provider-token', 'redaction', 'readonly-gateway'];
  assert.deepEqual(Object.keys(manifest.groups).sort(), required.sort());
  for (const [group, files] of Object.entries(manifest.groups)) for (const file of files) assert.ok(manifest.files.includes(file), `${group}: ${file}`);
  for (const file of manifest.files) assert.ok(fs.existsSync(path.join(__dirname, '..', file)), `${file} must exist`);
});
test('test:security explicitly matches the security manifest', () => {
  assert.equal(require('../package.json').scripts['test:security'], `node --test ${manifest.files.join(' ')}`);
});
