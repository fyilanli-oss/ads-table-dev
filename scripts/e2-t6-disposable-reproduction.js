'use strict';

const assert = require('node:assert/strict');
const cp = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const { buildEvidence } = require('./e2-t6-v3-evidence');

function psql(databaseUrl, file) {
  return cp.execFileSync('psql', ['--no-psqlrc', '--quiet', '--tuples-only', '--no-align', '--set', 'ON_ERROR_STOP=1', databaseUrl, '--file', file], { encoding: 'utf8' });
}

function main() {
  const databaseUrl = process.env.E2_T6_DISPOSABLE_DATABASE_URL;
  assert(databaseUrl && /^postgres(?:ql)?:\/\//.test(databaseUrl), 'disposable database URL is required');
  const root = path.join(__dirname, '..');
  psql(databaseUrl, path.join(root, 'tests/fixtures/e2-t6-disposable-schema.sql'));
  const output = psql(databaseUrl, path.join(root, 'docs/security/sql/E2_T6_RLS_V3_DISPOSABLE_TRANSACTION.sql')).trim();
  const evidence = buildEvidence(JSON.parse(output));
  assert.equal(evidence.status, 'PASS');
  process.stdout.write(`${JSON.stringify({ operation: evidence.operation_code, status: evidence.status, cases: evidence.passed_case_count, rollback: evidence.rollback_required })}\n`);
}

if (require.main === module) {
  try { main(); } catch (_) { process.stderr.write('E2-T6 disposable reproduction failed\n'); process.exitCode = 1; }
}
