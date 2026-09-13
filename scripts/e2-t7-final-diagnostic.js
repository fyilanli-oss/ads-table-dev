#!/usr/bin/env node
'use strict';

const os = require('node:os');
const path = require('node:path');
const { createManagementClient } = require('../operator/management-api');
const op = require('../operator/e2-t7-final-diagnostic');

function parseArgs(args) {
  if (args.length !== 2 || args[0] !== '--confirm') throw new Error('Invalid diagnostic arguments');
  return Object.freeze({ confirmation: args[1] });
}

async function main() {
  const { confirmation } = parseArgs(process.argv.slice(2));
  const repo = path.join(__dirname, '..');
  const stateFile = process.env.ADS_TABLE_OPERATOR_STATE || path.join(os.homedir(), '.local', 'state', 'ads-table', 'e2-t7-v2.json');
  const client = createManagementClient({ token: process.env.SUPABASE_ACCESS_TOKEN, projectRef: process.env.SUPABASE_PROJECT_REF });
  const report = await op.diagnose({ repo, stateFile, client, confirmation });
  process.stdout.write(`${JSON.stringify(report)}\n`);
}

if (require.main === module) main().catch(error => {
  process.stderr.write(`${JSON.stringify({ operation: 'e2_t7_final_diagnostic_v1', status: 'FAIL_CLOSED', safeCode: error && error.safeCode || 'LOCAL_GUARD_FAILED' })}\n`);
  process.exitCode = 1;
});

module.exports = Object.freeze({ main, parseArgs });
