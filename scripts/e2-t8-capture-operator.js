#!/usr/bin/env node
'use strict';
const os = require('node:os');
const path = require('node:path');
const { createManagementClient } = require('../operator/management-api');
const op = require('../operator/e2-t8-capture');
function parseArgs(args) {
  if (args.length === 3 && args[0] === 'preflight' && args[1] === '--confirm') return { action: 'preflight', confirmation: args[2] };
  if (args.length === 3 && args[0] === 'execute' && args[1] === '--confirm') return { action: 'execute', confirmation: args[2] };
  throw new Error('Invalid E2-T8 capture arguments');
}
async function main() {
  const args = parseArgs(process.argv.slice(2)), repo = path.join(__dirname, '..');
  const stateFile = process.env.ADS_TABLE_OPERATOR_STATE || path.join(os.homedir(), '.local', 'state', 'ads-table', 'e2-t8-capture-v1.json');
  const report = args.action === 'preflight'
    ? await op.preflight({ repo, stateFile, confirmation: args.confirmation, client: createManagementClient({ token: process.env.SUPABASE_ACCESS_TOKEN, projectRef: process.env.SUPABASE_PROJECT_REF }) })
    : op.execute({ repo, stateFile, confirmation: args.confirmation });
  process.stdout.write(`${JSON.stringify(report)}\n`);
}
if (require.main === module) main().catch(error => { process.stderr.write(`${JSON.stringify({ operation: op.OPERATION, status: 'FAIL_CLOSED', safeCode: error && error.safeCode || 'PREREQUISITE_FAILED' })}\n`); process.exitCode = 1; });
module.exports = Object.freeze({ main, parseArgs });
