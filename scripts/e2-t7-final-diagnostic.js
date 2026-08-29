#!/usr/bin/env node
'use strict';
const path = require('node:path');
const { createClient } = require('../operator/management-api');
const op = require('../operator/e2-t7-final-diagnostic');
const repo = path.join(__dirname, '..');
const args = process.argv.slice(2);
if (args.length !== 2 || args[0] !== '--confirm') throw new Error('Usage: --confirm E2-T7-FINAL-DIAGNOSTIC');
op.diagnose({ repo, stateFile: path.join(process.env.HOME, '.local/state/ads-table/e2-t7-v2.json'), client: createClient(), confirmation: args[1] })
  .then(value => process.stdout.write(`${JSON.stringify(value)}\n`))
  .catch(error => { process.stderr.write(`${JSON.stringify({ operation: 'e2_t7_final_diagnostic_v1', status: 'FAIL_CLOSED', safeCode: error.safeCode || 'LOCAL_GUARD_FAILED' })}\n`); process.exitCode = 1; });
