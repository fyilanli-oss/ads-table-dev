'use strict';
const os = require('node:os');
const path = require('node:path');
const { createManagementClient } = require('../operator/management-api');
const diagnostic = require('../operator/e2-t6-v3-postcheck-diagnostic');
async function main() {
  const args = process.argv.slice(2);
  if (args.length !== 3 || args[0] !== 'diagnose' || args[1] !== '--confirm') throw new Error('Invalid diagnostic arguments');
  const repo = path.join(__dirname, '..');
  const stateFile = process.env.ADS_TABLE_OPERATOR_STATE || path.join(os.homedir(), '.local', 'state', 'ads-table', 'e2-t6-v3.json');
  const client = createManagementClient({ token: process.env.SUPABASE_ACCESS_TOKEN, projectRef: process.env.SUPABASE_PROJECT_REF });
  const report = await diagnostic.diagnose({ repo, stateFile, client, confirmation: args[2] });
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
}
if (require.main === module) main().catch((error) => { process.stderr.write(`${JSON.stringify({ operation: 'e2_t6_rls_v3', status: 'DIAGNOSTIC_FAIL_CLOSED', safeCode: error && error.safeCode || 'DIAGNOSTIC_PREREQUISITE_FAILED' })}\n`); process.exitCode = 1; });
module.exports = Object.freeze({ main });
