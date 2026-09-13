'use strict';
const path = require('node:path');
const os = require('node:os');
const { createManagementClient } = require('../operator/management-api');
const operation = require('../operator/e2-t6-v4');
function parseArgs(argv) {
  if (argv.length === 1 && argv[0] === 'preflight') return Object.freeze({ action:'preflight', confirmation:undefined });
  if (argv.length === 3 && argv[0] === 'execute' && argv[1] === '--confirm') return Object.freeze({ action:'execute', confirmation:argv[2] });
  throw new Error('Invalid operator arguments');
}
async function main() {
  const { action, confirmation } = parseArgs(process.argv.slice(2));
  const client = createManagementClient({ token:process.env.SUPABASE_ACCESS_TOKEN, projectRef:process.env.SUPABASE_PROJECT_REF });
  const stateFile = process.env.ADS_TABLE_OPERATOR_STATE
    || path.join(os.homedir(), '.local', 'state', 'ads-table', 'e2-t6-v4.json');
  const options = { repo:path.join(__dirname, '..'), stateFile, client, confirmation };
  const report = action === 'preflight' ? await operation.preflight(options) : await operation.execute(options);
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
}
if (require.main === module) main().catch((error) => { const safeCode = error && typeof error.safeCode === 'string' ? error.safeCode : 'STOPPED_FAIL_CLOSED'; process.stderr.write(`${JSON.stringify({ operation:'e2_t6_rls_v4', status:'FAIL', safeCode })}\n`); process.exitCode = 1; });
module.exports = Object.freeze({ main, parseArgs });
