'use strict';
const path = require('node:path');
const os = require('node:os');
const { createManagementClient } = require('../operator/management-api');
const operation = require('../operator/e2-t6-v1');
function parseArgs(argv) {
  if (argv.length === 1 && argv[0] === 'preflight') return Object.freeze({ action:'preflight', confirmation:undefined });
  if (argv.length === 3 && argv[0] === 'execute' && argv[1] === '--confirm') return Object.freeze({ action:'execute', confirmation:argv[2] });
  throw new Error('Invalid operator arguments');
}
async function main() {
  const { action, confirmation } = parseArgs(process.argv.slice(2));
  const client = createManagementClient({ token:process.env.SUPABASE_ACCESS_TOKEN, projectRef:process.env.SUPABASE_PROJECT_REF });
  const stateFile = process.env.ADS_TABLE_OPERATOR_STATE
    || path.join(os.homedir(), '.local', 'state', 'ads-table', 'e2-t6-v1.json');
  const options = { repo:path.join(__dirname, '..'), stateFile, client, confirmation };
  const report = action === 'preflight' ? await operation.preflight(options) : await operation.execute(options);
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
}
if (require.main === module) main().catch(() => { process.stderr.write('E2-T6 V1 operator stopped safely; no sensitive details were emitted.\n'); process.exitCode = 1; });
module.exports = Object.freeze({ main, parseArgs });
