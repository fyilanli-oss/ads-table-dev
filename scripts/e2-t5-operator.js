'use strict';
const path = require('node:path');
const { createManagementClient } = require('../operator/management-api');
const { defaultStatePath } = require('../operator/state-store');
const operation = require('../operator/e2-t5-v2');
function argument(name) { const index = process.argv.indexOf(name); return index < 0 ? undefined : process.argv[index + 1]; }
async function main() {
  const action = process.argv[2]; if (!['preflight','execute'].includes(action)) throw new Error('Unknown operator action');
  const client = createManagementClient({ token:process.env.SUPABASE_ACCESS_TOKEN, projectRef:process.env.SUPABASE_PROJECT_REF });
  const options = { repo:path.join(__dirname, '..'), stateFile:defaultStatePath(), client, confirmation:argument('--confirm') };
  const report = action === 'preflight' ? await operation.preflight(options) : await operation.execute(options);
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
}
main().catch(() => { process.stderr.write('E2-T5 V2 operator stopped safely; no sensitive details were emitted.\n'); process.exitCode = 1; });
