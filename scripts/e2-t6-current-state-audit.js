'use strict';
const path = require('node:path');
const { createManagementClient } = require('../operator/management-api');
const currentStateAudit = require('../operator/e2-t6-current-state-audit');
async function main() {
  const args = process.argv.slice(2);
  if (args.length !== 2 || args[0] !== '--confirm') throw new Error('Invalid current-state audit arguments');
  const repo = path.join(__dirname, '..');
  const client = createManagementClient({ token: process.env.SUPABASE_ACCESS_TOKEN, projectRef: process.env.SUPABASE_PROJECT_REF });
  const report = await currentStateAudit.audit({ repo, client, confirmation: args[1] });
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
}
if (require.main === module) main().catch((error) => {
  process.stderr.write(`${JSON.stringify({ operation: 'e2_t6_current_state_audit_v1', status: 'AUDIT_FAIL_CLOSED', safeCode: error && error.safeCode || 'AUDIT_PREREQUISITE_FAILED' })}\n`);
  process.exitCode = 1;
});
module.exports = Object.freeze({ main });
