'use strict';

const CONFIRMATION = 'E2_T8_SCHEMA_ONLY_CAPTURE';
const FIXED_ARGUMENTS = Object.freeze(['--schema-only', '--schema=public', '--no-owner', '--no-privileges']);

function parseArguments(argv) {
  const allowed = new Set(['--execute', '--confirm', CONFIRMATION]);
  if (argv.some((value) => /(?:\/\/|password|uri|table|data|extra)/i.test(value))) throw new Error('FORBIDDEN_ARGUMENT');
  for (const value of argv) if (!allowed.has(value)) throw new Error('UNKNOWN_ARGUMENT');
  const execute = argv.includes('--execute');
  const confirmationIndex = argv.indexOf('--confirm');
  const confirmed = confirmationIndex >= 0 && argv[confirmationIndex + 1] === CONFIRMATION;
  if (execute && !confirmed) throw new Error('EXPLICIT_CONFIRMATION_REQUIRED');
  return { execute, confirmed };
}

function capturePlan() {
  return Object.freeze({ contract_version: 'e2-t8-schema-capture-v1', mode: 'schema-only', schema: 'public', row_data: false, restore_owner: false, deterministic: true, arguments: FIXED_ARGUMENTS });
}

function run(argv = [], dependencies = {}) {
  const options = parseArguments(argv);
  const plan = capturePlan();
  if (!options.execute) return { status: 'PLAN_ONLY', plan };
  if (!process.env.E2_T8_SOURCE_DATABASE_URL) return { status: 'SOURCE_CREDENTIAL_UNAVAILABLE', plan };
  if (typeof dependencies.toolAvailable !== 'function' || !dependencies.toolAvailable('pg_dump')) return { status: 'CAPTURE_TOOL_UNAVAILABLE', plan };
  if (typeof dependencies.spawn !== 'function') return { status: 'CAPTURE_TOOL_UNAVAILABLE', plan };
  const result = dependencies.spawn('pg_dump', FIXED_ARGUMENTS, { env: { ...process.env }, stdio: ['ignore', 'pipe', 'pipe'] });
  return { status: 'CAPTURE_REQUIRES_VALIDATION', exitCode: result.status, plan };
}

module.exports = { CONFIRMATION, FIXED_ARGUMENTS, parseArguments, capturePlan, run };

if (require.main === module) {
  try { process.stdout.write(`${JSON.stringify(run(process.argv.slice(2)))}\n`); }
  catch (error) { process.stderr.write(`${error.message}\n`); process.exitCode = 1; }
}
