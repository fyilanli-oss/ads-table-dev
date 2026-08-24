'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { canonicalUniqueKey } = require('../funnel-core/dataset-repository');
const { validateCanonicalRow } = require('../funnel-core/canonical-contract');
const { validateEntityHierarchy } = require('../funnel-core/entity-hierarchy');

const BLOCKED = /postgres(?:ql)?:\/\/|authorization\s*:|bearer\s+|-----BEGIN [A-Z ]*PRIVATE KEY-----|\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b|[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}|[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}/i;
const ALLOWED = Object.freeze([
  'initial_operation_count','upsert_operation_count','fixture_row_count','duplicate_group_count',
  'duplicate_excess_row_count','updated_contract_match_count','dataset_before','v1_before','v1_after',
  'snapshot_before','snapshot_after','identity_unchanged','hierarchy_unchanged','unsupported_null_preserved',
  'supported_zero_preserved','oauth_unchanged','connected_unchanged','encrypted_unchanged','plaintext_unchanged'
]);
const BOOLEANS = Object.freeze([
  'identity_unchanged','hierarchy_unchanged','unsupported_null_preserved','supported_zero_preserved',
  'oauth_unchanged','connected_unchanged','encrypted_unchanged','plaintext_unchanged'
]);

function assert(condition, message) { if (!condition) throw new Error(message); }
function sameKeys(object, keys) { return JSON.stringify(Object.keys(object).sort()) === JSON.stringify([...keys].sort()); }

function buildEvidence(result, initial, updated) {
  assert(result && typeof result === 'object' && !Array.isArray(result), 'result must be one redacted object');
  assert(!BLOCKED.test(JSON.stringify(result)), 'forbidden identity or credential material');
  assert(sameKeys(result, ALLOWED), 'result fields do not match allowlist');
  validateCanonicalRow(initial); validateEntityHierarchy(initial.identity, initial.entity);
  validateCanonicalRow(updated); validateEntityHierarchy(updated.identity, updated.entity);
  assert(canonicalUniqueKey(initial) === canonicalUniqueKey(updated), 'fixture canonical keys differ');
  assert(JSON.stringify(initial.identity) === JSON.stringify(updated.identity), 'fixture identity differs');
  assert(JSON.stringify(initial.entity) === JSON.stringify(updated.entity), 'fixture hierarchy differs');
  for (const key of ['initial_operation_count','upsert_operation_count','fixture_row_count','updated_contract_match_count']) assert(result[key] === 1, `${key} must equal one`);
  for (const key of ['duplicate_group_count','duplicate_excess_row_count']) assert(result[key] === 0, `${key} must equal zero`);
  for (const key of BOOLEANS) assert(result[key] === true, `${key} must be true`);
  assert(result.v1_before === result.v1_after, 'V1 count parity failed');
  assert(result.snapshot_before === result.snapshot_after, 'snapshot count parity failed');
  assert(initial.raw_metrics.session === null && updated.raw_metrics.session === null, 'unsupported null drift');
  assert(initial.raw_metrics.add_to_cart === 0 && updated.raw_metrics.add_to_cart === 0, 'supported zero drift');
  return Object.freeze({
    evidence_version:'e2-t4-upsert-v1',namespace:'e2_t4_same_key_v1',status:'PASS',
    initial_operation_count:1,upsert_operation_count:1,final_fixture_row_count:1,
    duplicate_group_count:0,duplicate_excess_row_count:0,
    canonical_key_unchanged:true,identity_unchanged:true,hierarchy_unchanged:true,
    unsupported_null_preserved:true,supported_zero_preserved:true,v1_unchanged:true,snapshot_unchanged:true,
    rollback_required:true
  });
}

function main(argv) {
  assert(argv.length === 1, 'usage: node scripts/e2-t4-upsert-evidence.js <redacted-result.json>');
  const dir = path.join(__dirname,'../artifacts/dataset-v2-acceptance/e2-t4-upsert');
  const result=JSON.parse(fs.readFileSync(path.resolve(argv[0]),'utf8'));
  const initial=JSON.parse(fs.readFileSync(path.join(dir,'expected-initial-canonical.json'),'utf8'));
  const updated=JSON.parse(fs.readFileSync(path.join(dir,'expected-updated-canonical.json'),'utf8'));
  process.stdout.write(`${JSON.stringify(buildEvidence(result,initial,updated),null,2)}\n`);
}
if(require.main===module){try{main(process.argv.slice(2));}catch(error){process.stderr.write(`${error.message}\n`);process.exitCode=1;}}
module.exports=Object.freeze({ALLOWED,BOOLEANS,buildEvidence});
