'use strict';
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const root = path.join(__dirname, '..');
const read = (file) => fs.readFileSync(path.join(root, file), 'utf8');
const json = (file) => JSON.parse(read(file));
const contract = require('../security/e2-t8-restore-contract');
const capture = require('../scripts/e2-t8-schema-capture');
const evidence = require('../scripts/e2-t8-restore-evidence');
const base = 'artifacts/dataset-v2-acceptance/e2-t8-restore/';
const scope = json(`${base}restore-scope.json`); const classification = json(`${base}migration-classification.json`); const manifest = json(`${base}restore-artifact-manifest.template.json`);
const required = [`${base}restore-scope.json`,`${base}migration-classification.json`,`${base}restore-artifact-manifest.template.json`,'docs/security/E2_T8_RESTORE_READINESS_RUNBOOK.md','docs/security/sql/E2_T8_SOURCE_INVENTORY.sql','docs/security/sql/E2_T8_TARGET_PREFLIGHT.sql','docs/security/sql/E2_T8_TARGET_ACCEPTANCE.sql','security/e2-t8-restore-contract.js','scripts/e2-t8-schema-capture.js','scripts/e2-t8-restore-evidence.js'];

test('all restore preparation artifacts exist and JSON allowlists are exact', () => {
  for (const file of required) assert.ok(fs.existsSync(path.join(root,file)), file);
  assert.deepEqual(Object.keys(scope), contract.SCOPE_KEYS); assert.deepEqual(Object.keys(manifest), contract.MANIFEST_KEYS);
  assert.equal(manifest.restore_ready,false); assert.equal(manifest.baseline_cutoff,null); assert.equal(manifest.provenance,'CURRENT_STATE_BASELINE');
});

test('six migrations have exact filenames and repository SHA-256 values without historical SQL', () => {
  assert.equal(classification.migrations.length,6);
  for (const m of classification.migrations) {
    assert.deepEqual(Object.keys(m),contract.MIGRATION_KEYS);
    assert.equal(crypto.createHash('sha256').update(read(`supabase/migrations/${m.filename}`)).digest('hex'),m.sha256);
    assert.equal(m.candidate_classification,'absorbed_by_current_state_baseline'); assert.equal(m.final_classification_status,'pending_capture_checksum'); assert.equal(m.replay_allowed,false);
  }
  assert.equal(classification.historical_sql_available,false); assert.doesNotMatch(JSON.stringify(classification),/historical_sql_body|create table.*202606/i);
});

test('target contract forbids generic PostgreSQL and requires exact managed primitives', () => {
  assert.equal(scope.target.plain_generic_postgresql_target,false); assert.equal(scope.target.required_managed_primitives.length,5);
  const good={kind:'disposable_supabase',auth_schema:true,auth_uid_exact_signature:true,anon_role:true,authenticated_role:true,service_role_role:true,production:false,shared:false,empty:true,production_ref_match:false};
  assert.equal(contract.validateTarget(good).status,'PASS');
  assert.equal(contract.validateTarget({...good,kind:'postgresql'}).status,'FAIL'); assert.equal(contract.validateTarget({...good,auth_schema:false}).status,'FAIL');
  assert.equal(contract.validateTarget({...good,production:true}).status,'FAIL'); assert.equal(contract.validateTarget({...good,shared:true}).status,'FAIL'); assert.equal(contract.validateTarget({...good,empty:false}).status,'FAIL');
});

test('capture is side-effect-free, plan-only by default and uses a fixed no-data/no-owner plan', () => {
  let calls=0; assert.equal(capture.run([], {spawn(){calls++;}}).status,'PLAN_ONLY'); assert.equal(calls,0);
  const plan=capture.capturePlan(); assert.equal(plan.schema,'public'); assert.equal(plan.row_data,false); assert.equal(plan.restore_owner,false); assert.deepEqual(plan.arguments,['--schema-only','--schema=public','--no-owner','--no-privileges']);
  assert.throws(()=>capture.run(['--execute']),/CONFIRMATION/); assert.throws(()=>capture.run(['--execute','--confirm',capture.CONFIRMATION,'--table=x']),/FORBIDDEN|UNKNOWN/); assert.throws(()=>capture.run(['postgresql://host/db']),/FORBIDDEN/);
});

test('execute path is dependency-injected and safely reports unavailable tool', () => {
  const old=process.env.E2_T8_SOURCE_DATABASE_URL; process.env.E2_T8_SOURCE_DATABASE_URL='in-memory-test-placeholder'; let calls=0;
  try { assert.equal(capture.run(['--execute','--confirm',capture.CONFIRMATION],{toolAvailable:()=>false,spawn(){calls++;}}).status,'CAPTURE_TOOL_UNAVAILABLE'); assert.equal(calls,0); }
  finally { if(old===undefined) delete process.env.E2_T8_SOURCE_DATABASE_URL; else process.env.E2_T8_SOURCE_DATABASE_URL=old; }
});

for (const file of ['docs/security/sql/E2_T8_SOURCE_INVENTORY.sql','docs/security/sql/E2_T8_TARGET_PREFLIGHT.sql','docs/security/sql/E2_T8_TARGET_ACCEPTANCE.sql']) test(`${file} is one read-only WITH statement`,()=>{
  const sql=read(file).replace(/^\s*--.*$/gm,'').trim(); assert.match(sql,/^WITH\b/i); assert.equal(sql.split(';').filter(x=>x.trim()).length,1);
  assert.doesNotMatch(sql.replace(/'(?:''|[^'])*'/g,"''"),/(?:^|[;(])\s*(?:insert|update|delete|alter|create|drop|grant|revoke|copy|call|do|truncate)\b/i);
  assert.doesNotMatch(sql,/\bFROM\s+(?:auth|storage)\./i);
});

test('source and target acceptance use deterministic normalized fingerprints without raw bodies',()=>{ const a=read('docs/security/sql/E2_T8_SOURCE_INVENTORY.sql'); const b=read('docs/security/sql/E2_T8_TARGET_ACCEPTANCE.sql'); for(const value of ['column:','constraint:','index:','function:','trigger:','policy:','grant:','md5']) { assert.match(a,new RegExp(value)); assert.match(b,new RegExp(value)); } assert.doesNotMatch(a,/SELECT\s+prosrc\b/i); });

test('preparation validator passes contract but never declares restore safety',()=>{ const result=contract.validatePreparation(scope,classification,manifest); assert.deepEqual(result,{status:'PASS',errors:[],restoreSafeDecision:false}); const ready={...manifest,restore_ready:true}; assert.equal(contract.validatePreparation(scope,classification,ready).status,'FAIL'); });

test('validator rejects checksums and sensitive material',()=>{ const bad=structuredClone(classification); bad.migrations[0].sha256='0'.repeat(64); assert.equal(contract.validatePreparation(scope,bad,manifest).status,'FAIL'); const secret={...manifest,capture_tool_version:'postgresql://host/db'}; assert.equal(contract.validatePreparation(scope,classification,secret).status,'FAIL'); });

test('evidence rejects pending cutoff/classification, missing extra duplicate and sensitive inputs',()=>{
  const row={object_key:'relation:a',object_class:'relation',fingerprint:'abc'}; const pre={managed_primitives_ok:true,target_kind_ok:true,target_identity_distinct:true,public_allowlist_only:true,application_relation_count:0,ledger_unambiguous:true,passed:true};
  assert.equal(evidence.convert({sourceInventory:[row],targetPreflight:pre,targetInventory:[row],manifest,migrationClassification:classification}).status,'FAIL');
  const finalManifest={...manifest,baseline_cutoff:'20260824120000',restore_ready:true}; const finalClass=structuredClone(classification); finalClass.migrations.forEach(m=>{m.final_classification_status='final';});
  assert.equal(evidence.convert({sourceInventory:[row],targetPreflight:pre,targetInventory:[row],manifest:finalManifest,migrationClassification:finalClass}).status,'PASS');
  for(const target of [[],[row,row],[row,{object_key:'relation:b',object_class:'relation',fingerprint:'def'}]]) assert.equal(evidence.convert({sourceInventory:[row],targetPreflight:pre,targetInventory:target,manifest:finalManifest,migrationClassification:finalClass}).status,'FAIL');
  assert.equal(evidence.convert({sourceInventory:[row],targetPreflight:{...pre,managed_primitives_ok:false},targetInventory:[row],manifest:finalManifest,migrationClassification:finalClass}).status,'FAIL');
  assert.equal(evidence.convert({sourceInventory:[{...row,fingerprint:'postgresql://host/db'}],targetPreflight:pre,targetInventory:[row],manifest:finalManifest,migrationClassification:finalClass}).status,'FAIL');
});

test('execution plan keeps E2-T8 in Verification and records no live operation',()=>{ const plan=read('codex-input/AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md'); assert.match(plan,/E2-T8 task aynası/); assert.match(plan,/\*\*Durum:\*\* `Verification`/); for(const value of ['Actual schema capture yapılmadı','baseline SQL üretilmedi','target provision edilmedi','restore çalıştırılmadı','production değişmedi']) assert.match(plan,new RegExp(value,'i')); });
