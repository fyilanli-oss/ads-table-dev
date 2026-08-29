'use strict';
const assert=require('node:assert/strict'),crypto=require('node:crypto'),fs=require('node:fs'),path=require('node:path'),test=require('node:test');
const root=path.join(__dirname,'..'),read=f=>fs.readFileSync(path.join(root,f),'utf8'),json=f=>JSON.parse(read(f));
const contract=require('../security/e2-t8-restore-contract'),validator=require('../security/e2-t8-captured-schema-validator'),capture=require('../scripts/e2-t8-schema-capture'),evidence=require('../scripts/e2-t8-restore-evidence');
const base='artifacts/dataset-v2-acceptance/e2-t8-restore/',scope=json(base+'restore-scope.json'),classification=json(base+'migration-classification.json'),manifest=json(base+'restore-artifact-manifest.template.json'),FP='a'.repeat(64);
const attempts=json(base+'source-inventory-attempts.json');
const inventory=[{object_key:'relation:accounts',object_class:'relation',ownership_class:'application_owned',fingerprint:FP}],grants={grantees:['anon','authenticated','service_role'],privileges:['SELECT','INSERT','UPDATE','DELETE']};
const validSql='CREATE TABLE public.accounts (id bigint);\nGRANT SELECT ON TABLE public.accounts TO authenticated;';
const validationInputs={approvedSourceInventory:inventory,restoreScope:scope,expectedGrantContract:grants,captureManifest:manifest};
const preflight={managed_primitives_ok:true,target_kind_ok:true,target_identity_distinct:true,public_allowlist_only:true,application_relation_count:0,ledger_unambiguous:true,passed:true};
const gates={managed_primitives_ok:true,source_inventory_sha256:crypto.createHash('sha256').update(JSON.stringify(inventory)).digest('hex'),application_table_counts:[{table_name:'accounts',row_count:0}],allowlist_complete:true};

test('artifacts and exact JSON contracts exist',()=>{for(const f of [base+'restore-scope.json',base+'migration-classification.json',base+'restore-artifact-manifest.template.json','docs/security/E2_T8_RESTORE_READINESS_RUNBOOK.md','docs/security/sql/E2_T8_SOURCE_INVENTORY.sql','docs/security/sql/E2_T8_TARGET_PREFLIGHT.sql','docs/security/sql/E2_T8_TARGET_ACCEPTANCE.sql','docs/security/sql/E2_T8_TARGET_FINAL_GATES.sql','security/e2-t8-restore-contract.js','security/e2-t8-captured-schema-validator.js','scripts/e2-t8-schema-capture.js','scripts/e2-t8-restore-evidence.js'])assert.ok(fs.existsSync(path.join(root,f)),f);assert.deepEqual(Object.keys(scope),contract.SCOPE_KEYS);assert.deepEqual(Object.keys(manifest),contract.MANIFEST_KEYS);assert.equal(manifest.restore_ready,false);});
test('source inventory attempts are redacted, no-change, and no-retry evidence',()=>{assert.deepEqual(attempts.attempts.map(x=>x.safe_code),['SOURCE_INVENTORY_QUERY_FAILED','SOURCE_INVENTORY_MANAGEMENT_TRANSPORT_FAILED','SOURCE_INVENTORY_CONTRACT_FAILED']);assert.deepEqual(attempts.attempts.map(x=>x.request_count),[1,1,1]);assert.ok(attempts.attempts.every(x=>x.state_capsule_created===false));assert.equal(attempts.schema_capture_requests,0);assert.equal(attempts.automatic_retries,0);assert.equal(attempts.production_mutations,0);assert.doesNotMatch(JSON.stringify(attempts),/https?:|project.ref|authorization|bearer|password|token/i);});
test('six checksummed migrations remain structured, pending and replay-disabled',()=>{assert.equal(classification.migrations.length,6);for(const m of classification.migrations){assert.equal(crypto.createHash('sha256').update(read('supabase/migrations/'+m.filename)).digest('hex'),m.sha256);assert.equal(m.final_classification_status,'pending_capture_checksum');assert.equal(m.replay_allowed,false);for(const o of m.objects){assert.deepEqual(Object.keys(o),contract.OBJECT_KEYS);assert.equal(o.migration_version,m.version);assert.equal(o.pending_capture_checksum,true);}}assert.equal(classification.historical_sql_available,false);});
test('overlap chains explicitly relate create and corrective state',()=>{const all=classification.migrations.flatMap(m=>m.objects);assert.ok(all.some(o=>o.overlap_group==='oauth_transactions'&&o.effect==='create'));assert.ok(all.some(o=>o.overlap_group==='oauth_transactions'&&o.effect==='harden'));});
test('capture defaults to plan only and preserves grants with no owner',()=>{let calls=0;assert.equal(capture.run([],{spawnSync(){calls++;}}).status,'PLAN_ONLY');assert.equal(calls,0);assert.ok(capture.FIXED_ARGUMENTS.includes('--no-owner'));assert.ok(!capture.FIXED_ARGUMENTS.includes('--no-privileges'));assert.equal(capture.capturePlan().deterministic,false);assert.throws(()=>capture.run(['--execute']),/CONFIRMATION/);assert.throws(()=>capture.run(['postgresql://host/db']),/FORBIDDEN/);});
test('standalone dependencies exist and child env is credential-minimal',()=>{const d=capture.defaultDependencies();assert.equal(typeof d.spawnSync,'function');assert.equal(typeof d.write,'function');const env=capture.childEnvironment('secret-uri',{PATH:'/bin',HOME:'/home',PGSSLMODE:'verify-full',AUTHORIZATION:'no',VERCEL_TOKEN:'no'});assert.deepEqual(env,{PATH:'/bin',PGDATABASE:'secret-uri',HOME:'/home',PGSSLMODE:'verify-full'});assert.doesNotMatch(JSON.stringify(capture.capturePlan()),/secret-uri/);});
test('execute rejects repository output, nonzero exit, and withholds acceptance before validation',()=>{const common={env:{PATH:'/bin',E2_T8_SOURCE_DATABASE_URL:'secret-uri',E2_T8_CAPTURE_QUARANTINE_DIR:'/repo/raw'},repoRoot:'/repo',resolve:p=>p,mkdir(){},write(){throw Error('no');},spawnSync(){throw Error('no');}};assert.equal(capture.run(['--execute','--confirm',capture.CONFIRMATION],common).status,'REPOSITORY_OUTPUT_FORBIDDEN');let n=0;const fail={...common,env:{...common.env,E2_T8_CAPTURE_QUARANTINE_DIR:'/quarantine'},spawnSync(){n++;return n===1?{status:0,stdout:'17'}:{status:2,stderr:'raw secret'};},write(){}};assert.equal(capture.run(['--execute','--confirm',capture.CONFIRMATION],fail).status,'CAPTURE_COMMAND_FAILED');assert.doesNotMatch(JSON.stringify(capture.run(['--execute','--confirm',capture.CONFIRMATION],fail)),/raw secret|secret-uri/);});
test('fake capture quarantines output and requires validator contract',()=>{let n=0,writes=0;const d={env:{PATH:'/bin',E2_T8_SOURCE_DATABASE_URL:'secret-uri',E2_T8_CAPTURE_QUARANTINE_DIR:'/q'},repoRoot:'/repo',resolve:p=>p,mkdir(){},write(p,data){writes++;assert.equal(data,validSql);},now:()=>1,pid:2,spawnSync(cmd,args,opts){n++;assert.equal(cmd,'pg_dump');assert.ok(!args.includes('secret-uri'));assert.deepEqual(Object.keys(opts.env).sort(),['PATH','PGDATABASE']);return n===1?{status:0,stdout:'pg_dump 17'}:{status:0,stdout:validSql};}};assert.equal(capture.run(['--execute','--confirm',capture.CONFIRMATION],d).status,'CAPTURE_QUARANTINED_VALIDATION_REQUIRED');n=0;assert.equal(capture.run(['--execute','--confirm',capture.CONFIRMATION],{...d,validationInputs}).status,'CAPTURE_QUARANTINED_CONTRACT_PASS');assert.equal(writes,2);});
test('captured SQL tokenizer handles dollar bodies, comments and strings',()=>{const sql="-- INSERT hidden\nCREATE FUNCTION public.f() RETURNS void LANGUAGE plpgsql AS $$ BEGIN INSERT INTO x VALUES (';'); END $$;";const out=validator.validateCapturedSchema({capturedSql:sql,approvedSourceInventory:[{object_key:'function:f()',object_class:'function',ownership_class:'application_owned',fingerprint:FP}],restoreScope:scope,expectedGrantContract:grants,captureManifest:manifest});assert.equal(out.status,'ARTIFACT_CONTRACT_PASS');assert.equal(validator.lex("SELECT ';not split'; SELECT 2;").length,2);});
test('validator preserves expected table grants and rejects dangerous SQL',()=>{assert.equal(validator.validateCapturedSchema({capturedSql:validSql,...validationInputs}).status,'ARTIFACT_CONTRACT_PASS');for(const sql of ['','CREATE TABLE public.accounts(id int); GRANT SELECT ON TABLE public.accounts TO stranger;','CREATE ROLE x;','ALTER ROLE x;','ALTER TABLE public.accounts OWNER TO x;','GRANT USAGE ON SCHEMA public TO anon;','INSERT INTO public.accounts VALUES (1);','COPY public.accounts FROM stdin;','CREATE TABLE auth.bad(id int);','CREATE DATABASE bad;','\\include /tmp/x'])assert.equal(validator.validateCapturedSchema({capturedSql:sql,...validationInputs}).status,'ARTIFACT_CONTRACT_FAIL',sql);});
test('normalized SQL checksum is stable and semantic changes differ',()=>{const a=validator.validateCapturedSchema({capturedSql:validSql,...validationInputs}),b=validator.validateCapturedSchema({capturedSql:validSql,...validationInputs}),c=validator.validateCapturedSchema({capturedSql:validSql.replace('bigint','text'),...validationInputs});assert.equal(a.checksum,b.checksum);assert.notEqual(a.checksum,c.checksum);assert.match(a.checksum,/^[0-9a-f]{64}$/);assert.equal(a.restoreSafeDecision,false);});
for(const f of ['docs/security/sql/E2_T8_SOURCE_INVENTORY.sql','docs/security/sql/E2_T8_TARGET_PREFLIGHT.sql','docs/security/sql/E2_T8_TARGET_ACCEPTANCE.sql','docs/security/sql/E2_T8_TARGET_FINAL_GATES.sql'])test(f+' is one read-only WITH statement',()=>{const s=read(f).replace(/^\s*--.*$/gm,'').trim();assert.match(s,/^WITH\b/i);assert.equal(s.split(';').filter(Boolean).length,1);assert.doesNotMatch(s.replace(/'(?:''|[^'])*'/g,"''"),/(?:^|[;(])\s*(?:insert|update|delete|alter|create|drop|grant|revoke|copy|call|do|truncate)\b/i);});
test('source and target inventories are identical SHA-256 contracts',()=>{const s=read('docs/security/sql/E2_T8_SOURCE_INVENTORY.sql'),t=read('docs/security/sql/E2_T8_TARGET_ACCEPTANCE.sql');assert.equal(s,t);assert.doesNotMatch(s,/md5/i);assert.match(s,/digest\(convert_to\(normalized,'UTF8'\),'sha256'\)/);});
test('target preflight kind is operator-local and final rows are exact',()=>{const p=read('docs/security/sql/E2_T8_TARGET_PREFLIGHT.sql'),g=read('docs/security/sql/E2_T8_TARGET_FINAL_GATES.sql');assert.doesNotMatch(p,/true AS target_kind_ok/i);assert.match(p,/current_setting\('e2_t8.target_kind',true\)/);assert.match(p,/disposable_supabase/);assert.match(p,/official_full_local_supabase/);assert.doesNotMatch(g,/pg_stat|n_live_tup/i);assert.match(g,/count\(\*\)/i);assert.match(g,/%I/);});
test('evidence shape accepts exact target result and rejects MD5, rows, missing and extra objects',()=>{const finalManifest={...manifest,baseline_cutoff:'cutoff',restore_ready:true},finalClass=structuredClone(classification);finalClass.migrations.forEach(m=>m.final_classification_status='final');const input={sourceInventory:inventory,targetPreflight:preflight,targetInventory:inventory,targetFinalGates:gates,manifest:finalManifest,migrationClassification:finalClass};assert.equal(evidence.convert(input).status,'PASS');assert.equal(evidence.convert({...input,targetFinalGates:{...gates,application_table_counts:[{table_name:'accounts',row_count:1}]}}).status,'FAIL');assert.equal(evidence.convert({...input,targetInventory:[{...inventory[0],fingerprint:'a'.repeat(32)}]}).status,'FAIL');assert.equal(evidence.convert({...input,targetInventory:[]}).status,'FAIL');assert.equal(evidence.convert({...input,targetInventory:[...inventory,{object_key:'relation:extra',object_class:'relation',ownership_class:'application_owned',fingerprint:FP}]}).status,'FAIL');assert.equal(evidence.convert({...input,targetInventory:[...inventory,...inventory]}).status,'FAIL');});
test('pending cutoff and classification remain fail closed',()=>{assert.equal(contract.validatePreparation(scope,classification,manifest).status,'PASS');assert.equal(evidence.convert({sourceInventory:inventory,targetPreflight:preflight,targetInventory:inventory,targetFinalGates:gates,manifest,migrationClassification:classification}).status,'FAIL');});
test('plan remains Verification with no capture or restore claim',()=>{const p=read('codex-input/AdsTable_EXECUTION_PLAN_V4_2026-08-17_TR.md');assert.match(p,/E2-T8 task aynası/);assert.match(p,/\*\*Durum:\*\* `Verification`/);for(const x of ['Actual schema capture yapılmadı','baseline SQL üretilmedi','target provision edilmedi','restore çalıştırılmadı','production değişmedi'])assert.match(p,new RegExp(x,'i'));});

test('captured identities match overloaded functions, trigger, policy, and quoted names exactly',()=>{
  const sql=`
CREATE TABLE public.accounts(id bigint);
CREATE FUNCTION public.calculate(value integer) RETURNS integer LANGUAGE sql AS $$ SELECT value $$;
CREATE FUNCTION public.calculate(value text) RETURNS text LANGUAGE sql AS $$ SELECT value $$;
CREATE FUNCTION public."Quoted Fn"("Input Value" integer) RETURNS integer LANGUAGE sql AS $$ SELECT 1 $$;
CREATE TRIGGER audit_trigger BEFORE INSERT ON public.accounts FOR EACH ROW EXECUTE FUNCTION public.calculate(1);
CREATE POLICY account_read ON public.accounts FOR SELECT USING (true);`;
  const keys=['relation:accounts','function:calculate(value integer)','function:calculate(value text)','function:Quoted Fn("Input Value" integer)','trigger:accounts.audit_trigger','policy:public.accounts.account_read'];
  const approved=keys.map((object_key)=>({object_key,object_class:object_key.split(':')[0],ownership_class:'application_owned',fingerprint:FP}));
  assert.equal(validator.validateCapturedSchema({capturedSql:sql,approvedSourceInventory:approved,restoreScope:scope,expectedGrantContract:grants,captureManifest:manifest}).status,'ARTIFACT_CONTRACT_PASS');
  for(const bad of [approved.map(x=>x.object_key==='function:calculate(value text)'?{...x,object_key:'function:calculate(value uuid)'}:x),approved.map(x=>x.object_key.startsWith('trigger:')?{...x,object_key:'trigger:other.audit_trigger'}:x),approved.map(x=>x.object_key.startsWith('policy:')?{...x,object_key:'policy:public.other.account_read'}:x)])
    assert.equal(validator.validateCapturedSchema({capturedSql:sql,approvedSourceInventory:bad,restoreScope:scope,expectedGrantContract:grants,captureManifest:manifest}).status,'ARTIFACT_CONTRACT_FAIL');
});

test('managed extension public objects are parity metadata, not required capture objects',()=>{
  const managed=[{object_key:'relation:spatial_ref_sys',object_class:'relation',ownership_class:'managed_extension_owned',fingerprint:FP}];
  assert.equal(validator.validateCapturedSchema({capturedSql:'SET statement_timeout = 0;',approvedSourceInventory:managed,restoreScope:scope,expectedGrantContract:grants,captureManifest:manifest}).status,'ARTIFACT_CONTRACT_PASS');
  assert.equal(validator.validateCapturedSchema({capturedSql:'CREATE TABLE public.spatial_ref_sys(id integer);',approvedSourceInventory:managed,restoreScope:scope,expectedGrantContract:grants,captureManifest:manifest}).status,'ARTIFACT_CONTRACT_FAIL');
});

test('final table gates bind exactly to source application relations and exclude managed extensions',()=>{
  const source=[...inventory,{object_key:'relation:thirteenth',object_class:'relation',ownership_class:'application_owned',fingerprint:FP},{object_key:'relation:spatial_ref_sys',object_class:'relation',ownership_class:'managed_extension_owned',fingerprint:FP}];
  const finalManifest={...manifest,baseline_cutoff:'cutoff',restore_ready:true},finalClass=structuredClone(classification);finalClass.migrations.forEach(m=>m.final_classification_status='final');
  const checksum=crypto.createHash('sha256').update(JSON.stringify(source)).digest('hex');
  const common={sourceInventory:source,targetInventory:source,targetPreflight:preflight,manifest:finalManifest,migrationClassification:finalClass};
  assert.equal(evidence.convert({...common,targetFinalGates:{managed_primitives_ok:true,source_inventory_sha256:checksum,application_table_counts:[{table_name:'accounts',row_count:0}],allowlist_complete:true}}).status,'FAIL');
  assert.equal(evidence.convert({...common,targetFinalGates:{managed_primitives_ok:true,source_inventory_sha256:checksum,application_table_counts:[{table_name:'accounts',row_count:0},{table_name:'thirteenth',row_count:0},{table_name:'extra',row_count:0}],allowlist_complete:true}}).status,'FAIL');
  assert.equal(evidence.convert({...common,targetFinalGates:{managed_primitives_ok:true,source_inventory_sha256:checksum,application_table_counts:[{table_name:'accounts',row_count:0},{table_name:'thirteenth',row_count:0}],allowlist_complete:true}}).status,'PASS');
});

test('CLI status allowlist makes every non-accepted status fail',()=>{
  for(const status of ['SOURCE_CREDENTIAL_UNAVAILABLE','QUARANTINE_DIRECTORY_REQUIRED','REPOSITORY_OUTPUT_FORBIDDEN','CAPTURE_TOOL_UNAVAILABLE','CAPTURE_COMMAND_FAILED','CAPTURE_QUARANTINED_VALIDATION_REQUIRED','CAPTURE_QUARANTINED_CONTRACT_FAIL','UNKNOWN'])assert.equal(capture.exitCodeForStatus(status),1,status);
  for(const status of ['PLAN_ONLY','CAPTURE_QUARANTINED_CONTRACT_PASS'])assert.equal(capture.exitCodeForStatus(status),0,status);
  const cp=require('node:child_process');const cli=path.join(root,'scripts/e2-t8-schema-capture.js');
  assert.equal(cp.spawnSync(process.execPath,[cli],{env:{PATH:process.env.PATH},encoding:'utf8'}).status,0);
  assert.equal(cp.spawnSync(process.execPath,[cli,'--execute','--confirm',capture.CONFIRMATION],{env:{PATH:process.env.PATH},encoding:'utf8'}).status,1);
});

test('SQL ownership provenance is explicit, owner-independent, inherited, and fail-closed',()=>{
  const sql=read('docs/security/sql/E2_T8_SOURCE_INVENTORY.sql');
  for(const relation of ['dashboard_snapshots','fx_rates_daily','oauth_transactions','performance_dataset_rows','performance_dataset_rows_v2','platform_account_ownerships','platform_ad_accounts','platform_connection_tokens','platform_connections','snapshot_jobs','snapshot_schedules','users'])assert.match(sql,new RegExp(`\\('${relation}'\\)`));
  assert.doesNotMatch(sql,/c\.relowner|supabase_admin|rolname IN \('postgres'/i);
  assert.match(sql,/e\.objid IS NOT NULL THEN 'managed_extension_owned'[\s\S]*a\.relname IS NOT NULL THEN 'application_owned'[\s\S]*m\.relname IS NOT NULL THEN 'excluded_managed'[\s\S]*ELSE 'unclassified'/);
  for(const objectClass of ['column:','constraint:','index:','trigger:','policy:','grant:'])assert.match(sql,new RegExp(objectClass));
  assert.ok((sql.match(/o\.ownership_class/g)||[]).length>=7);
  assert.match(sql,/VALUES \('spatial_ref_sys'\)/);
});

test('postgres or supabase_admin ownership cannot override explicit application provenance',()=>{
  const classify=({extension=false,application=false,managed=false})=>extension?'managed_extension_owned':application?'application_owned':managed?'excluded_managed':'unclassified';
  assert.equal(classify({application:true,owner:'postgres'}),'application_owned');
  assert.equal(classify({application:true,owner:'supabase_admin'}),'application_owned');
  assert.equal(classify({extension:true,application:true}),'managed_extension_owned');
  assert.equal(classify({managed:true}),'excluded_managed');
  assert.equal(classify({}),'unclassified');
});

test('unclassified source ownership stops captured contract and evidence',()=>{
  const unknown=[{object_key:'relation:unknown',object_class:'relation',ownership_class:'unclassified',fingerprint:FP}];
  assert.equal(validator.validateCapturedSchema({capturedSql:'SET statement_timeout=0;',approvedSourceInventory:unknown,restoreScope:scope,expectedGrantContract:grants,captureManifest:manifest}).status,'ARTIFACT_CONTRACT_FAIL');
});

test('regular and unique index identities match exact inventory keys',()=>{
  const relation={object_key:'relation:performance_dataset_rows_v2',object_class:'relation',ownership_class:'application_owned',fingerprint:FP};
  const check=(statement,key)=>validator.validateCapturedSchema({capturedSql:`CREATE TABLE public.performance_dataset_rows_v2(id bigint); ${statement}`,approvedSourceInventory:[relation,{object_key:key,object_class:'index',ownership_class:'application_owned',fingerprint:FP}],restoreScope:scope,expectedGrantContract:grants,captureManifest:manifest}).status;
  const unique='CREATE UNIQUE INDEX performance_dataset_rows_v2_canonical_uidx ON public.performance_dataset_rows_v2 (id);';
  assert.equal(check(unique,'index:performance_dataset_rows_v2_canonical_uidx'),'ARTIFACT_CONTRACT_PASS');
  assert.equal(check(unique,'index:wrong_name'),'ARTIFACT_CONTRACT_FAIL');
  assert.equal(check('CREATE INDEX performance_dataset_rows_v2_idx ON public.performance_dataset_rows_v2(id);','index:performance_dataset_rows_v2_idx'),'ARTIFACT_CONTRACT_PASS');
  assert.equal(check('CREATE UNIQUE INDEX "Canonical UIdx" ON public.performance_dataset_rows_v2(id);','index:Canonical UIdx'),'ARTIFACT_CONTRACT_PASS');
  assert.equal(check('CREATE UNIQUE INDEX private.performance_dataset_rows_v2_canonical_uidx ON public.performance_dataset_rows_v2(id);','index:performance_dataset_rows_v2_canonical_uidx'),'ARTIFACT_CONTRACT_FAIL');
});
