'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path'),test=require('node:test');const op=require('../operator/e2-t7-v2'),store=require('../operator/state-store'),converter=require('../scripts/e2-t7-cleanup-evidence'),{parseArgs}=require('../scripts/e2-t7-v2-operator');const repo=path.join(__dirname,'..');
function tmp(){const dir=fs.mkdtempSync(path.join(os.tmpdir(),'e2-t7-v2-'));return{dir,file:path.join(dir,'state.json')}}
function rows(final=false){return converter.CHECK_CODES.map((check_code,i)=>{let n=['DATASET_V2_BASELINE','DATASET_V1_BASELINE','SNAPSHOT_BASELINE'].includes(check_code)?10+i:0;if(check_code==='LEDGER_TOTAL')n=37;if(['CONNECTED_CONNECTIONS','ENCRYPTED_TOKEN_ROWS'].includes(check_code))n=4;if(check_code==='CONNECTION_TOKEN_PARITY')n=1;if(check_code==='DATASET_V2_SCHEMA_STATE')n=26;if(check_code==='DATASET_V2_RLS_POLICY_GRANT_STATE')n=5;return{check_code,actual_count:n,expected_count:n,comparison:final?'eq':(['DATASET_V2_BASELINE','DATASET_V1_BASELINE','SNAPSHOT_BASELINE','CONNECTED_CONNECTIONS','ENCRYPTED_TOKEN_ROWS'].includes(check_code)?'capture':'eq'),passed:true}})}
function fake(results){let calls=0;return{get calls(){return calls},query:async()=>{const x=results[calls++];if(x instanceof Error)throw x;return{rows:x}}}}
const verified={verifyRepository(){}};
const diagnostic=require('../operator/e2-t7-final-diagnostic');
const diagnosticCli=require('../scripts/e2-t7-final-diagnostic');
test('T7 V2 binds exact approved main and complete artifacts',()=>{assert.equal(op.APPROVED_MAIN_SHA,'ba298aa8e9ac5701d5609d48770f13909951bdb2');for(const file of [...Object.values(op.SQL),'operator/e2-t7-v2.js','scripts/e2-t7-v2-operator.js'])assert(op.ARTIFACTS.includes(file));assert.equal(op.checksum(repo).length,64)});
test('T7 V2 CLI accepts only exact preflight and execute forms',()=>{assert.deepEqual(parseArgs(['preflight']),{action:'preflight'});assert.deepEqual(parseArgs(['execute','--confirm',op.CONFIRMATION]),{action:'execute',confirmation:op.CONFIRMATION});for(const args of [[],['execute'],['preflight','x']])assert.throws(()=>parseArgs(args))});
test('T7 V2 preflight captures one read-only baseline and creates external capsule',async()=>{const t=tmp(),client=fake([rows()]),report=await op.preflight({repo,stateFile:t.file,client,runTests(){},...verified});assert.equal(report.baseline,'19/19 PASS');assert.equal(client.calls,1);assert.equal(fs.statSync(`${t.file}.baseline.json`).mode&0o777,0o600);assert.deepEqual(store.readState(t.file,op.binding(repo),repo).baselines,{datasetRows:10,v1Rows:16,snapshotRows:17,connectedRows:4,encryptedRows:4})});
test('T7 V2 execute sends final once, consumes state, and emits redacted PASS',async()=>{const t=tmp(),pre=fake([rows()]);await op.preflight({repo,stateFile:t.file,client:pre,runTests(){},...verified});const client=fake([rows(true)]),report=await op.execute({repo,stateFile:t.file,client,confirmation:op.CONFIRMATION,...verified});assert.equal(client.calls,1);assert.equal(report.status,'PASS');assert.equal(report.finalRequests,1);assert.equal(report.retries,0);assert.equal(report.productionCountsExposed,false);assert.equal(store.readState(t.file,op.binding(repo),repo).consumed,true);await assert.rejects(op.execute({repo,stateFile:t.file,client,confirmation:op.CONFIRMATION,...verified}));assert.equal(client.calls,1)});
test('T7 V2 fails closed before or after one final request',async()=>{const t=tmp();await assert.rejects(op.preflight({repo,stateFile:t.file,client:fake([new Error('raw')]),runTests(){},...verified}),e=>e.safeCode==='BASELINE_QUERY_FAILED');const t2=tmp();await op.preflight({repo,stateFile:t2.file,client:fake([rows()]),runTests(){},...verified});await assert.rejects(op.execute({repo,stateFile:t2.file,client:fake([new Error('raw')]),confirmation:op.CONFIRMATION,...verified}),e=>e.safeCode==='FINAL_QUERY_FAILED');assert.equal(store.readState(t2.file,op.binding(repo),repo).consumed,true)});
test('T7 final substitution is exact and zero-safe',()=>{const sql=fs.readFileSync(path.join(repo,op.SQL.final),'utf8'),out=op.finalSql(sql,{datasetRows:0,v1Rows:0,snapshotRows:0,connectedRows:0,encryptedRows:0});assert.equal((sql.match(/\(-1\)::bigint/g)||[]).length,5);assert.doesNotMatch(out,/\(-1\)::bigint/);assert.equal((out.match(/\(0\)::bigint/g)||[]).length,5)});
test('committed T7 baseline evidence is redacted and final-free',()=>{const value=JSON.parse(fs.readFileSync(path.join(repo,'artifacts/dataset-v2-acceptance/e2-t7-cleanup/v2-baseline-live.json'),'utf8'));assert.deepEqual(value,{operation:'e2_t7_no_change_v2',status:'APPROVAL_READY',baseline:'19/19 PASS',baselineCount:5,baselineRequests:1,finalRequests:0,retries:0,productionCountsExposed:false,productionIdentitiesExposed:false});assert.doesNotMatch(JSON.stringify(value),/actual_count|expected_count|user_id|email|uuid|https?:/i)});
test('T7 diagnostic CLI uses the management client factory and is import-safe',()=>{
  assert.deepEqual(diagnosticCli.parseArgs(['--confirm',diagnostic.CONFIRMATION]),{confirmation:diagnostic.CONFIRMATION});
  for(const args of [[],['--confirm'],['diagnose','--confirm',diagnostic.CONFIRMATION]])assert.throws(()=>diagnosticCli.parseArgs(args));
  const source=fs.readFileSync(path.join(repo,'scripts/e2-t7-final-diagnostic.js'),'utf8');
  assert.match(source,/createManagementClient\(\{ token:/);
  assert.doesNotMatch(source,/\bcreateClient\(/);
});
test('T7 corrective diagnostic returns classifications without production values',async()=>{
  const t=tmp();
  await op.preflight({repo,stateFile:t.file,client:fake([rows()]),runTests(){},...verified});
  await assert.rejects(op.execute({repo,stateFile:t.file,client:fake([[]]),confirmation:op.CONFIRMATION,...verified}),e=>e.safeCode==='FINAL_EVIDENCE_FAILED');
  const classified=converter.CHECK_CODES.map(check_code=>({check_code,passed:check_code!=='DATASET_V2_BASELINE'}));
  const report=await diagnostic.diagnose({repo,stateFile:t.file,client:fake([classified]),confirmation:diagnostic.CONFIRMATION,...verified});
  assert.deepEqual(report.failedCodes,['DATASET_V2_BASELINE']);
  assert.equal(report.productionCountsExposed,false);
  assert.doesNotMatch(JSON.stringify(report),/actual_count|expected_count|user_id|email|uuid|https?:/i);
  await assert.rejects(diagnostic.diagnose({repo,stateFile:t.file,client:fake([classified]),confirmation:diagnostic.CONFIRMATION,...verified}));
});
test('T7 corrective diagnostic SQL is one read-only statement and returns no counts',()=>{
  const sql=fs.readFileSync(path.join(repo,diagnostic.SQL),'utf8');
  const executable=sql.replace(/'(?:''|[^'])*'/g,"''");
  assert.match(sql,/^-- E2-T7 corrective diagnostic/);
  assert.doesNotMatch(executable,/\b(insert|update|delete|truncate|alter|drop|create|grant|revoke|commit|rollback)\b/i);
  assert.match(sql,/select check_code, case comparison/);
  assert.doesNotMatch(sql,/select check_code,actual_count/);
  assert.equal((sql.match(/\(-1\)::bigint/g)||[]).length,5);
});
