"use strict";

const test=require("node:test");
const assert=require("node:assert/strict");
const fs=require("node:fs");
const path=require("node:path");
const crypto=require("node:crypto");
const {encodeCursor}=require("../security/provider-token-backfill");
const {BackfillOperatorError,parseArguments,readConfig,safeEvidence,runDryRunOperator}=require("../security/provider-token-backfill-operator");

const key=crypto.createHash("sha256").update("test-only-provider-key").digest("base64");
const referenceSecret=crypto.createHash("sha256").update("test-only-reference-key").digest("base64");
const projectRef="abcdefghijklmnopqrst";
const env={SUPABASE_URL:`https://${projectRef}.supabase.co`,SUPABASE_PROJECT_REF:projectRef,SUPABASE_SERVICE_ROLE_KEY:"service-role-test-value",PROVIDER_TOKEN_ACTIVE_KEY_ID:"current",PROVIDER_TOKEN_ENCRYPTION_KEYS:JSON.stringify({current:key}),PROVIDER_TOKEN_BACKFILL_REFERENCE_SECRET:referenceSecret};

function dependencies(result={scanned:1,eligible:1,written:0,alreadyEncrypted:0,rotationCandidates:0,empty:0,failed:0,nextCursor:null,failures:[]}){
  const calls={};
  return {calls,value:{
    createClient(...args){calls.client=args;return {from(){return {};}};},
    createVault(received){calls.vaultEnv=received;return {encrypt(){},decrypt(){},needsRotation(){return false;}};},
    createStore(options){calls.store=options;return {resolve(){},write(){calls.write=true;}};},
    createBackfill(options){calls.backfill=options;return async options2=>{calls.run=options2;return result;};}
  }};
}

test("argument parser defaults to 25 and accepts boundary batch sizes",()=>{
  assert.deepEqual(parseArguments([]),{batchSize:25,cursor:null});
  assert.equal(parseArguments(["--batch-size","1"]).batchSize,1);
  assert.equal(parseArguments(["--batch-size","100"]).batchSize,100);
});

test("argument parser rejects invalid batches and unknown arguments",()=>{
  for(const value of ["0","101","-1","1.5","NaN","text"])assert.throws(()=>parseArguments(["--batch-size",value]),BackfillOperatorError);
  assert.throws(()=>parseArguments(["--unknown"]),{code:"BACKFILL_ARGUMENT_INVALID"});
});

test("all write-mode spellings are forbidden",()=>{
  for(const args of [["--write"],["--execute"],["--apply"],["--dry-run=false"],["--dry-run","false"],["dryRun=false"]])assert.throws(()=>parseArguments(args),{code:"BACKFILL_WRITE_MODE_FORBIDDEN"});
});

test("config is fail-fast for missing values and binds the URL to the exact Supabase project",()=>{
  for(const name of ["SUPABASE_URL","SUPABASE_PROJECT_REF","SUPABASE_SERVICE_ROLE_KEY","PROVIDER_TOKEN_ACTIVE_KEY_ID","PROVIDER_TOKEN_ENCRYPTION_KEYS","PROVIDER_TOKEN_BACKFILL_REFERENCE_SECRET"]){
    assert.throws(()=>readConfig({...env,[name]:""}),{code:"BACKFILL_CONFIG_MISSING"});
  }
  for(const url of ["https://example.com",`https://wrongprojectref00000.supabase.co`,`${env.SUPABASE_URL}/rest/v1`,`${env.SUPABASE_URL}?key=value`,`${env.SUPABASE_URL}#fragment`,`https://user:password@${projectRef}.supabase.co`,`http://${projectRef}.supabase.co`]){
    assert.throws(()=>readConfig({...env,SUPABASE_URL:url}),{code:"BACKFILL_CONFIG_INVALID"});
  }
  assert.throws(()=>readConfig({...env,SUPABASE_PROJECT_REF:"wrongprojectref00000"}),{code:"BACKFILL_CONFIG_INVALID"});
});

test("reference secret requires canonical, high-entropy 32-byte base64 and key separation",()=>{
  const lowEntropy=Buffer.alloc(32,65).toString("base64");
  for(const secret of ["not-base64!",Buffer.alloc(31,3).toString("base64"),lowEntropy])assert.throws(()=>readConfig({...env,PROVIDER_TOKEN_BACKFILL_REFERENCE_SECRET:secret}),{code:"BACKFILL_CONFIG_INVALID"});
  assert.throws(()=>readConfig({...env,PROVIDER_TOKEN_BACKFILL_REFERENCE_SECRET:key}),{code:"BACKFILL_CONFIG_INVALID"});
  for(const name of ["SUPABASE_SERVICE_ROLE_KEY","SUPABASE_DB_PASSWORD","SUPABASE_ACCESS_TOKEN"])assert.throws(()=>readConfig({...env,[name]:referenceSecret}),{code:"BACKFILL_CONFIG_INVALID"});
  assert.equal(readConfig(env).referenceSecret,referenceSecret);
  let error;try{readConfig({...env,SUPABASE_DB_PASSWORD:referenceSecret});}catch(candidate){error=candidate;}
  assert.doesNotMatch(JSON.stringify({message:error.message,code:error.code}),new RegExp(referenceSecret.replace(/[.*+?^${}()|[\]\\]/g,"\\$&")));
});

test("operator composes server-side dependencies and always explicitly runs dry-run",async()=>{
  const injected=dependencies();
  const output=await runDryRunOperator({env,dependencies:injected.value});
  assert.equal(injected.calls.client[0],env.SUPABASE_URL);assert.equal(injected.calls.client[1],env.SUPABASE_SERVICE_ROLE_KEY);
  assert.deepEqual(injected.calls.client[2].auth,{persistSession:false,autoRefreshToken:false,detectSessionInUrl:false});
  assert.equal(injected.calls.store.legacyReadsEnabled,true);
  assert.deepEqual(injected.calls.run,{dryRun:true,batchSize:25,cursor:null});
  assert.equal(output.mode,"dry-run");assert.equal(output.written,0);assert.equal(injected.calls.write,undefined);
});

test("valid cursor is passed opaquely and a tampered cursor fails closed",async()=>{
  const cursor=encodeCursor({userId:"private-user",platform:"meta"},referenceSecret);
  const injected=dependencies();
  const output=await runDryRunOperator({argv:["--cursor",cursor],env,dependencies:injected.value});
  assert.equal(injected.calls.run.cursor,cursor);assert.doesNotMatch(JSON.stringify(output),/private-user/);
  await assert.rejects(runDryRunOperator({argv:["--cursor",`${cursor}x`],env,dependencies:injected.value}),{code:"BACKFILL_CURSOR_INVALID"});
});

test("evidence is allowlisted and sanitizes failures without hiding writes",()=>{
  const result={scanned:1,eligible:1,written:0,alreadyEncrypted:0,rotationCandidates:0,empty:0,failed:1,nextCursor:null,access_token:"raw-access",refresh_token:"raw-refresh",user_id:"raw-user",secret:key,failures:[{userRef:"a".repeat(64),platform:"meta",code:"TOKEN_DECRYPTION_FAILED",message:"raw database error",accessToken:"raw-access"}]};
  const evidence=safeEvidence(result,25,referenceSecret);
  assert.deepEqual(Object.keys(evidence.failures[0]),["userRef","platform","code"]);
  assert.equal(evidence.written,0);assert.equal(evidence.nextCursor,null);
  assert.doesNotMatch(JSON.stringify(evidence),new RegExp(`raw-access|raw-refresh|raw-user|raw database error|${referenceSecret.replace(/[.*+?^${}()|[\]\\]/g,"\\$&")}`));
  assert.throws(()=>safeEvidence({...result,written:99},25,referenceSecret),{code:"BACKFILL_DRY_RUN_INVARIANT_FAILED"});
});

test("all counters must be finite non-negative integers",()=>{
  const valid={scanned:0,eligible:0,written:0,alreadyEncrypted:0,rotationCandidates:0,empty:0,failed:0,nextCursor:null,failures:[]};
  for(const value of [-1,1.5,NaN,Infinity,"0",undefined])assert.throws(()=>safeEvidence({...valid,scanned:value},25,referenceSecret),{code:"BACKFILL_DRY_RUN_INVARIANT_FAILED"});
});

test("runner nextCursor must be null or an authenticated cursor",()=>{
  const valid={scanned:0,eligible:0,written:0,alreadyEncrypted:0,rotationCandidates:0,empty:0,failed:0,nextCursor:null,failures:[]};
  assert.equal(safeEvidence(valid,25,referenceSecret).nextCursor,null);
  const cursor=encodeCursor({userId:"private-user",platform:"meta"},referenceSecret);
  assert.equal(safeEvidence({...valid,nextCursor:cursor},25,referenceSecret).nextCursor,cursor);
  for(const nextCursor of [`${cursor}x`,"arbitrary-secret-token","",42,{},[],true,undefined]){
    let error;try{safeEvidence({...valid,nextCursor},25,referenceSecret);}catch(candidate){error=candidate;}
    assert.equal(error.code,"BACKFILL_DRY_RUN_INVARIANT_FAILED");
    assert.doesNotMatch(JSON.stringify({message:error.message,code:error.code}),/arbitrary-secret-token|private-user/);
  }
});

test("failed count and failure records must satisfy the safe contract",()=>{
  const validFailure={userRef:"b".repeat(64),platform:"google_ads",code:"TOKEN_DECRYPTION_FAILED",accessToken:"sensitive-extra"};
  const base={scanned:1,eligible:0,written:0,alreadyEncrypted:0,rotationCandidates:0,empty:0,failed:0,nextCursor:null,failures:[]};
  assert.deepEqual(safeEvidence(base,25,referenceSecret).failures,[]);
  assert.deepEqual(safeEvidence({...base,failed:1,failures:[validFailure]},25,referenceSecret).failures,[{userRef:validFailure.userRef,platform:validFailure.platform,code:validFailure.code}]);
  const invalidResults=[
    {...base,failed:1,failures:[]},{...base,failures:[validFailure]},{...base,failures:undefined},{...base,failures:{}},
    {...base,failed:1,failures:[{...validFailure,userRef:"RAW-USER-ID"}]},
    {...base,failed:1,failures:[{...validFailure,platform:"invalid platform/value"}]},
    {...base,failed:1,failures:[{...validFailure,code:"unsafe-error"}]}
  ];
  for(const result of invalidResults){
    let error;try{safeEvidence(result,25,referenceSecret);}catch(candidate){error=candidate;}
    assert.equal(error.code,"BACKFILL_DRY_RUN_INVARIANT_FAILED");
    assert.doesNotMatch(JSON.stringify({message:error.message,code:error.code}),/RAW-USER-ID|invalid platform|unsafe-error|sensitive-extra/);
  }
});

test("runner write or counter invariant violation fails closed with no successful evidence",async()=>{
  for(const result of [{scanned:1,eligible:1,written:1,alreadyEncrypted:0,rotationCandidates:0,empty:0,failed:0,failures:[]},{scanned:-1,eligible:0,written:0,alreadyEncrypted:0,rotationCandidates:0,empty:0,failed:0,failures:[]}]){
    const injected=dependencies(result);
    await assert.rejects(runDryRunOperator({env,dependencies:injected.value}),{code:"BACKFILL_DRY_RUN_INVARIANT_FAILED"});
  }
});

test("scan and raw runtime failures map to deterministic redacted codes",async()=>{
  for(const [error,code] of [[Object.assign(new Error("secret scan response"),{code:"BACKFILL_SCAN_FAILED"}),"BACKFILL_SCAN_FAILED"],[new Error("secret stack and token"),"BACKFILL_DRY_RUN_FAILED"]]){
    const injected=dependencies();injected.value.createBackfill=()=>async()=>{throw error;};
    await assert.rejects(runDryRunOperator({env,dependencies:injected.value}),candidate=>candidate.code===code&&!candidate.message.includes("secret"));
  }
});

test("invalid keyring is redacted as config invalid",async()=>{
  const injected=dependencies();injected.value.createVault=()=>{throw Object.assign(new Error("key material leaked"),{code:"TOKEN_VAULT_CONFIG_ERROR"});};
  await assert.rejects(runDryRunOperator({env,dependencies:injected.value}),candidate=>candidate.code==="BACKFILL_CONFIG_INVALID"&&!candidate.message.includes("material"));
});

test("operator has no server import and package script is dry-run-only",()=>{
  const operator=fs.readFileSync(path.join(__dirname,"../security/provider-token-backfill-operator.js"),"utf8");
  const script=fs.readFileSync(path.join(__dirname,"../scripts/provider-token-backfill-dry-run.js"),"utf8");
  const pkg=require("../package.json");
  assert.doesNotMatch(operator,/require\(["']\.\.\/server/);assert.doesNotMatch(script,/server\.js/);
  assert.equal(pkg.scripts["tokens:backfill:dry-run"],"node scripts/provider-token-backfill-dry-run.js");
  assert.doesNotMatch(pkg.scripts["tokens:backfill:dry-run"],/write|execute|apply/);
});

test("example environment contains names only, not usable provider secrets",()=>{
  const example=fs.readFileSync(path.join(__dirname,"../.env.example"),"utf8");
  for(const name of ["SUPABASE_PROJECT_REF","PROVIDER_TOKEN_ACTIVE_KEY_ID","PROVIDER_TOKEN_ENCRYPTION_KEYS","PROVIDER_TOKEN_BACKFILL_REFERENCE_SECRET"])assert.match(example,new RegExp(`^${name}=$`,"m"));
  assert.doesNotMatch(example,/PROVIDER_TOKEN_(?:ENCRYPTION_KEYS|BACKFILL_REFERENCE_SECRET)=.+/);
});
