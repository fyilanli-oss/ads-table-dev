"use strict";

const test=require("node:test");
const assert=require("node:assert/strict");
const fs=require("node:fs");
const path=require("node:path");
const {encodeCursor}=require("../security/provider-token-backfill");
const {BackfillOperatorError,parseArguments,readConfig,safeEvidence,runDryRunOperator}=require("../security/provider-token-backfill-operator");

const key=Buffer.alloc(32,7).toString("base64");
const referenceSecret="operator-test-reference-secret-with-32-bytes";
const env={SUPABASE_URL:"https://example.supabase.co",SUPABASE_SERVICE_ROLE_KEY:"service-role-test-value",PROVIDER_TOKEN_ACTIVE_KEY_ID:"current",PROVIDER_TOKEN_ENCRYPTION_KEYS:JSON.stringify({current:key}),PROVIDER_TOKEN_BACKFILL_REFERENCE_SECRET:referenceSecret};

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

test("config is fail-fast for missing, invalid, short, or reused secrets",()=>{
  for(const name of ["SUPABASE_URL","SUPABASE_SERVICE_ROLE_KEY","PROVIDER_TOKEN_ACTIVE_KEY_ID","PROVIDER_TOKEN_ENCRYPTION_KEYS","PROVIDER_TOKEN_BACKFILL_REFERENCE_SECRET"]){
    assert.throws(()=>readConfig({...env,[name]:""}),{code:"BACKFILL_CONFIG_MISSING"});
  }
  assert.throws(()=>readConfig({...env,SUPABASE_URL:"not-a-url"}),{code:"BACKFILL_CONFIG_INVALID"});
  assert.throws(()=>readConfig({...env,PROVIDER_TOKEN_BACKFILL_REFERENCE_SECRET:"too-short"}),{code:"BACKFILL_CONFIG_INVALID"});
  assert.throws(()=>readConfig({...env,PROVIDER_TOKEN_BACKFILL_REFERENCE_SECRET:env.SUPABASE_SERVICE_ROLE_KEY.padEnd(32,"x"),SUPABASE_SERVICE_ROLE_KEY:env.SUPABASE_SERVICE_ROLE_KEY.padEnd(32,"x")}),{code:"BACKFILL_CONFIG_INVALID"});
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

test("evidence is allowlisted, forces written zero, and sanitizes failures",()=>{
  const evidence=safeEvidence({scanned:1,written:99,nextCursor:undefined,access_token:"raw-access",refresh_token:"raw-refresh",user_id:"raw-user",secret:key,failures:[{userRef:"a".repeat(64),platform:"meta",code:"TOKEN_DECRYPTION_FAILED",message:"raw database error",accessToken:"raw-access"}]},25);
  assert.deepEqual(Object.keys(evidence.failures[0]),["userRef","platform","code"]);
  assert.equal(evidence.written,0);assert.equal(evidence.nextCursor,null);
  assert.doesNotMatch(JSON.stringify(evidence),/raw-access|raw-refresh|raw-user|raw database error/);
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
  for(const name of ["PROVIDER_TOKEN_ACTIVE_KEY_ID","PROVIDER_TOKEN_ENCRYPTION_KEYS","PROVIDER_TOKEN_BACKFILL_REFERENCE_SECRET"])assert.match(example,new RegExp(`^${name}=$`,"m"));
  assert.doesNotMatch(example,/PROVIDER_TOKEN_(?:ENCRYPTION_KEYS|BACKFILL_REFERENCE_SECRET)=.+/);
});
