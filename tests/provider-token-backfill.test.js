"use strict";

const test=require("node:test");
const assert=require("node:assert/strict");
const fs=require("node:fs");
const path=require("node:path");
const {createProviderTokenBackfill,encodeCursor,decodeCursor}=require("../security/provider-token-backfill");

const secret="test-only-redaction-secret";
const client={from(){return {}}};
const sortedList=rows=>async(_client,cursor,limit)=>rows.filter(row=>!cursor||row.user_id>cursor.userId||(row.user_id===cursor.userId&&row.platform>cursor.platform)).sort((a,b)=>a.user_id.localeCompare(b.user_id)||a.platform.localeCompare(b.platform)).slice(0,limit);

function harness(rows,states={}){
  const writes=[];
  const store={
    async resolve({userId,platform,legacyAccessToken,legacyRefreshToken}){
      const state=states[`${userId}/${platform}`];
      if(state?.error)throw state.error;
      if(state)return state;
      return {accessToken:legacyAccessToken,refreshToken:legacyRefreshToken,source:"legacy",needsRotation:false};
    },
    async write(value){writes.push(value);}
  };
  return {writes,run:createProviderTokenBackfill({client,tokenStore:store,userRefSecret:secret,listConnections:sortedList(rows)})};
}

test("dry-run is default, makes no writes, and does not disclose plaintext",async()=>{
  const {run,writes}=harness([{user_id:"real-user",platform:"meta",access_token:"plain-access",refresh_token:"plain-refresh"}]);
  const result=await run();
  assert.equal(result.eligible,1);assert.equal(result.written,0);assert.equal(writes.length,0);
  assert.doesNotMatch(JSON.stringify(result),/plain-access|plain-refresh/);
});

test("stable keyset cursor resumes after the last user/platform pair",async()=>{
  const rows=[{user_id:"a",platform:"google",access_token:"1"},{user_id:"a",platform:"meta",access_token:"2"},{user_id:"b",platform:"google",access_token:"3"}];
  const {run}=harness(rows);
  const first=await run({batchSize:1});
  assert.deepEqual(decodeCursor(first.nextCursor,secret),{userId:"a",platform:"google"});
  const second=await run({batchSize:1,cursor:first.nextCursor});
  assert.deepEqual(decodeCursor(second.nextCursor,secret),{userId:"a",platform:"meta"});
});

test("cursor validation rejects malformed and non-canonical values",()=>{
  assert.throws(()=>decodeCursor("not-json",secret),{code:"INVALID_CURSOR"});
  assert.throws(()=>decodeCursor("v1.bad.bad.bad",secret),{code:"INVALID_CURSOR"});
  const cursor=encodeCursor({userId:"a",platform:"meta"},secret);
  assert.equal(decodeCursor(cursor,secret).platform,"meta");
  assert.doesNotMatch(cursor,/meta/);
});

test("batch sizes outside 1 through 100 are rejected",async()=>{
  const {run}=harness([]);
  for(const batchSize of [0,101,1.5,"25"])await assert.rejects(run({batchSize}),{code:"INVALID_BATCH_SIZE"});
});

test("write mode encrypts legacy tokens through the token store",async()=>{
  const {run,writes}=harness([{user_id:"u",platform:"google",access_token:"access",refresh_token:"refresh"}]);
  const result=await run({dryRun:false});
  assert.equal(result.written,1);assert.equal(writes.length,1);assert.equal(writes[0].accessToken,"access");
});

test("current envelopes are skipped idempotently",async()=>{
  const state={accessToken:"access",refreshToken:null,source:"encrypted",needsRotation:false};
  const {run,writes}=harness([{user_id:"u",platform:"meta"}],{"u/meta":state});
  const result=await run({dryRun:false});
  assert.equal(result.alreadyEncrypted,1);assert.equal(result.eligible,0);assert.equal(writes.length,0);
});

test("old-key envelopes are rotation candidates and are rewritten",async()=>{
  const state={accessToken:"access",refreshToken:"refresh",source:"encrypted",needsRotation:true};
  const {run,writes}=harness([{user_id:"u",platform:"meta"}],{"u/meta":state});
  const result=await run({dryRun:false});
  assert.equal(result.rotationCandidates,1);assert.equal(result.written,1);assert.equal(writes.length,1);
});

test("tokenless rows are counted as empty",async()=>{
  const {run}=harness([{user_id:"u",platform:"meta",access_token:null,refresh_token:null}]);
  const result=await run();assert.equal(result.empty,1);assert.equal(result.eligible,0);
});

test("row failure is redacted and does not stop later rows",async()=>{
  const error=Object.assign(new Error("secret exception message with plaintext-token"),{code:"unsafe-code secret"});
  const rows=[{user_id:"actual-user-id",platform:"google"},{user_id:"later-user",platform:"meta",access_token:"later-token"}];
  const {run}=harness(rows,{"actual-user-id/google":{error}});
  const result=await run();const evidence=JSON.stringify(result);
  assert.equal(result.scanned,2);assert.equal(result.failed,1);assert.equal(result.eligible,1);
  assert.deepEqual(Object.keys(result.failures[0]),["userRef","platform","code"]);
  assert.match(result.failures[0].userRef,/^[a-f0-9]{64}$/);assert.equal(result.failures[0].code,"BACKFILL_ROW_FAILED");
  assert.doesNotMatch(evidence,/actual-user-id|secret exception|plaintext-token|later-token/);
});

test("runtime token consumers use centralized getConnection hydration",()=>{
  const source=fs.readFileSync(path.join(__dirname,"..","server.js"),"utf8");
  assert.match(source,/runQueuedBackfillJob[\s\S]*?const conn=await getConnection\(job\.user_id,platform\)/);
  assert.match(source,/runMetaAutoRefreshForSchedule[\s\S]*?getConnection\(schedule\.user_id,"meta"\)/);
  assert.match(source,/runGoogleAutoRefreshForSchedule[\s\S]*?getConnection\(schedule\.user_id,"google"\)/);
  assert.match(source,/disconnectPlatformLifecycle[\s\S]*?getConnection\(userId,platform\)/);
  assert.match(source,/auth\/google-sheets\/callback[\s\S]*?getConnection\(userId,GOOGLE_SHEETS_PLATFORM\)/);
});
