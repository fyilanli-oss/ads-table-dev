"use strict";

const assert=require("node:assert/strict");
const test=require("node:test");
const {createProviderTokenVault}=require("../security/provider-token-vault");
const {createProviderTokenStore}=require("../security/provider-token-store");

const vault=createProviderTokenVault({activeKeyId:"key-a",keys:new Map([["key-a",Buffer.alloc(32,7)]])});

function fakeClient(initial=null){
  const state={row:initial,upserts:[],deletes:0};
  return {state,from(table){
    assert.equal(table,"platform_connection_tokens");
    const filters={};
    const builder={
      select(){return builder;},
      eq(key,value){filters[key]=value;return builder;},
      async maybeSingle(){return {data:state.row,error:null};},
      async upsert(row){state.upserts.push(row);state.row=row;return {error:null};},
      delete(){builder.then=resolve=>{state.deletes+=1;state.row=null;resolve({error:null});};return builder;}
    };
    return builder;
  }};
}

test("encrypted write persists envelopes and never plaintext",async()=>{
  const client=fakeClient();
  const store=createProviderTokenStore({client,vault});
  await store.write({userId:"user-a",platform:"google",accessToken:"access-secret",refreshToken:"refresh-secret"});
  assert.equal(client.state.upserts.length,1);
  const serialized=JSON.stringify(client.state.upserts[0]);
  assert.doesNotMatch(serialized,/access-secret|refresh-secret/);
  assert.equal(client.state.upserts[0].user_id,"user-a");
  assert.equal(client.state.upserts[0].platform,"google");
});

test("encrypted read decrypts both token types with their bound context",async()=>{
  const client=fakeClient({
    access_token_envelope:vault.encrypt("access",{userId:"user-a",platform:"google",tokenType:"access"}),
    refresh_token_envelope:vault.encrypt("refresh",{userId:"user-a",platform:"google",tokenType:"refresh"})
  });
  const result=await createProviderTokenStore({client,vault}).resolve({userId:"user-a",platform:"google",legacyAccessToken:"legacy"});
  assert.deepEqual(result,{accessToken:"access",refreshToken:"refresh",source:"encrypted",needsRotation:false});
});

test("legacy reads are explicit and can be disabled",async()=>{
  const client=fakeClient();
  const enabled=await createProviderTokenStore({client,vault,legacyReadsEnabled:true}).resolve({userId:"user-a",platform:"meta",legacyAccessToken:"legacy"});
  assert.equal(enabled.accessToken,"legacy");
  assert.equal(enabled.source,"legacy");
  const disabled=await createProviderTokenStore({client,vault,legacyReadsEnabled:false}).resolve({userId:"user-a",platform:"meta",legacyAccessToken:"legacy"});
  assert.equal(disabled.accessToken,null);
});

test("encrypted row never falls back after an envelope fails authentication",async()=>{
  const envelope=vault.encrypt("access",{userId:"user-a",platform:"google",tokenType:"access"});
  envelope.tag=Buffer.alloc(16).toString("base64");
  const store=createProviderTokenStore({client:fakeClient({access_token_envelope:envelope,refresh_token_envelope:null}),vault,legacyReadsEnabled:true});
  await assert.rejects(()=>store.resolve({userId:"user-a",platform:"google",legacyAccessToken:"legacy"}),error=>error.code==="TOKEN_DECRYPTION_FAILED");
});

test("disconnect cleanup deletes encrypted token envelopes",async()=>{
  const client=fakeClient({access_token_envelope:{},refresh_token_envelope:null});
  await createProviderTokenStore({client,vault}).remove({userId:"user-a",platform:"tiktok"});
  assert.equal(client.state.deletes,1);
  assert.equal(client.state.row,null);
});
