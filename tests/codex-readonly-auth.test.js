"use strict";

const assert=require("node:assert/strict");
const test=require("node:test");
const crypto=require("node:crypto");
const {MIN_TOKEN_BYTES,configuredToken,extractBearer,constantTimeTokenEqual,createCodexReadonlyAuth}=require("../security/codex-readonly-auth");

test("gateway token configuration requires at least 32 UTF-8 bytes",()=>{
  assert.equal(MIN_TOKEN_BYTES,32);
  assert.equal(configuredToken("a".repeat(31)),false);
  assert.equal(configuredToken("a".repeat(32)),true);
});

test("Bearer parsing is exact and missing, malformed, and invalid credentials share denial",()=>{
  const token=crypto.randomBytes(32).toString("base64url");
  const auth=createCodexReadonlyAuth(token);
  assert.equal(extractBearer(`Bearer ${token}`),token);
  for(const header of [undefined,"",token,`bearer ${token}`,`Bearer  ${token}`,"Bearer wrong"]){
    assert.deepEqual(auth.authenticate(header),{ok:false,configurationUnavailable:false});
  }
  assert.deepEqual(auth.authenticate(`Bearer ${token}`),{ok:true,configurationUnavailable:false});
});

test("token comparison delegates equal-length digests to timingSafeEqual",t=>{
  let calls=0;
  const original=crypto.timingSafeEqual;
  crypto.timingSafeEqual=(left,right)=>{calls+=1;assert.equal(left.length,32);assert.equal(right.length,32);return original(left,right);};
  t.after(()=>{crypto.timingSafeEqual=original;});
  assert.equal(constantTimeTokenEqual("short","a-different-length-value"),false);
  assert.equal(constantTimeTokenEqual("same","same"),true);
  assert.equal(calls,2);
});

test("missing or undersized server token is fail closed",()=>{
  for(const token of [undefined,"", "x".repeat(31)])assert.deepEqual(createCodexReadonlyAuth(token).authenticate("Bearer anything"),{ok:false,configurationUnavailable:true});
});
