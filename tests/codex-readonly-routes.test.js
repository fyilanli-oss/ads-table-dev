"use strict";

const assert=require("node:assert/strict");
const http=require("node:http");
const test=require("node:test");
const express=require("express");
const {createCodexReadonlyAuth}=require("../security/codex-readonly-auth");
const {createRateLimiter,createCodexReadonlyRouter}=require("../routes/codex-readonly-routes");

const token="T".repeat(32);
async function fixture({auth=createCodexReadonlyAuth(token),service={execute:async()=>({datasetV2Rows:1})},rateLimiter,logger}={}){
  const app=express();
  app.use("/api/internal/codex-readonly",createCodexReadonlyRouter({auth,service,rateLimiter,logger:logger||{info(){}}}));
  const server=http.createServer(app);
  await new Promise(resolve=>server.listen(0,"127.0.0.1",resolve));
  return {url:`http://127.0.0.1:${server.address().port}/api/internal/codex-readonly`,close:()=>new Promise(resolve=>server.close(resolve))};
}

test("route authentication, allowlist, query rejection, no-store, request ID, and no CORS are enforced",async t=>{
  const target=await fixture();t.after(target.close);
  for(const header of [undefined,"Bearer wrong","Basic anything",`Bearer  ${token}`]){
    const response=await fetch(`${target.url}/dataset-v2-safe-counts`,{headers:header?{Authorization:header}:{}});
    assert.equal(response.status,401);assert.deepEqual(Object.keys(await response.json()).sort(),["error","requestId"]);
  }
  const headers={Authorization:`Bearer ${token}`,"X-Request-ID":"test-request"};
  const ok=await fetch(`${target.url}/dataset-v2-safe-counts`,{headers});
  assert.equal(ok.status,200);assert.equal(ok.headers.get("cache-control"),"no-store");assert.equal(ok.headers.get("x-request-id"),"test-request");assert.equal(ok.headers.get("access-control-allow-origin"),null);
  assert.equal((await fetch(`${target.url}/dataset-v2-safe-counts?sql=select&table=users&column=email`,{headers})).status,400);
  assert.equal((await fetch(`${target.url}/unknown`,{headers})).status,404);
  assert.equal((await fetch(`${target.url}/dataset-v2-safe-counts`,{method:"POST",headers})).status,404);
});

test("missing server credential returns safe 503",async t=>{
  const target=await fixture({auth:createCodexReadonlyAuth(undefined)});t.after(target.close);
  const response=await fetch(`${target.url}/health`);
  assert.equal(response.status,503);assert.match(await response.text(),/Service unavailable/);
});

test("rate limit returns safe 429 and resets by window",async t=>{
  let time=0;const limiter=createRateLimiter({limit:1,windowMs:100,now:()=>time});
  const target=await fixture({rateLimiter:limiter});t.after(target.close);
  const headers={Authorization:`Bearer ${token}`};
  assert.equal((await fetch(`${target.url}/dataset-v2-safe-counts`,{headers})).status,200);
  assert.equal((await fetch(`${target.url}/dataset-v2-safe-counts`,{headers})).status,429);
  time=100;assert.equal((await fetch(`${target.url}/dataset-v2-safe-counts`,{headers})).status,200);
});

test("audit logging contains only safe structured fields and errors never leak",async t=>{
  const credential="S".repeat(32),serviceRole="service-role-do-not-log",rawError=`SQL failed ${serviceRole}`;const logs=[];
  const target=await fixture({auth:createCodexReadonlyAuth(credential),service:{execute:async()=>{throw new Error(rawError);}},logger:{info:value=>logs.push(value)}});t.after(target.close);
  const response=await fetch(`${target.url}/health`,{headers:{Authorization:`Bearer ${credential}`}});
  assert.equal(response.status,502);
  const body=await response.text();const output=body+logs.join("");
  assert.doesNotMatch(output,new RegExp(`${credential}|${serviceRole}|SQL failed|Authorization`));
  assert.deepEqual(Object.keys(JSON.parse(logs[0])).sort(),["contractVersion","durationMs","operation","rateLimitResult","requestId","status"]);
});

test("server composition registers only thin gateway delegation",()=>{
  const source=require("node:fs").readFileSync(require("node:path").join(__dirname,"..","server.js"),"utf8");
  assert.match(source,/app\.use\("\/api\/internal\/codex-readonly",createCodexReadonlyRouter/);
  assert.equal((source.match(/codex-readonly/g)||[]).length,4);
});
