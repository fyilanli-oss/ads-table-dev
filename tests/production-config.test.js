"use strict";

const assert=require("node:assert/strict");
const {spawn}=require("node:child_process");
const fs=require("node:fs");
const path=require("node:path");
const test=require("node:test");
const {createProductionConfig,assertSafeProductionConfig}=require("../security/production-config");

const root=path.join(__dirname,"..");
const unsafeSource=/5252399301|5383556660|7654240828777955348|TikTok Test Advertiser|req\.query\.sandbox_access_token|\?sandbox_access_token=/;

test("review and sandbox switches default to disabled",()=>{
  const config=createProductionConfig({NODE_ENV:"production"});
  assert.equal(config.googleReviewHardRouteEnabled,false);
  assert.equal(config.tiktokReviewFallbackEnabled,false);
  assert.equal(config.tiktokSandboxEnabled,false);
  assert.equal(config.tiktokTestPageEnabled,false);
  assert.doesNotThrow(()=>assertSafeProductionConfig(config));
});

test("production rejects every unsafe review or sandbox switch without exposing values",()=>{
  for(const key of ["GOOGLE_REVIEW_HARD_ROUTE_ENABLED","TIKTOK_REVIEW_FALLBACK_ENABLED","TIKTOK_SANDBOX_ENABLED"]){
    const secret="do-not-print-this-value";
    assert.throws(
      ()=>assertSafeProductionConfig(createProductionConfig({NODE_ENV:"production",[key]:"true",TIKTOK_REVIEW_ADVERTISER_ID:secret})),
      error=>error.message.startsWith("Unsafe production configuration:")&&!error.message.includes(secret)
    );
  }
});

test("the test page requires explicit non-production sandbox mode",()=>{
  assert.equal(createProductionConfig({NODE_ENV:"development"}).tiktokTestPageEnabled,false);
  assert.equal(createProductionConfig({NODE_ENV:"development",TIKTOK_SANDBOX_ENABLED:"true"}).tiktokTestPageEnabled,true);
  assert.equal(createProductionConfig({NODE_ENV:"production",TIKTOK_SANDBOX_ENABLED:"true"}).tiktokTestPageEnabled,false);
});

test("runtime and UI sources contain no known unsafe IDs or sandbox token query transport",()=>{
  const files=["server.js","security/production-config.js",...fs.readdirSync(path.join(root,"public")).filter(name=>name.endsWith(".html")).map(name=>`public/${name}`)];
  for(const file of files)assert.doesNotMatch(fs.readFileSync(path.join(root,file),"utf8"),unsafeSource,file);
});

test("TikTok test page has safe UI defaults and header-only sandbox token transport",()=>{
  const source=fs.readFileSync(path.join(root,"public/tiktok-test.html"),"utf8");
  assert.match(source,/id="sandbox" type="checkbox"\/>/);
  assert.match(source,/id="advertiserId"[^>]*value=""/);
  assert.match(source,/headers\["X-Sandbox-Access-Token"\]=sandboxToken/g);
  assert.doesNotMatch(source,/sandbox_access_token/);
});

async function routeStatus(env){
  const port=32000+Math.floor(Math.random()*10000);
  const child=spawn(process.execPath,["-e",`const app=require('./server');app.listen(${port},()=>console.log('READY'))`],{
    cwd:root,env:{...process.env,VERCEL:"1",...env},stdio:["ignore","pipe","pipe"]
  });
  let stderr="";
  child.stderr.on("data",chunk=>{stderr+=chunk});
  try{
    await new Promise((resolve,reject)=>{
      const timer=setTimeout(()=>reject(new Error(`server timeout: ${stderr}`)),5000);
      child.stdout.on("data",chunk=>{if(chunk.toString().includes("READY")){clearTimeout(timer);resolve();}});
      child.once("exit",code=>{clearTimeout(timer);reject(new Error(`server exited ${code}: ${stderr}`));});
    });
    const route=(await fetch(`http://127.0.0.1:${port}/tiktok-test`)).status;
    const staticFile=(await fetch(`http://127.0.0.1:${port}/tiktok-test.html`)).status;
    return {route,staticFile};
  }finally{
    child.kill();
  }
}

test("TikTok test-page route follows the production/development matrix",async()=>{
  assert.deepEqual(await routeStatus({NODE_ENV:"production"}),{route:404,staticFile:404});
  assert.deepEqual(await routeStatus({NODE_ENV:"development",TIKTOK_SANDBOX_ENABLED:"false"}),{route:404,staticFile:404});
  assert.deepEqual(await routeStatus({NODE_ENV:"development",TIKTOK_SANDBOX_ENABLED:"true"}),{route:200,staticFile:200});
});
