"use strict";

const assert=require("node:assert/strict");
const {spawn}=require("node:child_process");
const fs=require("node:fs");
const path=require("node:path");
const test=require("node:test");
const {ProductionConfigError,parseExplicitBoolean,isProductionRuntime,createRuntimeFlags,validateProductionConfig,reportProductionConfigFailure,loadProductionConfig}=require("../security/production-config");

const root=path.join(__dirname,"..");
const unsafeSource=/5252399301|5383556660|7654240828777955348|TikTok Test Advertiser|req\.query\.sandbox_access_token|\?sandbox_access_token=/;

test("review and sandbox switches default to disabled",()=>{
  const config=validateProductionConfig({NODE_ENV:"production"});
  assert.equal(config.googleReviewHardRouteEnabled,false);
  assert.equal(config.tiktokReviewFallbackEnabled,false);
  assert.equal(config.tiktokSandboxEnabled,false);
  assert.equal(config.tiktokTestPageEnabled,false);
});

test("TikTok V2 shadow is an explicit production-safe runtime flag",()=>{
  assert.equal(createRuntimeFlags({NODE_ENV:"production"}).tiktokV2ShadowEnabled,false);
  assert.equal(validateProductionConfig({NODE_ENV:"production",TIKTOK_V2_SHADOW_ENABLED:"true"}).tiktokV2ShadowEnabled,true);
  assert.throws(()=>createRuntimeFlags({TIKTOK_V2_SHADOW_ENABLED:"yes"}),error=>error instanceof ProductionConfigError&&error.variables.includes("TIKTOK_V2_SHADOW_ENABLED"));
});

test("boolean parsing is strict and production detection gives VERCEL_ENV precedence",()=>{
  for(const value of ["true","TRUE","1"])assert.equal(parseExplicitBoolean(value),true);
  for(const value of [undefined,"","false","FALSE","0"])assert.equal(parseExplicitBoolean(value),false);
  assert.throws(()=>parseExplicitBoolean("yes",false,"FLAG"),error=>error.code==="UNSAFE_PRODUCTION_CONFIG");
  assert.equal(isProductionRuntime({VERCEL_ENV:"preview",NODE_ENV:"production"}),false);
  assert.equal(isProductionRuntime({VERCEL_ENV:"production",NODE_ENV:"development"}),true);
  assert.equal(isProductionRuntime({NODE_ENV:"production"}),true);
});

test("production rejects incomplete review bridge or unsafe sandbox settings without exposing values",()=>{
  for(const key of ["GOOGLE_REVIEW_HARD_ROUTE_ENABLED","GOOGLE_TEST_CUSTOMER_ID","GOOGLE_TEST_LOGIN_CUSTOMER_ID","TIKTOK_REVIEW_ADVERTISER_ID","TIKTOK_REVIEW_ADVERTISER_NAME","TIKTOK_REVIEW_ACCESS_TOKEN","TIKTOK_SANDBOX_ENABLED","TIKTOK_SANDBOX_ACCESS_TOKEN","TIKTOK_SANDBOX_ADVERTISER_ID","TIKTOK_SANDBOX_ADVERTISER_NAME","TIKTOK_TEST_ACCESS_TOKEN","TIKTOK_FORCE_SANDBOX_REPORTS"]){
    const secret="do-not-print-this-value";
    assert.throws(
      ()=>validateProductionConfig({NODE_ENV:"production",[key]:key.endsWith("ENABLED")||key==="TIKTOK_FORCE_SANDBOX_REPORTS"?"true":secret}),
      error=>error.code==="UNSAFE_PRODUCTION_CONFIG"&&error.message.includes(key)&&!error.message.includes(secret)
    );
  }
  assert.throws(()=>validateProductionConfig({NODE_ENV:"production",TIKTOK_REVIEW_FALLBACK_ENABLED:"true"}),error=>error.variables.join(",")==="TIKTOK_REVIEW_ACCESS_TOKEN,TIKTOK_REVIEW_ADVERTISER_ID");
});

test("production review bridge requires the explicit flag, advertiser and server-only token",()=>{
  const env={VERCEL_ENV:"production",TIKTOK_REVIEW_FALLBACK_ENABLED:"true",TIKTOK_REVIEW_ADVERTISER_ID:"review-advertiser",TIKTOK_REVIEW_ACCESS_TOKEN:"server-secret"};
  assert.equal(validateProductionConfig(env).tiktokReviewFallbackEnabled,true);
  assert.throws(()=>validateProductionConfig({...env,TIKTOK_REVIEW_ADVERTISER_ID:""}),error=>error.variables.includes("TIKTOK_REVIEW_ADVERTISER_ID")&&!error.message.includes("server-secret"));
  assert.throws(()=>validateProductionConfig({...env,TIKTOK_REVIEW_ACCESS_TOKEN:""}),error=>error.variables.includes("TIKTOK_REVIEW_ACCESS_TOKEN"));
});

test("unsafe production variables are reported in deterministic order",()=>{
  assert.throws(
    ()=>validateProductionConfig({NODE_ENV:"production",TIKTOK_SANDBOX_ACCESS_TOKEN:"secret",GOOGLE_REVIEW_HARD_ROUTE_ENABLED:"true"}),
    error=>error.variables.join(",")==="GOOGLE_REVIEW_HARD_ROUTE_ENABLED,TIKTOK_SANDBOX_ACCESS_TOKEN"&&!error.message.includes("secret")
  );
});

test("safe startup does not emit a production config diagnostic",()=>{
  const calls=[];
  assert.equal(loadProductionConfig({NODE_ENV:"production"},{error:value=>calls.push(value)}).production,true);
  assert.deepEqual(calls,[]);
});

test("unsafe startup emits one allowlisted, deterministic, secret-free JSON diagnostic and rethrows",()=>{
  const secret="super-secret-value-that-must-not-appear";
  const env={NODE_ENV:"production",TIKTOK_SANDBOX_ACCESS_TOKEN:secret,GOOGLE_TEST_CUSTOMER_ID:secret};
  const calls=[];
  let thrown;
  try{loadProductionConfig(env,{error:value=>calls.push(value)});}catch(error){thrown=error;}
  assert.ok(thrown instanceof ProductionConfigError);
  assert.equal(calls.length,1);
  assert.equal(calls[0].includes("\n"),false);
  assert.equal(calls[0].includes(secret),false);
  const diagnostic=JSON.parse(calls[0]);
  assert.deepEqual(Object.keys(diagnostic),["event","code","variables"]);
  assert.deepEqual(diagnostic,{
    event:"PRODUCTION_CONFIG_REJECTED",
    code:"UNSAFE_PRODUCTION_CONFIG",
    variables:["GOOGLE_TEST_CUSTOMER_ID","TIKTOK_SANDBOX_ACCESS_TOKEN"]
  });
  assert.strictEqual(thrown.variables.join(","),diagnostic.variables.join(","));
});

test("diagnostic deduplicates known names and omits unknown names without logging the error object",()=>{
  const error=new ProductionConfigError("message-without-sensitive-data",[
    "TIKTOK_SANDBOX_ENABLED","UNKNOWN_SECRET_NAME","GOOGLE_TEST_CUSTOMER_ID","TIKTOK_SANDBOX_ENABLED"
  ]);
  error.stack="stack-that-must-not-appear";
  error.metadata={env:{SECRET:"value-that-must-not-appear"}};
  const calls=[];
  assert.equal(reportProductionConfigFailure(error,{error:(...args)=>calls.push(args)}),true);
  assert.equal(calls.length,1);
  assert.equal(calls[0].length,1);
  assert.equal(typeof calls[0][0],"string");
  assert.deepEqual(JSON.parse(calls[0][0]).variables,["GOOGLE_TEST_CUSTOMER_ID","TIKTOK_SANDBOX_ENABLED"]);
  assert.doesNotMatch(calls[0][0],/UNKNOWN|stack|metadata|value-that-must-not-appear/);
});

test("diagnostic accepts only the production config error contract",()=>{
  const calls=[];
  const logger={error:value=>calls.push(value)};
  assert.equal(reportProductionConfigFailure(new Error("ordinary error"),logger),false);
  const wrongCode=new ProductionConfigError("wrong code",["GOOGLE_TEST_CUSTOMER_ID"]);
  wrongCode.code="OTHER_CODE";
  assert.equal(reportProductionConfigFailure(wrongCode,logger),false);
  assert.deepEqual(calls,[]);
});

test("logger failure cannot bypass unsafe startup fail-fast behavior",()=>{
  const expectedSecret="not-logged-even-when-logger-fails";
  let thrown;
  try{
    loadProductionConfig(
      {NODE_ENV:"production",TIKTOK_TEST_ACCESS_TOKEN:expectedSecret},
      {error:()=>{throw new Error("logger unavailable");}}
    );
  }catch(error){thrown=error;}
  assert.ok(thrown instanceof ProductionConfigError);
  assert.deepEqual(thrown.variables,["TIKTOK_TEST_ACCESS_TOKEN"]);
});

test("non-production review modes require complete explicit configuration",()=>{
  assert.throws(()=>validateProductionConfig({NODE_ENV:"development",GOOGLE_REVIEW_HARD_ROUTE_ENABLED:"true"}),/GOOGLE_TEST_CUSTOMER_ID/);
  assert.doesNotThrow(()=>validateProductionConfig({NODE_ENV:"development",GOOGLE_REVIEW_HARD_ROUTE_ENABLED:"true",GOOGLE_TEST_CUSTOMER_ID:"111",GOOGLE_TEST_LOGIN_CUSTOMER_ID:"222"}));
  assert.throws(()=>validateProductionConfig({NODE_ENV:"development",TIKTOK_REVIEW_FALLBACK_ENABLED:"true"}),/TIKTOK_REVIEW_ADVERTISER_ID/);
  assert.doesNotThrow(()=>validateProductionConfig({NODE_ENV:"development",TIKTOK_REVIEW_FALLBACK_ENABLED:"true",TIKTOK_REVIEW_ADVERTISER_ID:"111",TIKTOK_REVIEW_ACCESS_TOKEN:"secret"}));
  assert.throws(()=>validateProductionConfig({NODE_ENV:"development",TIKTOK_FORCE_SANDBOX_REPORTS:"true"}),/TIKTOK_SANDBOX_ENABLED/);
  assert.throws(()=>validateProductionConfig({VERCEL_ENV:"preview",TIKTOK_SANDBOX_ENABLED:"true",TIKTOK_FORCE_SANDBOX_REPORTS:"true"}),/TIKTOK_SANDBOX_ACCESS_TOKEN/);
  assert.doesNotThrow(()=>validateProductionConfig({VERCEL_ENV:"preview",TIKTOK_SANDBOX_ENABLED:"true",TIKTOK_FORCE_SANDBOX_REPORTS:"true",TIKTOK_SANDBOX_ACCESS_TOKEN:"secret",TIKTOK_SANDBOX_ADVERTISER_ID:"111"}));
});

test("the test page requires explicit non-production sandbox mode",()=>{
  assert.equal(createRuntimeFlags({NODE_ENV:"development"}).tiktokTestPageEnabled,false);
  assert.equal(createRuntimeFlags({NODE_ENV:"development",TIKTOK_SANDBOX_ENABLED:"true"}).tiktokTestPageEnabled,true);
  assert.equal(createRuntimeFlags({NODE_ENV:"production",TIKTOK_SANDBOX_ENABLED:"true"}).tiktokTestPageEnabled,false);
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

test("TikTok endpoints reject the sandbox token query parameter before reading a token",()=>{
  const source=fs.readFileSync(path.join(root,"server.js"),"utf8");
  const rejection=/hasOwnProperty\.call\(req\.query,"sandbox_access_token"\).*status\(400\)/g;
  assert.equal((source.match(rejection)||[]).length,2);
  assert.doesNotMatch(source,/req\.query\.sandbox_access_token/);
});

async function routeStatus(env){
  const child=spawn(process.execPath,["-e",`const app=require('./server');const server=app.listen(0,'127.0.0.1',()=>console.log('READY '+server.address().port))`],{
    cwd:root,env:{...process.env,VERCEL:"1",...env},stdio:["ignore","pipe","pipe"]
  });
  let stderr="";
  let stdout="";
  child.stderr.on("data",chunk=>{stderr+=chunk});
  let port;
  try{
    await new Promise((resolve,reject)=>{
      // The legacy monolith imports all provider SDKs before it can emit READY.
      // Keep this characterization tolerant of cold/contended CI startup while
      // retaining a bounded fail-closed timeout.
      const timer=setTimeout(()=>reject(new Error(`server timeout: ${stderr}`)),20000);
      child.stdout.on("data",chunk=>{
        stdout+=chunk;
        const match=stdout.match(/READY (\d+)/);
        if(match){port=Number(match[1]);clearTimeout(timer);resolve();}
      });
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
