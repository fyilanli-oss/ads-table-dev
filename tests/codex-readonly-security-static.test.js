"use strict";

const assert=require("node:assert/strict");
const fs=require("node:fs");
const path=require("node:path");
const test=require("node:test");

const root=path.join(__dirname,"..");
const gatewayRuntimeFiles=[
  "security/codex-readonly-auth.js",
  "security/codex-readonly-contract.js",
  "services/codex-readonly-service.js",
  "routes/codex-readonly-routes.js"
];

function runtimeSource(){return gatewayRuntimeFiles.map(file=>fs.readFileSync(path.join(root,file),"utf8")).join("\n");}

test("gateway runtime has no Management API, personal access token, CLI session, or local session artifact channel",()=>{
  const source=runtimeSource();
  const managementCredential=["SUPABASE","ACCESS","TOKEN"].join("_");
  const managementQuery=["database","query"].join("/");
  assert.equal(source.includes(managementCredential),false);
  assert.equal(source.includes(`/v1/projects/`),false);
  assert.equal(source.includes(`/${managementQuery}`),false);
  assert.doesNotMatch(source,/supabase\/\.temp|linked-project|project-ref|personal[ _-]?access|\.supabase|homedir\s*\(/i);
});

test("gateway runtime exposes only fixed SELECT/count and contains no mutation or RPC call",()=>{
  const source=runtimeSource();
  const serviceSource=fs.readFileSync(path.join(root,"services/codex-readonly-service.js"),"utf8");
  assert.match(source,/\.select\("id",\{count:"exact",head:true\}\)/);
  assert.doesNotMatch(serviceSource,/\.(?:insert|update|upsert|delete|rpc)\s*\(/i);
  assert.doesNotMatch(source,/\b(?:alter|create|drop|truncate)\s+(?:table|role|function|index)\b/i);
});
