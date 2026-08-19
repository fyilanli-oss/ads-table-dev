"use strict";

const test=require("node:test");
const assert=require("node:assert/strict");
const fs=require("node:fs");
const path=require("node:path");

const root=path.join(__dirname,"..");
const workflowPath=path.join(root,".github/workflows/provider-token-production-dry-run.yml");
const workflow=fs.readFileSync(workflowPath,"utf8");
const pkg=require("../package.json");
const lock=require("../package-lock.json");

function section(start,end){
  const from=workflow.indexOf(start);
  const to=end?workflow.indexOf(end,from):workflow.length;
  assert.notEqual(from,-1,`missing section: ${start}`);
  assert.notEqual(to,-1,`missing section end: ${end}`);
  return workflow.slice(from,to);
}

test("workflow exists and exposes only the manual dry-run inputs",()=>{
  assert.equal(fs.existsSync(workflowPath),true);
  const trigger=section("on:\n","\npermissions:");
  assert.match(trigger,/^on:\n  workflow_dispatch:/);
  for(const forbidden of ["push:","pull_request:","schedule:","workflow_call:"])assert.doesNotMatch(trigger,new RegExp(`\\b${forbidden}`));
  assert.match(trigger,/batch_size:[\s\S]*default: "25"[\s\S]*type: choice/);
  for(const value of ["1","10","25","50","100"])assert.match(trigger,new RegExp(`          - "${value}"`));
  assert.match(trigger,/cursor:[\s\S]*required: false[\s\S]*default: ""[\s\S]*type: string/);
  for(const input of ["write","execute","apply","force","confirm_write","mode","operation","encryption_enabled","legacy_read_enabled"])assert.doesNotMatch(trigger,new RegExp(`^      ${input}:`,"m"));
});

test("branch, environment, permissions, and concurrency fail closed",()=>{
  assert.match(workflow,/permissions:\n  contents: read/);
  assert.doesNotMatch(workflow,/permissions:[\s\S]*?\b(?:contents|actions|checks|deployments|id-token|issues|pull-requests|packages|security-events): write/);
  assert.match(workflow,/group: provider-token-production-dry-run\n  cancel-in-progress: false/);
  assert.match(workflow,/if \[\[ "\$\{GITHUB_REF\}" != "refs\/heads\/main" \]\]; then[\s\S]*exit 1/);
  const production=section("  production-dry-run:");
  assert.match(production,/needs: validation/);
  assert.match(production,/if: github\.ref == 'refs\/heads\/main'/);
  assert.match(production,/environment: production-token-backfill/);
});

test("official actions are immutable and checkout does not retain credentials",()=>{
  const references=[...workflow.matchAll(/^\s+uses:\s+([^\s#]+)/gm)].map(match=>match[1]);
  assert.ok(references.length>=2);
  for(const reference of references){
    assert.match(reference,/^actions\/(?:checkout|setup-node)@[a-f0-9]{40}$/);
    assert.doesNotMatch(reference,/@(?:main|master|v\d+(?:\.\d+)*)$/);
  }
  const checkoutCount=(workflow.match(/persist-credentials: false/g)||[]).length;
  assert.equal(checkoutCount,2);
  assert.equal((workflow.match(/fetch-depth: 1/g)||[]).length,2);
});

test("installs are locked and both jobs use the same pinned Node release",()=>{
  assert.equal((workflow.match(/npm ci --ignore-scripts --no-audit --no-fund/g)||[]).length,2);
  assert.doesNotMatch(workflow,/\bnpm install\b/);
  assert.equal((workflow.match(/node-version: "22\.18\.0"/g)||[]).length,2);
  assert.equal(lock.lockfileVersion,3);
  assert.deepEqual(lock.packages[""].dependencies,pkg.dependencies);
});

test("production values are scoped only to the operator step",()=>{
  const operatorStep=section("      - name: Run dry-run operator");
  const beforeOperator=workflow.slice(0,workflow.indexOf("      - name: Run dry-run operator"));
  const required=["SUPABASE_URL","SUPABASE_PROJECT_REF","PROVIDER_TOKEN_ACTIVE_KEY_ID","SUPABASE_SERVICE_ROLE_KEY","PROVIDER_TOKEN_ENCRYPTION_KEYS","PROVIDER_TOKEN_BACKFILL_REFERENCE_SECRET"];
  for(const name of required){
    assert.match(operatorStep,new RegExp(`^          ${name}:`,"m"));
    assert.doesNotMatch(beforeOperator,new RegExp(`^\\s+${name}:`,"m"));
    assert.equal((workflow.match(new RegExp(`^\\s+${name}:`,"gm"))||[]).length,1);
  }
  for(const forbidden of ["SUPABASE_DB_PASSWORD","SUPABASE_ACCESS_TOKEN","SUPABASE_ANON_KEY","PROVIDER_TOKEN_ENCRYPTION_ENABLED","PROVIDER_TOKEN_LEGACY_READ_ENABLED"])assert.doesNotMatch(workflow,new RegExp(forbidden));
});

test("operator invocation remains quoted, direct, dry-run-only, and non-reporting",()=>{
  const operatorStep=section("      - name: Run dry-run operator");
  assert.match(operatorStep,/set -euo pipefail/);
  assert.match(operatorStep,/args=\(--batch-size "\$\{BATCH_SIZE\}"\)/);
  assert.match(operatorStep,/args\+=\(--cursor "\$\{CURSOR\}"\)/);
  assert.match(operatorStep,/node scripts\/provider-token-backfill-dry-run\.js "\$\{args\[@\]\}"/);
  assert.doesNotMatch(operatorStep,/\b(?:--write|--execute|--apply)\b|dry-run=false|set -x|\beval\b|bash -c|\becho\b/);
  assert.doesNotMatch(workflow,/upload-artifact|GITHUB_STEP_SUMMARY|server\.js|supabase\s+(?:db|migration)|vercel\s+(?:deploy|--prod)/i);
  assert.doesNotMatch(workflow,/^(?:run-name|\s+name):.*\$\{\{\s*inputs\.cursor/m);
  assert.equal(pkg.scripts["tokens:backfill:dry-run"],"node scripts/provider-token-backfill-dry-run.js");
  assert.doesNotMatch(pkg.scripts["tokens:backfill:dry-run"],/write|execute|apply/);
});

test("workflow contains names and contexts, never credential-like literals",()=>{
  assert.doesNotMatch(workflow,/https?:\/\/|eyJ[A-Za-z0-9_-]{20,}|[A-Za-z0-9+/]{43}=/);
  assert.doesNotMatch(workflow,/SUPABASE_(?:SERVICE_ROLE_KEY|URL):\s+(?!\$\{\{)/);
  assert.doesNotMatch(workflow,/PROVIDER_TOKEN_(?:ENCRYPTION_KEYS|BACKFILL_REFERENCE_SECRET):\s+(?!\$\{\{)/);
});
