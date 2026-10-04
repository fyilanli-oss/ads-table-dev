"use strict";

const assert=require("node:assert/strict");
const fs=require("node:fs");
const path=require("node:path");
const {test}=require("node:test");
const {
  ProviderTokenRuntimePostureError,
  inspectProviderTokenRuntimePosture,
  assertProductionProviderTokenPosture,
}=require("../security/provider-token-runtime-posture");

const root=path.join(__dirname,"..");
const migration=fs.readFileSync(path.join(root,"supabase/migrations/20261004112000_a6_rm01_least_privilege_hardening.sql"),"utf8");
const preflight=fs.readFileSync(path.join(root,"docs/security/sql/A6_RM01_PRIVILEGE_PREFLIGHT.sql"),"utf8");
const postcheck=fs.readFileSync(path.join(root,"docs/security/sql/A6_RM01_PRIVILEGE_POSTCHECK.sql"),"utf8");
const contract=JSON.parse(fs.readFileSync(path.join(root,"contracts/a6-rm-01-database-crypto-hardening-v1.json"),"utf8"));
const server=fs.readFileSync(path.join(root,"server.js"),"utf8");

function secureEnv(){
  return {
    NODE_ENV:"production",
    SUPABASE_URL:"https://project.invalid",
    SUPABASE_SERVICE_ROLE_KEY:"service-role-secret",
    PROVIDER_TOKEN_ENCRYPTION_ENABLED:"true",
    PROVIDER_TOKEN_LEGACY_READ_ENABLED:"false",
    PROVIDER_TOKEN_ACTIVE_KEY_ID:"active",
    PROVIDER_TOKEN_ENCRYPTION_KEYS:JSON.stringify({
      active:Buffer.alloc(32,7).toString("base64"),
    }),
  };
}

test("production posture accepts encrypted-only server runtime without exposing values",()=>{
  const env=secureEnv();
  const result=inspectProviderTokenRuntimePosture(env);
  assert.equal(result.production,true);
  assert.equal(result.secure_posture,true);
  assert.equal(result.startup_allowed,true);
  assert.deepEqual(result.reason_codes,[]);
  const serialized=JSON.stringify(result);
  for(const value of [
    env.SUPABASE_URL,
    env.SUPABASE_SERVICE_ROLE_KEY,
    env.PROVIDER_TOKEN_ACTIVE_KEY_ID,
    env.PROVIDER_TOKEN_ENCRYPTION_KEYS,
  ])assert.equal(serialized.includes(value),false);
});

test("production posture fails closed for disabled encryption, legacy reads and invalid keyring",()=>{
  const secret="must-not-leak";
  const env={
    ...secureEnv(),
    PROVIDER_TOKEN_ENCRYPTION_ENABLED:"false",
    PROVIDER_TOKEN_LEGACY_READ_ENABLED:"true",
    PROVIDER_TOKEN_ENCRYPTION_KEYS:secret,
  };
  const result=inspectProviderTokenRuntimePosture(env);
  assert.equal(result.startup_allowed,false);
  assert.deepEqual(result.reason_codes,[
    "ENCRYPTION_NOT_ENABLED",
    "LEGACY_READS_ENABLED",
    "KEYRING_INVALID_OR_INCOMPLETE",
  ]);
  assert.equal(JSON.stringify(result).includes(secret),false);
  assert.throws(
    ()=>assertProductionProviderTokenPosture(env),
    error=>error instanceof ProviderTokenRuntimePostureError
      && error.code==="PROVIDER_TOKEN_RUNTIME_POSTURE_UNSAFE"
      && !String(error.message).includes(secret)
  );
});

test("non-production remains startable while reporting an insecure diagnostic posture",()=>{
  const result=inspectProviderTokenRuntimePosture({NODE_ENV:"test"});
  assert.equal(result.production,false);
  assert.equal(result.secure_posture,false);
  assert.equal(result.startup_allowed,true);
  assert.ok(result.reason_codes.includes("ENCRYPTION_FLAG_MISSING"));
});

test("migration removes only audited authority and preserves service-role trial execution",()=>{
  for(const role of ["postgres","supabase_admin"]){
    assert.match(migration,new RegExp("alter default privileges for role "+role+" in schema public[\\s\\S]*?revoke all privileges on tables from anon, authenticated","i"));
    assert.match(migration,new RegExp("alter default privileges for role "+role+" in schema public[\\s\\S]*?revoke execute on functions from public, anon, authenticated","i"));
  }
  for(const fn of ["expire_trials","handle_new_user","enforce_platform_account_limit_guard"]){
    assert.match(migration,new RegExp("revoke all privileges on function public\\."+fn+"\\(\\) from public, anon, authenticated","i"));
    assert.match(migration,new RegExp("alter function public\\."+fn+"\\(\\) set search_path = ''","i"));
  }
  assert.match(migration,/grant execute on function public\.expire_trials\(\) to service_role/i);
  assert.match(migration,/revoke truncate, references, trigger on table[\s\S]+from anon, authenticated/i);
  assert.doesNotMatch(migration,/revoke\s+(?:select|insert|update|delete)\s+on table/i);
});

test("preflight and postcheck are read-only and cover exact acceptance gates",()=>{
  for(const sql of [preflight,postcheck]){
    assert.doesNotMatch(sql,/^\\s*(?:insert|update|delete|truncate|alter|grant|revoke|drop|create)\\b/im);
  }
  for(const gate of ["function_external_execute","service_role_trial_execute","legacy_non_dml","mutable_search_path"]){
    assert.match(postcheck,new RegExp(gate));
  }
});

test("trial bridge is preserved but Shopify Billing remains E10-T7 authority",()=>{
  assert.equal(contract.trial_preservation.legacy_expiry_function_preserved,true);
  assert.equal(contract.trial_preservation.service_role_execute_required,true);
  assert.equal(contract.trial_preservation.shopify_trial_days,14);
  assert.equal(contract.trial_preservation.shopify_billing_owner,"E10-T7");
  assert.match(server,/supabaseAdmin\.rpc\("expire_trials"\)/);
  assert.match(server,/createSharedClients\(\{env:process\.env/);
});
