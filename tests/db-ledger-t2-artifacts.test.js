"use strict";

const assert=require("node:assert/strict");
const crypto=require("node:crypto");
const fs=require("node:fs");
const path=require("node:path");
const test=require("node:test");

const root=path.join(__dirname,"..");
const paths={
  migration:"supabase/migrations/20260824120000_harden_oauth_transactions_service_role_grants.sql",
  preflight:"docs/security/sql/DB_LEDGER_T2_PREFLIGHT.sql",
  reconciliation:"docs/security/sql/DB_LEDGER_T2_RECONCILIATION.sql",
  acceptance:"docs/security/sql/DB_LEDGER_T2_ACCEPTANCE.sql",
  runbook:"docs/security/DB_LEDGER_T2_RUNBOOK.md",
  manifest:"docs/security/evidence/DB_LEDGER_T2_BASELINE_MANIFEST.json",
};
const read=(key)=>fs.readFileSync(path.join(root,paths[key]),"utf8");
const artifact=Object.fromEntries(Object.keys(paths).map((key)=>[key,read(key)]));
const stripComments=(sql)=>sql.replace(/^\s*--.*$/gm,"").trim();
const versions=["20260818090000","20260818120000","20260819120000","20260824120000"];
const names=["create_oauth_transactions","create_platform_connection_tokens","harden_platform_connection_tokens_service_role_grants","harden_oauth_transactions_service_role_grants"];

function statements(sql){
  return stripComments(sql).split(";").map((value)=>value.trim()).filter(Boolean);
}

test("DB-LEDGER-T2 artifacts exist in their intended repository surfaces",()=>{
  for(const file of Object.values(paths)) assert.equal(fs.existsSync(path.join(root,file)),true,file);
  assert.equal(paths.reconciliation.startsWith("supabase/migrations/"),false);
  assert.equal(paths.preflight.startsWith("supabase/migrations/"),false);
  assert.equal(paths.acceptance.startsWith("supabase/migrations/"),false);
});

test("corrective migration contains only the OAuth grant and enabled-RLS allowlist",()=>{
  const actual=statements(artifact.migration).map((value)=>value.replace(/\s+/g," ").toLowerCase());
  assert.deepEqual(actual,[
    "revoke all privileges on table public.oauth_transactions from service_role",
    "grant select, insert, delete on table public.oauth_transactions to service_role",
    "revoke all privileges on table public.oauth_transactions from public",
    "revoke all privileges on table public.oauth_transactions from anon",
    "revoke all privileges on table public.oauth_transactions from authenticated",
    "alter table public.oauth_transactions enable row level security",
  ]);
  assert.doesNotMatch(artifact.migration,/platform_connection_tokens/i);
  assert.doesNotMatch(artifact.migration,/\b(?:create|drop|update|insert into|delete from|truncate|force row level security|policy|function|constraint|index)\b/i);
});

for(const key of ["preflight","acceptance"]){
  test(`${key} is a single read-only WITH query`,()=>{
    const sql=stripComments(artifact[key]);
    assert.match(sql,/^with\b/i);
    assert.equal(statements(sql).length,1);
    const withoutLiterals=sql.replace(/'(?:''|[^'])*'/g,"''");
    assert.doesNotMatch(withoutLiterals,/(?:^|[;(])\s*(?:insert|update|delete|truncate|alter|create|drop|grant|revoke|comment|vacuum|analyze|refresh|call|do|copy|merge|lock)\b/i);
    assert.doesNotMatch(sql,/\b(?:access_token_envelope|refresh_token_envelope)\s*(?:->|#>)/i);
    assert.match(sql,/select check_code, actual_count, expected_count, actual_count=expected_count as passed/i);
  });
}

test("preflight fail-closes on the exact ledger, object, grant, and aggregate contract",()=>{
  for(const value of [...versions,...names]) assert.match(artifact.preflight,new RegExp(value));
  for(const check of ["TARGET_LEDGER_ROWS","TARGET_VERSION_WRONG_NAME","OAUTH_COLUMN_DRIFT","TOKEN_COLUMN_DRIFT","OAUTH_FUNCTION_SIGNATURES","OAUTH_SERVICE_PRIVILEGES","TOKEN_SERVICE_PRIVILEGES","OAUTH_ROWS","CONNECTED_CONNECTIONS","ENCRYPTED_TOKEN_ROWS","MISSING_ENCRYPTED","PLAINTEXT_ACCESS","PLAINTEXT_REFRESH"]){
    assert.match(artifact.preflight,new RegExp(check));
  }
});

test("reconciliation is one transaction limited to OAuth grants and four ledger inserts",()=>{
  assert.match(artifact.reconciliation,/^\s*--[\s\S]*?begin;/i);
  assert.match(artifact.reconciliation,/commit;\s*$/i);
  assert.equal((artifact.reconciliation.match(/^begin;/gmi)||[]).length,1);
  assert.equal((artifact.reconciliation.match(/^commit;/gmi)||[]).length,1);
  for(const value of [...versions,...names]) assert.match(artifact.reconciliation,new RegExp(value));
  assert.match(artifact.reconciliation,/get diagnostics affected_count = row_count;[\s\S]*affected_count<>4/i);
  assert.match(artifact.reconciliation,/DB_LEDGER_T2_PRECONDITION_[A-Z_]+/);
  assert.match(artifact.reconciliation,/DB_LEDGER_T2_POSTCONDITION_[A-Z_]+/);
  const inserts=[...artifact.reconciliation.matchAll(/\binsert into\s+([\w.]+)/gi)].map((match)=>match[1]);
  assert.deepEqual(inserts,["supabase_migrations.schema_migrations"]);
  const changedRelations=[...artifact.reconciliation.matchAll(/\b(?:alter table|grant[\s\S]*?on table|revoke[\s\S]*?on table)\s+(public\.[a-z_]+)/gi)].map((match)=>match[1]);
  assert.deepEqual([...new Set(changedRelations)],["public.oauth_transactions"]);
  const reconciliationWithoutLiterals=artifact.reconciliation.replace(/'(?:''|[^'])*'/g,"''");
  assert.doesNotMatch(reconciliationWithoutLiterals,/\b(?:update|delete from|truncate|create table|drop table|create or replace function|platform_connection_tokens\s+(?:enable|force)|alter table public\.platform_connection_tokens)\b/i);
});

test("reconciliation does not mutate any historical ledger version",()=>{
  const manifest=JSON.parse(artifact.manifest);
  const historical=manifest.historical_ledger.map((row)=>row.version);
  assert.equal(historical.length,31);
  for(const version of historical) assert.doesNotMatch(artifact.reconciliation,new RegExp(`['"]${version}['"]`));
});

test("acceptance requires the exact final grant and unchanged security postconditions",()=>{
  for(const check of ["TARGET_LEDGER_EXACT","TARGET_VERSION_DUPLICATES","OAUTH_SERVICE_PRIVILEGES","OAUTH_SERVICE_EXTRA_PRIVILEGES","OAUTH_NON_SERVICE_PRIVILEGES","OAUTH_FUNCTION_SERVICE_EXECUTE","OAUTH_FUNCTION_UNEXPECTED_EXECUTE","TOKEN_SERVICE_PRIVILEGES","TOKEN_SERVICE_EXTRA_PRIVILEGES","OAUTH_ROWS","CONNECTED_CONNECTIONS","ENCRYPTED_TOKEN_ROWS","MISSING_ENCRYPTED","PLAINTEXT_ACCESS","PLAINTEXT_REFRESH"]){
    assert.match(artifact.acceptance,new RegExp(check));
  }
  assert.match(artifact.acceptance,/privilege_type in \('SELECT','INSERT','DELETE'\)/);
  assert.match(artifact.acceptance,/privilege_type not in \('SELECT','INSERT','DELETE'\)/);
});

test("runbook defines stop gates and least-privilege-preserving rollback",()=>{
  assert.match(artifact.runbook,/must not recreate application objects/i);
  assert.match(artifact.runbook,/never pass it to the normal migration runner or `db push`/i);
  assert.match(artifact.runbook,/remove only the four exact ledger rows/i);
  assert.match(artifact.runbook,/assert exactly four affected rows/i);
  assert.match(artifact.runbook,/must (?:\*\*)?not(?:\*\*)? restore UPDATE, TRUNCATE, REFERENCES, or TRIGGER/i);
  assert.match(artifact.runbook,/31 historical versions/i);
  for(const value of [...versions,...names]) assert.match(artifact.runbook,new RegExp(value));
});

test("immutable baseline manifest contains no invented historical SQL",()=>{
  const manifest=JSON.parse(artifact.manifest);
  assert.equal(manifest.baseline_repository_commit,"b945a15348f690d89ff85c6b5aff40eac303769c");
  assert.equal(manifest.scope.auth_managed_schema_included,false);
  assert.equal(manifest.scope.row_data_included,false);
  assert.equal(manifest.scope.fresh_project_restore_verified,false);
  assert.equal(manifest.historical_ledger.length,31);
  assert.equal(new Set(manifest.historical_ledger.map((row)=>row.version)).size,31);
  assert.equal(manifest.historical_ledger.every((row)=>row.historical_sql_available===false),true);
  assert.equal(Object.hasOwn(manifest,"historical_sql"),false);
  assert.equal(Object.hasOwn(manifest,"schema_sql"),false);
});

test("repository checksums and generated evidence are deterministic",()=>{
  const expected={
    "supabase/migrations/20260818090000_create_oauth_transactions.sql":"728f62d4af52eccd0fee195b0a3dedf68889e52b7434f2676bfb90a5b65ef00b",
    "supabase/migrations/20260818120000_create_platform_connection_tokens.sql":"afada06dec7b95aca69e3d01dc432554c2599dcc1b357dbb300b3695ae8cb156",
    "supabase/migrations/20260819120000_harden_platform_connection_tokens_service_role_grants.sql":"7cd9c0dcf6b4ec8ece5c12656b974a9c02587158771404ba6e7e3735e9ca9c76",
  };
  for(const [file,digest] of Object.entries(expected)){
    const first=crypto.createHash("sha256").update(fs.readFileSync(path.join(root,file))).digest("hex");
    const second=crypto.createHash("sha256").update(fs.readFileSync(path.join(root,file))).digest("hex");
    assert.equal(first,digest);
    assert.equal(second,first);
  }
});

test("artifacts contain no credential, URI, identity, or secret material",()=>{
  const combined=Object.values(artifact).join("\n");
  assert.doesNotMatch(combined,/\b(?:postgres(?:ql)?:\/\/|https:\/\/[^\s/]*supabase|authorization\s*:|bearer\s+|service_role_key|anon_key)\b/i);
  assert.doesNotMatch(combined,/\b(?:sk|sbp|key|token)-[A-Za-z0-9_-]{16,}\b/);
  assert.doesNotMatch(combined,/[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/i);
});
