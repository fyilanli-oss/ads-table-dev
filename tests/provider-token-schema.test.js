"use strict";

const assert=require("node:assert/strict");
const fs=require("node:fs");
const path=require("node:path");
const test=require("node:test");

const migrationPath=path.join(__dirname,"../supabase/migrations/20260818120000_create_platform_connection_tokens.sql");
const sql=fs.readFileSync(migrationPath,"utf8");
const grantHardeningMigrationPath=path.join(__dirname,"../supabase/migrations/20260819120000_harden_platform_connection_tokens_service_role_grants.sql");
const grantHardeningSql=fs.readFileSync(grantHardeningMigrationPath,"utf8");

test("encrypted token migration creates a dedicated server-only table",()=>{
  assert.match(sql,/create table if not exists public\.platform_connection_tokens/i);
  assert.match(sql,/primary key \(user_id, platform\)/i);
  assert.match(sql,/references auth\.users\(id\) on delete cascade/i);
  assert.doesNotMatch(sql,/\baccess_token\s+text\b|\brefresh_token\s+text\b/i);
});

test("encrypted token table stores only versioned envelope objects",()=>{
  for(const column of ["access_token_envelope","refresh_token_envelope"]){
    assert.match(sql,new RegExp(`${column} jsonb`,`i`));
    assert.match(sql,new RegExp(`jsonb_typeof\\(${column}\\) = 'object'`,`i`));
    assert.match(sql,new RegExp(`${column} \\?& array\\['version', 'keyId', 'iv', 'tag', 'ciphertext'\\]`,`i`));
    assert.match(sql,new RegExp(`${column} - array\\['version', 'keyId', 'iv', 'tag', 'ciphertext'\\] = '\\{\\}'::jsonb`,`i`));
    assert.match(sql,new RegExp(`${column}->>'version' = 'v1'`,`i`));
    for(const field of ["keyId","iv","tag","ciphertext"]){
      assert.match(sql,new RegExp(`coalesce\\(length\\(${column}->>'${field}'\\), 0\\) > 0`,`i`));
    }
  }
  assert.match(sql,/access_token_envelope is not null or refresh_token_envelope is not null/i);
});

test("encrypted token table is forced-RLS and service-role only",()=>{
  assert.match(sql,/enable row level security/i);
  assert.match(sql,/force row level security/i);
  for(const role of ["public","anon","authenticated"])assert.match(sql,new RegExp(`revoke all on table public\\.platform_connection_tokens from ${role}`,"i"));
  assert.match(sql,/grant select, insert, update, delete on table public\.platform_connection_tokens to service_role/i);
  assert.doesNotMatch(sql,/create policy/i);
});

test("migration is additive and does not alter legacy token columns",()=>{
  assert.doesNotMatch(sql,/drop\s+(table|column)|truncate|delete\s+from|update\s+public\.platform_connections/i);
  assert.doesNotMatch(sql,/alter table public\.platform_connections/i);
});

test("corrective migration resets service-role grants before granting only CRUD",()=>{
  const revokeIndex=grantHardeningSql.search(/revoke all(?: privileges)? on table public\.platform_connection_tokens from service_role/i);
  const grants=[...grantHardeningSql.matchAll(/grant\s+([^;]+?)\s+on table public\.platform_connection_tokens to service_role\s*;/gi)];

  assert.notEqual(revokeIndex,-1);
  assert.equal(grants.length,1);
  assert.deepEqual(
    new Set(grants[0][1].split(",").map((privilege)=>privilege.trim().toUpperCase())),
    new Set(["SELECT","INSERT","UPDATE","DELETE"]),
  );
  assert.ok(revokeIndex<grants[0].index,"service_role privileges must be reset before CRUD is granted");
});

test("corrective migration preserves client revokes and forced RLS",()=>{
  for(const role of ["public","anon","authenticated"]){
    assert.match(grantHardeningSql,new RegExp(`revoke all(?: privileges)? on table public\\.platform_connection_tokens from ${role}\\s*;`,"i"));
  }
  assert.match(grantHardeningSql,/alter table public\.platform_connection_tokens enable row level security\s*;/i);
  assert.match(grantHardeningSql,/alter table public\.platform_connection_tokens force row level security\s*;/i);
});

test("corrective migration changes neither global privileges, schema, nor data",()=>{
  assert.doesNotMatch(grantHardeningSql,/alter\s+default\s+privileges|\bowner\s+to\b/i);
  assert.doesNotMatch(grantHardeningSql,/\b(?:insert\s+into|update|delete\s+from|truncate)\s+public\.platform_connection_tokens\b/i);
  assert.doesNotMatch(grantHardeningSql,/\b(?:create|drop)\s+(?:table|column|constraint)\b|\b(?:add|drop|rename)\s+(?:column|constraint)\b/i);
  assert.doesNotMatch(grantHardeningSql,/provider_token_(?:encryption|legacy_read)_enabled|backfill|rotation/i);
});
