"use strict";

const assert=require("node:assert/strict");
const fs=require("node:fs");
const path=require("node:path");
const test=require("node:test");

const migrationPath=path.join(__dirname,"../supabase/migrations/20260818120000_create_platform_connection_tokens.sql");
const sql=fs.readFileSync(migrationPath,"utf8");

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
