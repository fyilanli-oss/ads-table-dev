"use strict";

const assert=require("node:assert/strict");
const fs=require("node:fs");
const path=require("node:path");
const test=require("node:test");

const read=(relative)=>fs.readFileSync(path.join(__dirname,"..",relative),"utf8");
const migration=read("supabase/migrations/20260819160000_cleanup_e1_t6d_auth_orphan_connections.sql");
const preflight=read("docs/security/sql/E1_T6D_AUTH_ORPHAN_PREFLIGHT.sql");
const acceptance=read("docs/security/sql/E1_T6D_AUTH_ORPHAN_ACCEPTANCE.sql");

test("cleanup target is the exact connected auth-orphan pair",()=>{
  assert.match(migration,/pc\.connected = true/i);
  assert.match(migration,/pc\.platform in \('meta', 'pinterest'\)/i);
  assert.match(migration,/not exists \([\s\S]*from auth\.users as au[\s\S]*au\.id = pc\.user_id/i);
  assert.match(migration,/target_count <> 2/);
  assert.match(migration,/target_user_count <> 1/);
  assert.match(migration,/meta_count <> 1/);
  assert.match(migration,/pinterest_count <> 1/);
});

test("cleanup is transactional, locked, and fails closed",()=>{
  assert.match(migration,/^begin;/m);
  assert.match(migration,/lock table auth\.users in share mode/i);
  for(const table of ["platform_connections","platform_connection_tokens","platform_account_ownerships","snapshot_schedules","snapshot_jobs"]){
    assert.match(migration,new RegExp(`lock table public\\.${table} in share row exclusive mode`,`i`));
  }
  assert.match(migration,/raise exception using[\s\S]*E1_T6D_PRECONDITION_FAILED/);
  assert.match(migration,/raise exception using[\s\S]*E1_T6D_POSTCONDITION_FAILED/);
  assert.match(migration,/commit;\s*$/);
  assert.doesNotMatch(migration,/\bexecute\b|format\([^)]*%[IL]/i);
});

test("cleanup follows local disconnect lifecycle without deleting history",()=>{
  assert.match(migration,/update public\.platform_account_ownerships[\s\S]*status = 'disconnected'/i);
  assert.match(migration,/disconnect_reason = 'auth_orphan_cleanup_e1_t6d'/i);
  assert.match(migration,/update public\.snapshot_schedules[\s\S]*active = false/i);
  assert.match(migration,/update public\.snapshot_jobs[\s\S]*status = 'failed'/i);
  assert.match(migration,/delete from public\.platform_connection_tokens/i);
  assert.match(migration,/delete from public\.platform_connections/i);
  assert.match(migration,/deleted_connection_count <> 2/);
  assert.doesNotMatch(migration,/delete from public\.(?:platform_account_ownerships|snapshot_schedules|snapshot_jobs|dashboard_snapshots|performance_dataset_rows(?:_v2)?)/i);
});

test("postconditions preserve every non-target connection and encrypted token",()=>{
  assert.match(migration,/connected_outside_after <> connected_outside_before/);
  assert.match(migration,/encrypted_outside_after <> encrypted_outside_before/);
  assert.match(migration,/remaining_target_connections <> 0/);
  assert.match(migration,/remaining_target_tokens <> 0/);
  assert.match(migration,/using e1_t6d_cleanup_targets as target[\s\S]*pc\.connected = true/i);
});

test("migration emits no credential, envelope, or raw identifier values",()=>{
  assert.doesNotMatch(migration,/raise (?:notice|exception)[^\n]*(?:user_id|access_token|refresh_token|envelope)/i);
  assert.doesNotMatch(migration,/select\s+(?:pc\.)?(?:access_token|refresh_token)|returning/i);
  assert.doesNotMatch(migration,/raise notice/i);
});

for(const [name,sql] of [["preflight",preflight],["acceptance",acceptance]]){
  test(`${name} evidence is read-only and redacted`,()=>{
    assert.match(sql,/^with\s/i);
    assert.doesNotMatch(sql,/\b(?:insert|update|delete|truncate|alter|drop|create|call|do)\b/i);
    assert.doesNotMatch(sql,/access_token|refresh_token|envelope|metadata/i);
    assert.match(sql,/select check_name, platform, row_count/i);
  });
}

test("preflight covers every safe operational count",()=>{
  for(const check of ["orphan_connected_total","distinct_orphan_users","orphan_connected_by_platform","encrypted_target_total","ownership_target_total","active_schedule_target_total","open_job_target_total"]){
    assert.match(preflight,new RegExp(`'${check}'`));
  }
});

test("acceptance covers the seven required post-cleanup metrics",()=>{
  for(const check of ["connected_provider_connections_total","encrypted_provider_connections_total","connected_with_encrypted_token","connected_without_auth_user","connected_without_encrypted_token","target_auth_orphan_connected"]){
    assert.match(acceptance,new RegExp(`'${check}'`));
  }
  assert.match(acceptance,/values \('meta'::text\), \('pinterest'::text\)/);
});
