"use strict";

const assert=require("node:assert/strict");
const fs=require("node:fs");
const path=require("node:path");
const test=require("node:test");

const root=path.join(__dirname,"..");
const sqlDir=path.join(root,"docs/security/sql");
const names={
  preflight:"E1_T6E_PLAINTEXT_TOKEN_PREFLIGHT.sql",
  nulling:"E1_T6E_PLAINTEXT_TOKEN_NULLING.sql",
  acceptance:"E1_T6E_PLAINTEXT_TOKEN_ACCEPTANCE.sql",
};
const read=(name)=>fs.readFileSync(path.join(sqlDir,names[name]),"utf8");
const preflight=read("preflight");
const nulling=read("nulling");
const acceptance=read("acceptance");

test("all E1-T6E artifacts exist outside the migration ledger",()=>{
  for(const name of Object.values(names)) assert.equal(fs.existsSync(path.join(sqlDir,name)),true);
  const migrations=fs.readdirSync(path.join(root,"supabase/migrations"));
  assert.equal(migrations.some((name)=>/e1[_-]?t6e|plaintext[_-]?token[_-]?null/i.test(name)),false);
  assert.doesNotMatch(path.relative(root,path.join(sqlDir,names.nulling)),/^supabase\/migrations\//);
});

for(const [label,sql] of [["preflight",preflight],["acceptance",acceptance]]){
  test(`${label} is read-only with a three-column redacted result`,()=>{
    const withoutComments=sql.replace(/^\s*--.*$/gm,"");
    assert.match(withoutComments,/^\s*with\s/i);
    assert.doesNotMatch(withoutComments,/\b(?:insert|update|delete|truncate|alter|drop|create|call|do|lock|copy)\b/i);
    assert.match(sql,/select check_name, platform, row_count[\s\S]*order by check_name, platform nulls first;\s*$/i);
    assert.doesNotMatch(sql,/select\s+(?:pc\.|pct\.)?(?:user_id|access_token|refresh_token|access_token_envelope|refresh_token_envelope|metadata|account_id|account_name)\b/i);
    assert.doesNotMatch(sql,/\b(?:ciphertext|\biv\b|\btag\b|key_id)\b/i);
  });
}

test("preflight supplies the complete global and per-platform count contract",()=>{
  for(const check of ["connected_connections_total","connected_with_auth_user","connected_without_auth_user","connected_with_encrypted_token","connected_without_encrypted_token","encrypted_connections_total","connected_legacy_access_token_present","connected_legacy_refresh_token_present","connected_any_legacy_token_present","disconnected_legacy_access_token_present","disconnected_legacy_refresh_token_present","disconnected_any_legacy_token_present","global_legacy_access_token_present","global_legacy_refresh_token_present","global_any_legacy_token_present","legacy_access_without_encrypted_access_envelope","legacy_refresh_without_encrypted_refresh_envelope","encrypted_access_envelope_present","encrypted_refresh_envelope_present"]){
    assert.match(preflight,new RegExp(`'${check}'`));
  }
  assert.match(preflight,/group by platform/g);
  assert.match(preflight,/access_token is not null[\s\S]*refresh_token is not null/i);
});

test("acceptance supplies the post-operation count contract",()=>{
  for(const check of ["connected_connections_total","encrypted_connections_total","connected_with_encrypted_token","connected_without_encrypted_token","connected_without_auth_user","global_legacy_access_token_present","global_legacy_refresh_token_present","global_any_legacy_token_present","disconnected_any_legacy_token_present","legacy_access_without_encrypted_access_envelope","legacy_refresh_without_encrypted_refresh_envelope"]){
    assert.match(acceptance,new RegExp(`'${check}'`));
  }
});

test("nulling is one locked, fail-closed SQL Editor transaction",()=>{
  assert.match(nulling,/manual execution in Supabase SQL Editor/i);
  assert.match(nulling,/not a schema migration[\s\S]*not included in the Supabase migration ledger/i);
  assert.match(nulling,/irreversibly nulls plaintext tokens[\s\S]*no token backup or token output/i);
  assert.match(nulling,/^begin;/m);
  assert.match(nulling,/lock table auth\.users in share mode;/i);
  assert.match(nulling,/lock table public\.platform_connections in share row exclusive mode;/i);
  assert.match(nulling,/lock table public\.platform_connection_tokens in share row exclusive mode;/i);
  assert.match(nulling,/E1_T6E_PRECONDITION_FAILED/);
  assert.match(nulling,/E1_T6E_POSTCONDITION_FAILED/);
  assert.match(nulling,/commit;\s*$/);
});

test("materialized targets require connected, auth-existing, encrypted pairs",()=>{
  const targetInsert=nulling.match(/insert into e1_t6e_nulling_targets[\s\S]*?where pc\.connected = true;/i)?.[0] ?? "";
  assert.match(targetInsert,/join auth\.users as au on au\.id = pc\.user_id/i);
  assert.match(targetInsert,/join public\.platform_connection_tokens as pct[\s\S]*pct\.user_id = pc\.user_id and pct\.platform = pc\.platform/i);
  assert.doesNotMatch(targetInsert,/access_token|refresh_token|envelope/i);
  assert.match(nulling,/target_count <> 7 or target_distinct_count <> 7/);
});

test("preconditions bind all population and type-specific safety guards",()=>{
  assert.match(nulling,/connected_before <> 7[\s\S]*encrypted_before <> 7/);
  assert.match(nulling,/auth_orphans_before <> 0 or missing_encrypted_before <> 0/);
  assert.match(nulling,/disconnected_access_before <> 0 or disconnected_refresh_before <> 0/);
  assert.match(nulling,/pc\.access_token is not null and pct\.access_token_envelope is null/);
  assert.match(nulling,/pc\.refresh_token is not null and pct\.refresh_token_envelope is null/);
  assert.match(nulling,/outside_plaintext_before <> 0 or empty_encrypted_before <> 0/);
});

test("the only data mutation nulls target tokens and stamps updated_at",()=>{
  const updates=[...nulling.matchAll(/\bupdate\s+([\w.]+)/gi)].map((match)=>match[1]);
  assert.deepEqual(updates,["public.platform_connections"]);
  const statement=nulling.match(/update public\.platform_connections as pc[\s\S]*?get diagnostics updated_count = row_count;/i)?.[0] ?? "";
  assert.match(statement,/set access_token = null,\s*refresh_token = null,\s*updated_at = operation_time/i);
  assert.match(statement,/from e1_t6e_nulling_targets as target[\s\S]*target\.user_id = pc\.user_id and target\.platform = pc\.platform/i);
  assert.doesNotMatch(statement,/\bconnected\s*=|token_expires_at\s*=|account_id\s*=|account_name\s*=|metadata\s*=|disconnected_at\s*=|disconnect_reason\s*=/i);
  assert.doesNotMatch(nulling,/\bdelete\s+from|\breturning\b/i);
  assert.doesNotMatch(nulling,/\b(?:update|delete)\s+public\.(?:platform_connection_tokens|snapshot_jobs|snapshot_schedules|dashboard_snapshots|performance_dataset_rows(?:_v2)?)/i);
});

test("postconditions preserve rows, non-targets, envelopes, and protected fields",()=>{
  assert.match(nulling,/global_access_after <> 0 or global_refresh_after <> 0 or global_any_after <> 0/);
  assert.match(nulling,/connected_after <> 7 or encrypted_after <> 7 or connected_encrypted_after <> 7/);
  assert.match(nulling,/missing_encrypted_after <> 0 or auth_orphans_after <> 0 or updated_count <> 7/);
  assert.match(nulling,/connections_after <> connections_before/);
  assert.match(nulling,/outside_connections_after <> outside_connections_before/);
  assert.match(nulling,/outside_encrypted_after <> outside_encrypted_before/);
  assert.match(nulling,/envelope_digest_after is distinct from envelope_digest_before/);
  assert.match(nulling,/protected_connection_digest_after is distinct from protected_connection_digest_before/);
});

test("exceptions and test source contain no credential or raw identity material",()=>{
  const messages=[...nulling.matchAll(/message\s*=\s*format\(\s*'([^']+)'/gi)].map((match)=>match[1]);
  assert.equal(messages.length,2);
  for(const message of messages) assert.doesNotMatch(message,/user_id|access_token|refresh_token|envelope_digest|metadata|account_|[0-9a-f]{8}-[0-9a-f-]{27,}/i);
  assert.doesNotMatch(fs.readFileSync(__filename,"utf8"),/\b(?:sk|key|token)-[A-Za-z0-9_-]{16,}\b|[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/i);
});
