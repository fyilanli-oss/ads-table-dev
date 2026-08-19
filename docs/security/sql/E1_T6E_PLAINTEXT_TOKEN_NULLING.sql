-- One-time operational SQL for manual execution in Supabase SQL Editor.
-- This is not a schema migration and is not included in the Supabase migration ledger.
-- It irreversibly nulls plaintext tokens; it creates no token backup or token output.
begin;

lock table auth.users in share mode;
lock table public.platform_connections in share row exclusive mode;
lock table public.platform_connection_tokens in share row exclusive mode;

create temporary table e1_t6e_nulling_targets (
  user_id uuid not null,
  platform text not null
) on commit drop;

insert into e1_t6e_nulling_targets (user_id, platform)
select pc.user_id, pc.platform
from public.platform_connections as pc
join auth.users as au on au.id = pc.user_id
join public.platform_connection_tokens as pct
  on pct.user_id = pc.user_id and pct.platform = pc.platform
where pc.connected = true;

do $nulling$
declare
  operation_time timestamptz := statement_timestamp();
  connections_before bigint;
  connected_before bigint;
  encrypted_before bigint;
  outside_connections_before bigint;
  outside_encrypted_before bigint;
  target_count bigint;
  target_distinct_count bigint;
  target_access_before bigint;
  target_refresh_before bigint;
  auth_orphans_before bigint;
  missing_encrypted_before bigint;
  disconnected_access_before bigint;
  disconnected_refresh_before bigint;
  access_type_mismatch_before bigint;
  refresh_type_mismatch_before bigint;
  outside_plaintext_before bigint;
  empty_encrypted_before bigint;
  envelope_digest_before text;
  protected_connection_digest_before text;
  updated_count bigint;
  global_access_after bigint;
  global_refresh_after bigint;
  global_any_after bigint;
  connections_after bigint;
  connected_after bigint;
  encrypted_after bigint;
  connected_encrypted_after bigint;
  missing_encrypted_after bigint;
  auth_orphans_after bigint;
  outside_connections_after bigint;
  outside_encrypted_after bigint;
  envelope_digest_after text;
  protected_connection_digest_after text;
begin
  select count(*) into connections_before from public.platform_connections;
  select count(*) into connected_before from public.platform_connections where connected = true;
  select count(*) into encrypted_before from public.platform_connection_tokens;
  select count(*), count(distinct (user_id, platform))
    into target_count, target_distinct_count from e1_t6e_nulling_targets;

  select count(*) into outside_connections_before
  from public.platform_connections as pc
  where not exists (select 1 from e1_t6e_nulling_targets as target where target.user_id = pc.user_id and target.platform = pc.platform);

  select count(*) into outside_encrypted_before
  from public.platform_connection_tokens as pct
  where not exists (select 1 from e1_t6e_nulling_targets as target where target.user_id = pct.user_id and target.platform = pct.platform);

  select count(*) filter (where pc.access_token is not null),
         count(*) filter (where pc.refresh_token is not null)
    into target_access_before, target_refresh_before
  from public.platform_connections as pc
  join e1_t6e_nulling_targets as target on target.user_id = pc.user_id and target.platform = pc.platform;

  select count(*) into auth_orphans_before
  from public.platform_connections as pc
  where pc.connected = true and not exists (select 1 from auth.users as au where au.id = pc.user_id);

  select count(*) into missing_encrypted_before
  from public.platform_connections as pc
  where pc.connected = true and not exists (
    select 1 from public.platform_connection_tokens as pct where pct.user_id = pc.user_id and pct.platform = pc.platform
  );

  select count(*) filter (where access_token is not null), count(*) filter (where refresh_token is not null)
    into disconnected_access_before, disconnected_refresh_before
  from public.platform_connections where connected = false;

  select count(*) filter (where pc.access_token is not null and pct.access_token_envelope is null),
         count(*) filter (where pc.refresh_token is not null and pct.refresh_token_envelope is null)
    into access_type_mismatch_before, refresh_type_mismatch_before
  from public.platform_connections as pc
  join e1_t6e_nulling_targets as target on target.user_id = pc.user_id and target.platform = pc.platform
  join public.platform_connection_tokens as pct on pct.user_id = pc.user_id and pct.platform = pc.platform;

  select count(*) into outside_plaintext_before
  from public.platform_connections as pc
  where (pc.access_token is not null or pc.refresh_token is not null)
    and not exists (select 1 from e1_t6e_nulling_targets as target where target.user_id = pc.user_id and target.platform = pc.platform);

  select count(*) into empty_encrypted_before
  from public.platform_connection_tokens
  where access_token_envelope is null and refresh_token_envelope is null;

  select md5(coalesce(string_agg(md5(coalesce(access_token_envelope::text, '<null>') || '|' || coalesce(refresh_token_envelope::text, '<null>')), ',' order by user_id::text, platform), ''))
    into envelope_digest_before from public.platform_connection_tokens;

  select md5(coalesce(string_agg(md5((to_jsonb(pc) - 'access_token' - 'refresh_token' - 'updated_at')::text), ',' order by pc.user_id::text, pc.platform), ''))
    into protected_connection_digest_before from public.platform_connections as pc;

  if connected_before <> 7 or target_count <> 7 or target_distinct_count <> 7
     or auth_orphans_before <> 0 or missing_encrypted_before <> 0 or encrypted_before <> 7
     or disconnected_access_before <> 0 or disconnected_refresh_before <> 0
     or access_type_mismatch_before <> 0 or refresh_type_mismatch_before <> 0
     or outside_plaintext_before <> 0 or empty_encrypted_before <> 0 then
    raise exception using errcode = 'P0001', message = format(
      'E1_T6E_PRECONDITION_FAILED connected=%s target=%s distinct_target=%s auth_orphan=%s missing_encrypted=%s encrypted=%s disconnected_access=%s disconnected_refresh=%s access_mismatch=%s refresh_mismatch=%s outside_plaintext=%s empty_encrypted=%s',
      connected_before, target_count, target_distinct_count, auth_orphans_before, missing_encrypted_before, encrypted_before,
      disconnected_access_before, disconnected_refresh_before, access_type_mismatch_before, refresh_type_mismatch_before,
      outside_plaintext_before, empty_encrypted_before
    );
  end if;

  update public.platform_connections as pc
  set access_token = null,
      refresh_token = null,
      updated_at = operation_time
  from e1_t6e_nulling_targets as target
  where target.user_id = pc.user_id and target.platform = pc.platform;
  get diagnostics updated_count = row_count;

  select count(*) filter (where access_token is not null),
         count(*) filter (where refresh_token is not null),
         count(*) filter (where access_token is not null or refresh_token is not null)
    into global_access_after, global_refresh_after, global_any_after from public.platform_connections;
  select count(*) into connections_after from public.platform_connections;
  select count(*) into connected_after from public.platform_connections where connected = true;
  select count(*) into encrypted_after from public.platform_connection_tokens;
  select count(*) into connected_encrypted_after from public.platform_connections as pc
    where pc.connected = true and exists (select 1 from public.platform_connection_tokens as pct where pct.user_id = pc.user_id and pct.platform = pc.platform);
  select count(*) into missing_encrypted_after from public.platform_connections as pc
    where pc.connected = true and not exists (select 1 from public.platform_connection_tokens as pct where pct.user_id = pc.user_id and pct.platform = pc.platform);
  select count(*) into auth_orphans_after from public.platform_connections as pc
    where pc.connected = true and not exists (select 1 from auth.users as au where au.id = pc.user_id);
  select count(*) into outside_connections_after from public.platform_connections as pc
    where not exists (select 1 from e1_t6e_nulling_targets as target where target.user_id = pc.user_id and target.platform = pc.platform);
  select count(*) into outside_encrypted_after from public.platform_connection_tokens as pct
    where not exists (select 1 from e1_t6e_nulling_targets as target where target.user_id = pct.user_id and target.platform = pct.platform);
  select md5(coalesce(string_agg(md5(coalesce(access_token_envelope::text, '<null>') || '|' || coalesce(refresh_token_envelope::text, '<null>')), ',' order by user_id::text, platform), ''))
    into envelope_digest_after from public.platform_connection_tokens;
  select md5(coalesce(string_agg(md5((to_jsonb(pc) - 'access_token' - 'refresh_token' - 'updated_at')::text), ',' order by pc.user_id::text, pc.platform), ''))
    into protected_connection_digest_after from public.platform_connections as pc;

  if global_access_after <> 0 or global_refresh_after <> 0 or global_any_after <> 0
     or connected_after <> 7 or encrypted_after <> 7 or connected_encrypted_after <> 7
     or missing_encrypted_after <> 0 or auth_orphans_after <> 0 or updated_count <> 7
     or connections_after <> connections_before
     or outside_connections_after <> outside_connections_before
     or outside_encrypted_after <> outside_encrypted_before
     or envelope_digest_after is distinct from envelope_digest_before
     or protected_connection_digest_after is distinct from protected_connection_digest_before then
    raise exception using errcode = 'P0001', message = format(
      'E1_T6E_POSTCONDITION_FAILED plaintext_access=%s plaintext_refresh=%s plaintext_any=%s connected=%s encrypted=%s connected_encrypted=%s missing_encrypted=%s auth_orphan=%s updated=%s connection_delta=%s outside_connection_delta=%s outside_encrypted_delta=%s envelope_unchanged=%s protected_fields_unchanged=%s',
      global_access_after, global_refresh_after, global_any_after, connected_after, encrypted_after, connected_encrypted_after,
      missing_encrypted_after, auth_orphans_after, updated_count, connections_after - connections_before,
      outside_connections_after - outside_connections_before, outside_encrypted_after - outside_encrypted_before,
      envelope_digest_after is not distinct from envelope_digest_before,
      protected_connection_digest_after is not distinct from protected_connection_digest_before
    );
  end if;
end
$nulling$;

commit;
