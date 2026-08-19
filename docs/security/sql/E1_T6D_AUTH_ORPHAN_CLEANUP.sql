-- One-time operational cleanup for manual execution in Supabase SQL Editor.
-- This fail-closed data operation is not a schema migration or migration-ledger entry.
begin;

lock table auth.users in share mode;
lock table public.platform_connections in share row exclusive mode;
lock table public.platform_connection_tokens in share row exclusive mode;
lock table public.platform_account_ownerships in share row exclusive mode;
lock table public.snapshot_schedules in share row exclusive mode;
lock table public.snapshot_jobs in share row exclusive mode;

create temporary table e1_t6d_cleanup_targets (
  user_id uuid not null,
  platform text not null,
  primary key (user_id, platform)
) on commit drop;

insert into e1_t6d_cleanup_targets (user_id, platform)
select pc.user_id, pc.platform
from public.platform_connections as pc
where pc.connected = true
  and pc.platform in ('meta', 'pinterest')
  and not exists (
    select 1
    from auth.users as au
    where au.id = pc.user_id
  );

do $cleanup$
declare
  target_count bigint;
  target_user_count bigint;
  meta_count bigint;
  pinterest_count bigint;
  connected_outside_before bigint;
  encrypted_outside_before bigint;
  connected_outside_after bigint;
  encrypted_outside_after bigint;
  deleted_connection_count bigint;
  remaining_target_connections bigint;
  remaining_target_tokens bigint;
  cleanup_time timestamptz := statement_timestamp();
begin
  select count(*), count(distinct user_id),
         count(*) filter (where platform = 'meta'),
         count(*) filter (where platform = 'pinterest')
    into target_count, target_user_count, meta_count, pinterest_count
  from e1_t6d_cleanup_targets;

  if target_count <> 2
     or target_user_count <> 1
     or meta_count <> 1
     or pinterest_count <> 1 then
    raise exception using
      errcode = 'P0001',
      message = format(
        'E1_T6D_PRECONDITION_FAILED target_count=%s distinct_user_count=%s meta_count=%s pinterest_count=%s',
        target_count, target_user_count, meta_count, pinterest_count
      );
  end if;

  select count(*) into connected_outside_before
  from public.platform_connections as pc
  where pc.connected = true
    and not exists (
      select 1 from e1_t6d_cleanup_targets as target
      where target.user_id = pc.user_id and target.platform = pc.platform
    );

  select count(*) into encrypted_outside_before
  from public.platform_connection_tokens as pct
  where not exists (
    select 1 from e1_t6d_cleanup_targets as target
    where target.user_id = pct.user_id and target.platform = pct.platform
  );

  update public.platform_account_ownerships as ownership
  set status = 'disconnected',
      disconnected_at = cleanup_time,
      disconnect_reason = 'auth_orphan_cleanup_e1_t6d',
      lifecycle_version = 'v1',
      updated_at = cleanup_time
  from e1_t6d_cleanup_targets as target
  where ownership.owner_user_id = target.user_id
    and ownership.platform = target.platform
    and ownership.status in ('active', 'connected');

  update public.snapshot_schedules as schedule
  set active = false,
      stopped_at = cleanup_time,
      stop_reason = 'auth_orphan_cleanup_e1_t6d',
      lifecycle_version = 'v1',
      updated_at = cleanup_time
  from e1_t6d_cleanup_targets as target
  where schedule.user_id = target.user_id
    and schedule.platform = target.platform
    and schedule.active = true;

  update public.snapshot_jobs as job
  set status = 'failed',
      error_message = 'Stopped by auth orphan cleanup',
      finished_at = cleanup_time,
      lifecycle_version = 'v1',
      updated_at = cleanup_time
  from e1_t6d_cleanup_targets as target
  where job.user_id = target.user_id
    and job.platform = target.platform
    and job.status in ('queued', 'running');

  delete from public.platform_connection_tokens as pct
  using e1_t6d_cleanup_targets as target
  where pct.user_id = target.user_id
    and pct.platform = target.platform;

  delete from public.platform_connections as pc
  using e1_t6d_cleanup_targets as target
  where pc.user_id = target.user_id
    and pc.platform = target.platform
    and pc.connected = true;
  get diagnostics deleted_connection_count = row_count;

  select count(*) into remaining_target_connections
  from public.platform_connections as pc
  join e1_t6d_cleanup_targets as target
    on target.user_id = pc.user_id and target.platform = pc.platform
  where pc.connected = true;

  select count(*) into remaining_target_tokens
  from public.platform_connection_tokens as pct
  join e1_t6d_cleanup_targets as target
    on target.user_id = pct.user_id and target.platform = pct.platform;

  select count(*) into connected_outside_after
  from public.platform_connections as pc
  where pc.connected = true
    and not exists (
      select 1 from e1_t6d_cleanup_targets as target
      where target.user_id = pc.user_id and target.platform = pc.platform
    );

  select count(*) into encrypted_outside_after
  from public.platform_connection_tokens as pct
  where not exists (
    select 1 from e1_t6d_cleanup_targets as target
    where target.user_id = pct.user_id and target.platform = pct.platform
  );

  if deleted_connection_count <> 2
     or remaining_target_connections <> 0
     or remaining_target_tokens <> 0
     or connected_outside_after <> connected_outside_before
     or encrypted_outside_after <> encrypted_outside_before then
    raise exception using
      errcode = 'P0001',
      message = format(
        'E1_T6D_POSTCONDITION_FAILED deleted=%s remaining_connections=%s remaining_tokens=%s connected_outside_delta=%s encrypted_outside_delta=%s',
        deleted_connection_count,
        remaining_target_connections,
        remaining_target_tokens,
        connected_outside_after - connected_outside_before,
        encrypted_outside_after - encrypted_outside_before
      );
  end if;
end
$cleanup$;

commit;
