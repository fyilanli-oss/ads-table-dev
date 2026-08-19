with allowed_platforms as (
  select platform
  from (values ('meta'::text), ('pinterest'::text)) as allowed(platform)
), targets as (
  select pc.user_id, pc.platform
  from public.platform_connections as pc
  where pc.connected = true
    and pc.platform in ('meta', 'pinterest')
    and not exists (select 1 from auth.users as au where au.id = pc.user_id)
), evidence as (
  select 'orphan_connected_total'::text as check_name, null::text as platform, count(*)::bigint as row_count from targets
  union all
  select 'distinct_orphan_users', null, count(distinct user_id)::bigint from targets
  union all
  select 'orphan_connected_by_platform', allowed.platform, count(target.user_id)::bigint
  from allowed_platforms as allowed
  left join targets as target on target.platform = allowed.platform
  group by allowed.platform
  union all
  select 'encrypted_target_total', null, count(*)::bigint
  from public.platform_connection_tokens as pct
  join targets as target using (user_id, platform)
  union all
  select 'ownership_target_total', null, count(*)::bigint
  from public.platform_account_ownerships as ownership
  join targets as target on target.user_id = ownership.owner_user_id and target.platform = ownership.platform
  union all
  select 'active_schedule_target_total', null, count(*)::bigint
  from public.snapshot_schedules as schedule
  join targets as target using (user_id, platform)
  where schedule.active = true
  union all
  select 'open_job_target_total', null, count(*)::bigint
  from public.snapshot_jobs as job
  join targets as target using (user_id, platform)
  where job.status in ('queued', 'running')
)
select check_name, platform, row_count
from evidence
order by check_name, platform nulls first;
