with metrics as (
  select 'connected_provider_connections_total'::text as check_name, null::text as platform, count(*)::bigint as row_count
  from public.platform_connections where connected = true
  union all
  select 'encrypted_provider_connections_total', null, count(*)::bigint from public.platform_connection_tokens
  union all
  select 'connected_with_encrypted_token', null, count(*)::bigint
  from public.platform_connections as pc
  join public.platform_connection_tokens as pct using (user_id, platform)
  where pc.connected = true
  union all
  select 'connected_without_auth_user', null, count(*)::bigint
  from public.platform_connections as pc
  where pc.connected = true and not exists (select 1 from auth.users as au where au.id = pc.user_id)
  union all
  select 'connected_without_encrypted_token', null, count(*)::bigint
  from public.platform_connections as pc
  where pc.connected = true
    and not exists (
      select 1 from public.platform_connection_tokens as pct
      where pct.user_id = pc.user_id and pct.platform = pc.platform
    )
  union all
  select 'target_auth_orphan_connected', allowed.platform, count(pc.user_id)::bigint
  from (values ('meta'::text), ('pinterest'::text)) as allowed(platform)
  left join public.platform_connections as pc
    on pc.platform = allowed.platform
   and pc.connected = true
   and not exists (select 1 from auth.users as au where au.id = pc.user_id)
  group by allowed.platform
)
select check_name, platform, row_count
from metrics
order by check_name, platform nulls first;
