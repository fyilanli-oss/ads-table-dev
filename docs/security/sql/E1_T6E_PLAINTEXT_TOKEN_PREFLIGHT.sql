-- Read-only, redacted E1-T6E plaintext-token preflight.
with connection_facts as (
  select pc.platform,
         pc.connected,
         (au.id is not null) as has_auth_user,
         (pct.user_id is not null) as has_encrypted_row,
         (pc.access_token is not null) as has_legacy_access,
         (pc.refresh_token is not null) as has_legacy_refresh,
         (pct.access_token_envelope is not null) as has_access_envelope,
         (pct.refresh_token_envelope is not null) as has_refresh_envelope
  from public.platform_connections as pc
  left join auth.users as au on au.id = pc.user_id
  left join public.platform_connection_tokens as pct
    on pct.user_id = pc.user_id and pct.platform = pc.platform
), global_checks(check_name, platform, row_count) as (
  select 'connected_connections_total', null::text, count(*) filter (where connected)::bigint from connection_facts
  union all select 'connected_with_auth_user', null, count(*) filter (where connected and has_auth_user)::bigint from connection_facts
  union all select 'connected_without_auth_user', null, count(*) filter (where connected and not has_auth_user)::bigint from connection_facts
  union all select 'connected_with_encrypted_token', null, count(*) filter (where connected and has_encrypted_row)::bigint from connection_facts
  union all select 'connected_without_encrypted_token', null, count(*) filter (where connected and not has_encrypted_row)::bigint from connection_facts
  union all select 'encrypted_connections_total', null, count(*)::bigint from public.platform_connection_tokens
  union all select 'connected_legacy_access_token_present', null, count(*) filter (where connected and has_legacy_access)::bigint from connection_facts
  union all select 'connected_legacy_refresh_token_present', null, count(*) filter (where connected and has_legacy_refresh)::bigint from connection_facts
  union all select 'connected_any_legacy_token_present', null, count(*) filter (where connected and (has_legacy_access or has_legacy_refresh))::bigint from connection_facts
  union all select 'disconnected_legacy_access_token_present', null, count(*) filter (where not connected and has_legacy_access)::bigint from connection_facts
  union all select 'disconnected_legacy_refresh_token_present', null, count(*) filter (where not connected and has_legacy_refresh)::bigint from connection_facts
  union all select 'disconnected_any_legacy_token_present', null, count(*) filter (where not connected and (has_legacy_access or has_legacy_refresh))::bigint from connection_facts
  union all select 'global_legacy_access_token_present', null, count(*) filter (where has_legacy_access)::bigint from connection_facts
  union all select 'global_legacy_refresh_token_present', null, count(*) filter (where has_legacy_refresh)::bigint from connection_facts
  union all select 'global_any_legacy_token_present', null, count(*) filter (where has_legacy_access or has_legacy_refresh)::bigint from connection_facts
  union all select 'legacy_access_without_encrypted_access_envelope', null, count(*) filter (where has_legacy_access and not has_access_envelope)::bigint from connection_facts
  union all select 'legacy_refresh_without_encrypted_refresh_envelope', null, count(*) filter (where has_legacy_refresh and not has_refresh_envelope)::bigint from connection_facts
  union all select 'encrypted_access_envelope_present', null, count(*) filter (where access_token_envelope is not null)::bigint from public.platform_connection_tokens
  union all select 'encrypted_refresh_envelope_present', null, count(*) filter (where refresh_token_envelope is not null)::bigint from public.platform_connection_tokens
), platform_checks(check_name, platform, row_count) as (
  select 'platform_connected_connections', platform, count(*) filter (where connected)::bigint from connection_facts group by platform
  union all select 'platform_legacy_access_token_present', platform, count(*) filter (where has_legacy_access)::bigint from connection_facts group by platform
  union all select 'platform_legacy_refresh_token_present', platform, count(*) filter (where has_legacy_refresh)::bigint from connection_facts group by platform
  union all select 'platform_encrypted_envelope_present', platform, count(*) filter (where has_encrypted_row and (has_access_envelope or has_refresh_envelope))::bigint from connection_facts group by platform
)
select check_name, platform, row_count from global_checks
union all
select check_name, platform, row_count from platform_checks
order by check_name, platform nulls first;
