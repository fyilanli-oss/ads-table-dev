-- R6-D3-C read-only production postcheck.
-- Run after Save makes Meta Connected and after one Shopify page reload.
-- This query returns aggregate state only; it never returns IDs or tokens.

with table_state as (
  select
    c.relrowsecurity as rls_enabled,
    c.relforcerowsecurity as force_rls_enabled
  from pg_class c
  where c.oid = to_regclass('public.workspace_provider_connections')
), browser_grants as (
  select count(*)::integer as grant_count
  from information_schema.role_table_grants
  where table_schema = 'public'
    and table_name = 'workspace_provider_connections'
    and grantee in ('anon', 'authenticated')
), meta_state as (
  select
    count(*)::integer as canonical_meta_count,
    count(*) filter (where status = 'connected')::integer as connected_meta_count,
    count(*) filter (where status = 'pending_account_selection')::integer as pending_meta_count,
    count(*) filter (
      where status = 'connected'
        and (
          jsonb_typeof(selected_accounts) <> 'array'
          or jsonb_array_length(selected_accounts) not between 1 and 3
          or active_account_id is distinct from selected_accounts->0->>'id'
          or access_token_envelope is null
          or refresh_token_envelope is not null
          or access_token_expires_at is null
          or access_token_expires_at <= now()
          or not ('ads_read' = any(granted_scopes))
          or account_verified_at is null
          or connected_at is null
          or disconnected_at is not null
          or connection_version <= 0
          or monthly_plan_cost is not null
          or conversion_metric_id is not null
          or conversion_metric_name is not null
          or conversion_metric_integration_name is not null
          or conversion_metric_integration_category is not null
          or conversion_metric_verified_at is not null
        )
    )::integer as invalid_connected_meta_count
  from public.workspace_provider_connections
  where provider = 'meta'
), selected_account_state as (
  select count(*)::integer as invalid_selected_account_count
  from public.workspace_provider_connections connection
  cross join lateral jsonb_array_elements(connection.selected_accounts) account
  where connection.provider = 'meta'
    and connection.status = 'connected'
    and (
      jsonb_typeof(account) <> 'object'
      or nullif(btrim(account->>'id'), '') is null
      or nullif(btrim(account->>'name'), '') is null
      or coalesce(account->>'currency', '') !~ '^[A-Z]{3}$'
    )
), isolation_state as (
  select
    (select count(*) from public.performance_dataset_rows_v2 where platform = 'meta')::integer
      as dataset_v2_meta_count,
    (select count(*) from public.snapshot_schedules where active and platform = 'meta')::integer
      as active_legacy_meta_schedule_count,
    (select count(*) from public.snapshot_jobs where status in ('queued', 'running') and platform = 'meta')::integer
      as open_legacy_meta_job_count,
    (select count(*) from public.platform_connections where platform = 'meta')::integer
      as legacy_meta_connection_after
)
select
  'R6D3C_META_ACCOUNT_SELECTION_POSTCHECK'::text as check_name,
  table_state.rls_enabled,
  table_state.force_rls_enabled,
  browser_grants.grant_count as browser_grant_count,
  meta_state.canonical_meta_count,
  meta_state.connected_meta_count,
  meta_state.pending_meta_count,
  meta_state.invalid_connected_meta_count,
  selected_account_state.invalid_selected_account_count,
  isolation_state.dataset_v2_meta_count,
  isolation_state.active_legacy_meta_schedule_count,
  isolation_state.open_legacy_meta_job_count,
  isolation_state.legacy_meta_connection_after,
  (
    table_state.rls_enabled
    and table_state.force_rls_enabled
    and browser_grants.grant_count = 0
    and meta_state.canonical_meta_count = 1
    and meta_state.connected_meta_count = 1
    and meta_state.pending_meta_count = 0
    and meta_state.invalid_connected_meta_count = 0
    and selected_account_state.invalid_selected_account_count = 0
    and isolation_state.dataset_v2_meta_count = 0
    and isolation_state.active_legacy_meta_schedule_count = 0
    and isolation_state.open_legacy_meta_job_count = 0
  ) as pass
from table_state, browser_grants, meta_state, selected_account_state, isolation_state;
