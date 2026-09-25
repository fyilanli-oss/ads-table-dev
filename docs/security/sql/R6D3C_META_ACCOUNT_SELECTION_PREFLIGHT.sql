-- R6-D3-C read-only production preflight.
-- Run immediately before the merchant starts the clean embedded Meta OAuth.
-- This query returns aggregate state only; it never returns IDs or tokens.

with table_state as (
  select
    c.oid is not null as canonical_table_exists,
    coalesce(c.relrowsecurity, false) as rls_enabled,
    coalesce(c.relforcerowsecurity, false) as force_rls_enabled
  from (values (1)) seed(value)
  left join pg_class c
    on c.oid = to_regclass('public.workspace_provider_connections')
), selected_accounts_column as (
  select count(*)::integer as column_count
  from information_schema.columns
  where table_schema = 'public'
    and table_name = 'workspace_provider_connections'
    and column_name = 'selected_accounts'
    and is_nullable = 'NO'
    and column_default = '''[]''::jsonb'
), account_constraints as (
  select
    count(*)::integer as constraint_count,
    coalesce(bool_and(convalidated), false) as all_validated
  from pg_constraint
  where conrelid = to_regclass('public.workspace_provider_connections')
    and conname in (
      'workspace_provider_selected_accounts_shape',
      'workspace_provider_pending_accounts_empty',
      'workspace_provider_connected_account_count'
    )
), browser_grants as (
  select count(*)::integer as grant_count
  from information_schema.role_table_grants
  where table_schema = 'public'
    and table_name = 'workspace_provider_connections'
    and grantee in ('anon', 'authenticated')
), aggregate_state as (
  select
    (select count(*) from public.workspace_settings where reporting_currency ~ '^[A-Z]{3}$')::integer
      as reporting_currency_count,
    (select count(*) from public.workspace_provider_connections where provider = 'meta')::integer
      as canonical_meta_count,
    (select count(*) from public.performance_dataset_rows_v2 where platform = 'meta')::integer
      as dataset_v2_meta_count,
    (select count(*) from public.snapshot_schedules where active and platform = 'meta')::integer
      as active_legacy_meta_schedule_count,
    (select count(*) from public.snapshot_jobs where status in ('queued', 'running') and platform = 'meta')::integer
      as open_legacy_meta_job_count,
    (select count(*) from public.platform_connections where platform = 'meta')::integer
      as legacy_meta_connection_baseline
)
select
  'R6D3C_META_ACCOUNT_SELECTION_PREFLIGHT'::text as check_name,
  table_state.canonical_table_exists,
  table_state.rls_enabled,
  table_state.force_rls_enabled,
  selected_accounts_column.column_count as selected_accounts_column_count,
  account_constraints.constraint_count as account_constraint_count,
  account_constraints.all_validated as account_constraints_validated,
  browser_grants.grant_count as browser_grant_count,
  aggregate_state.reporting_currency_count,
  aggregate_state.canonical_meta_count,
  aggregate_state.dataset_v2_meta_count,
  aggregate_state.active_legacy_meta_schedule_count,
  aggregate_state.open_legacy_meta_job_count,
  aggregate_state.legacy_meta_connection_baseline,
  (
    table_state.canonical_table_exists
    and table_state.rls_enabled
    and table_state.force_rls_enabled
    and selected_accounts_column.column_count = 1
    and account_constraints.constraint_count = 3
    and account_constraints.all_validated
    and browser_grants.grant_count = 0
    and aggregate_state.reporting_currency_count > 0
    and aggregate_state.canonical_meta_count = 0
    and aggregate_state.dataset_v2_meta_count = 0
    and aggregate_state.active_legacy_meta_schedule_count = 0
    and aggregate_state.open_legacy_meta_job_count = 0
  ) as pass
from table_state, selected_accounts_column, account_constraints, browser_grants, aggregate_state;
