-- R6-D2-C1 read-only preflight. Run before the additive migration.
with table_state as (
  select
    c.oid is not null as canonical_table_exists,
    coalesce(c.relrowsecurity, false) as rls_enabled,
    coalesce(c.relforcerowsecurity, false) as force_rls_enabled
  from (values (1)) seed(value)
  left join pg_class c on c.oid = to_regclass('public.workspace_provider_connections')
), target_columns as (
  select count(*)::integer as existing_count
  from information_schema.columns
  where table_schema = 'public'
    and table_name = 'workspace_provider_connections'
    and column_name in (
      'conversion_metric_id',
      'conversion_metric_name',
      'conversion_metric_integration_name',
      'conversion_metric_integration_category',
      'conversion_metric_verified_at'
    )
), target_constraint as (
  select count(*)::integer as existing_count
  from pg_constraint
  where conrelid = to_regclass('public.workspace_provider_connections')
    and conname = 'workspace_provider_conversion_metric_complete'
), browser_grants as (
  select count(*)::integer as grant_count
  from information_schema.role_table_grants
  where table_schema = 'public'
    and table_name = 'workspace_provider_connections'
    and grantee in ('anon', 'authenticated')
), aggregate_state as (
  select
    count(*)::integer as canonical_connection_count,
    count(*) filter (where provider = 'klaviyo' and status = 'connected')::integer as connected_klaviyo_count
  from public.workspace_provider_connections
)
select
  'R6D2C1_KLAVIYO_METRIC_BINDING_PREFLIGHT'::text as check_name,
  table_state.canonical_table_exists,
  table_state.rls_enabled,
  table_state.force_rls_enabled,
  target_columns.existing_count as target_column_count,
  target_constraint.existing_count as target_constraint_count,
  browser_grants.grant_count as browser_grant_count,
  aggregate_state.canonical_connection_count,
  aggregate_state.connected_klaviyo_count,
  (
    table_state.canonical_table_exists
    and table_state.rls_enabled
    and table_state.force_rls_enabled
    and target_columns.existing_count = 0
    and target_constraint.existing_count = 0
    and browser_grants.grant_count = 0
  ) as pass
from table_state, target_columns, target_constraint, browser_grants, aggregate_state;
