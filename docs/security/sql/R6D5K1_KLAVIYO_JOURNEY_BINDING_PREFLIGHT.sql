-- R6-D5-K1 read-only production preflight.
-- Run before 20260928145812_add_workspace_provider_journey_metrics.sql.
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
      'add_to_cart_metric_id',
      'add_to_cart_metric_name',
      'add_to_cart_metric_integration_name',
      'add_to_cart_metric_integration_category',
      'add_to_cart_metric_verified_at',
      'checkout_metric_id',
      'checkout_metric_name',
      'checkout_metric_integration_name',
      'checkout_metric_integration_category',
      'checkout_metric_verified_at'
    )
), target_constraints as (
  select
    count(*) filter (where conname = 'workspace_provider_add_to_cart_metric_complete')::integer as add_to_cart_count,
    count(*) filter (where conname = 'workspace_provider_checkout_metric_complete')::integer as checkout_count
  from pg_constraint
  where conrelid = to_regclass('public.workspace_provider_connections')
), browser_grants as (
  select count(*)::integer as grant_count
  from information_schema.role_table_grants
  where table_schema = 'public'
    and table_name = 'workspace_provider_connections'
    and grantee in ('anon', 'authenticated')
), aggregate_state as (
  select
    count(*)::integer as canonical_connection_count,
    count(*) filter (where provider = 'klaviyo' and status = 'connected')::integer as connected_klaviyo_count,
    count(*) filter (
      where provider = 'klaviyo'
        and status = 'connected'
        and conversion_metric_id is not null
    )::integer as purchase_binding_count
  from public.workspace_provider_connections
), dataset_state as (
  select
    count(*)::integer as dataset_v2_count,
    count(*) filter (where platform = 'klaviyo')::integer as klaviyo_dataset_v2_count
  from public.performance_dataset_rows_v2
)
select
  'R6D5K1_KLAVIYO_JOURNEY_BINDING_PREFLIGHT'::text as check_name,
  table_state.canonical_table_exists,
  table_state.rls_enabled,
  table_state.force_rls_enabled,
  target_columns.existing_count as target_column_count,
  target_constraints.add_to_cart_count as add_to_cart_constraint_count,
  target_constraints.checkout_count as checkout_constraint_count,
  browser_grants.grant_count as browser_grant_count,
  aggregate_state.canonical_connection_count,
  aggregate_state.connected_klaviyo_count,
  aggregate_state.purchase_binding_count,
  dataset_state.dataset_v2_count,
  dataset_state.klaviyo_dataset_v2_count,
  (
    table_state.canonical_table_exists
    and table_state.rls_enabled
    and table_state.force_rls_enabled
    and target_columns.existing_count = 0
    and target_constraints.add_to_cart_count = 0
    and target_constraints.checkout_count = 0
    and browser_grants.grant_count = 0
  ) as pass
from table_state, target_columns, target_constraints, browser_grants, aggregate_state, dataset_state;
