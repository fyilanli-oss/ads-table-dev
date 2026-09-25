-- R6-D2-C1 read-only postcheck. Run immediately after the additive migration.
with target_columns as (
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
  select count(*)::integer as existing_count,
         coalesce(bool_and(convalidated), false) as all_validated
  from pg_constraint
  where conrelid = to_regclass('public.workspace_provider_connections')
    and conname = 'workspace_provider_conversion_metric_complete'
), browser_grants as (
  select count(*)::integer as grant_count
  from information_schema.role_table_grants
  where table_schema = 'public'
    and table_name = 'workspace_provider_connections'
    and grantee in ('anon', 'authenticated')
), table_state as (
  select relrowsecurity as rls_enabled, relforcerowsecurity as force_rls_enabled
  from pg_class
  where oid = to_regclass('public.workspace_provider_connections')
), binding_state as (
  select
    count(*) filter (where conversion_metric_id is not null)::integer as bound_metric_count,
    count(*) filter (
      where conversion_metric_id is not null
        and (
          provider <> 'klaviyo'
          or status <> 'connected'
          or active_account_id is null
          or lower(btrim(conversion_metric_name)) <> 'placed order'
          or conversion_metric_integration_name is null
          or conversion_metric_verified_at is null
        )
    )::integer as invalid_binding_count
  from public.workspace_provider_connections
)
select
  'R6D2C1_KLAVIYO_METRIC_BINDING_POSTCHECK'::text as check_name,
  target_columns.existing_count as target_column_count,
  target_constraint.existing_count as target_constraint_count,
  target_constraint.all_validated,
  table_state.rls_enabled,
  table_state.force_rls_enabled,
  browser_grants.grant_count as browser_grant_count,
  binding_state.bound_metric_count,
  binding_state.invalid_binding_count,
  (
    target_columns.existing_count = 5
    and target_constraint.existing_count = 1
    and target_constraint.all_validated
    and table_state.rls_enabled
    and table_state.force_rls_enabled
    and browser_grants.grant_count = 0
    and binding_state.bound_metric_count = 0
    and binding_state.invalid_binding_count = 0
  ) as pass
from target_columns, target_constraint, browser_grants, table_state, binding_state;
