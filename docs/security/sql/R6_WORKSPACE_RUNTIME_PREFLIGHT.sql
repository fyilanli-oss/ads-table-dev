-- R6-A read-only production preflight. Expected gate: PASS_PREPARATION_ONLY.

with state as (
  select
    (select count(*) from public.workspaces) as workspace_rows,
    (select count(*) from public.workspace_settings) as workspace_settings_rows,
    (select count(*) from public.workspace_settings where reporting_currency is not null) as reporting_currency_rows,
    (select count(*) from public.workspace_provider_connections) as canonical_connection_rows,
    (select count(*) from public.workspace_provider_connections where status = 'connected') as canonical_connected_rows,
    (select count(*) from public.performance_dataset_rows_v2) as dataset_v2_rows,
    (select count(*) from public.performance_dataset_rows_v2 where workspace_id is not null) as workspace_dataset_rows,
    exists (
      select 1 from information_schema.columns
      where table_schema = 'public'
        and table_name = 'performance_dataset_rows_v2'
        and column_name = 'user_id'
        and is_nullable = 'YES'
    ) as user_id_nullable,
    exists (
      select 1 from pg_constraint
      where conrelid = 'public.performance_dataset_rows_v2'::regclass
        and conname = 'performance_dataset_rows_v2_workspace_fk'
        and convalidated
    ) as workspace_fk_valid,
    exists (
      select 1 from pg_indexes
      where schemaname = 'public'
        and indexname = 'performance_dataset_rows_v2_workspace_canonical_uidx'
    ) as workspace_unique_index_exists,
    (select count(*) from public.snapshot_schedules where active and platform in ('meta','google','klaviyo')) as active_legacy_schedules,
    (select count(*) from public.snapshot_jobs where status in ('queued','running') and platform in ('meta','google','klaviyo')) as open_legacy_jobs,
    (select count(*) from pg_trigger where tgname like 'r4c_freeze_%' and not tgisinternal) as legacy_freeze_triggers,
    (select count(*) from public.shopify_workspace_provider_connections where provider = 'klaviyo' and status = 'revoked') as revoked_embedded_klaviyo_rows
)
select case
  when workspace_rows = 0 then 'BLOCK_WORKSPACE_MISSING'
  when not workspace_fk_valid or not workspace_unique_index_exists then 'BLOCK_R3_FOUNDATION_INCOMPLETE'
  when dataset_v2_rows <> 0 or workspace_dataset_rows <> 0 then 'BLOCK_DATASET_NOT_EMPTY'
  when canonical_connection_rows <> 0 or canonical_connected_rows <> 0 then 'BLOCK_CANONICAL_CONNECTION_ALREADY_ACTIVE'
  when reporting_currency_rows <> 0 then 'BLOCK_CURRENCY_ALREADY_ACTIVE'
  when active_legacy_schedules <> 0 or open_legacy_jobs <> 0 then 'BLOCK_LEGACY_RUNTIME_ACTIVE'
  when legacy_freeze_triggers <> 6 then 'BLOCK_LEGACY_FREEZE_INCOMPLETE'
  when revoked_embedded_klaviyo_rows <> 1 then 'BLOCK_KLAVIYO_RESET_INCOMPLETE'
  when user_id_nullable then 'BLOCK_ACTIVATION_SCHEMA_ALREADY_APPLIED'
  else 'PASS_PREPARATION_ONLY'
end as r6a_preflight_gate,
state.*
from state;

