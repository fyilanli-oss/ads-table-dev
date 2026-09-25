-- R3-C1 read-only production preflight. Expected gate: PASS.

with state as (
  select
    (select count(*) from public.performance_dataset_rows_v2) as dataset_v2_rows,
    (select count(*) from public.performance_dataset_rows_v2 where workspace_id is null) as unbound_rows,
    exists (
      select 1 from information_schema.columns
      where table_schema = 'public'
        and table_name = 'performance_dataset_rows_v2'
        and column_name = 'workspace_id'
        and is_nullable = 'YES'
    ) as workspace_still_staged_nullable,
    exists (
      select 1 from pg_constraint
      where conrelid = 'public.performance_dataset_rows_v2'::regclass
        and conname = 'performance_dataset_rows_v2_workspace_fk'
        and contype = 'f'
        and confrelid = 'public.workspaces'::regclass
        and convalidated
    ) as workspace_fk_validated,
    (
      select count(*) = 4
      from pg_indexes
      where schemaname = 'public'
        and tablename = 'performance_dataset_rows_v2'
        and indexname in (
          'performance_dataset_rows_v2_canonical_uidx',
          'performance_dataset_rows_v2_user_date_idx',
          'performance_dataset_rows_v2_account_scope_date_idx',
          'performance_dataset_rows_v2_entity_history_idx'
        )
    ) as legacy_user_indexes_present,
    exists (
      select 1 from pg_policies
      where schemaname = 'public'
        and tablename = 'performance_dataset_rows_v2'
        and policyname = 'performance_dataset_rows_v2_select_own'
    ) as legacy_user_policy_present,
    not exists (
      select 1 from pg_views
      where definition ilike '%performance_dataset_rows_v2%'
    ) as no_dependent_views,
    not exists (
      select 1
      from pg_proc p
      join pg_namespace n on n.oid = p.pronamespace
      where p.prokind = 'f'
        and pg_get_functiondef(p.oid) ilike '%performance_dataset_rows_v2%'
    ) as no_dependent_functions,
    not exists (
      select 1 from information_schema.triggers
      where event_object_schema = 'public'
        and event_object_table = 'performance_dataset_rows_v2'
    ) as no_dataset_triggers
)
select case
  when dataset_v2_rows <> 0 then 'BLOCK_DATASET_NOT_EMPTY'
  when unbound_rows <> 0 then 'BLOCK_UNBOUND_ROWS'
  when not workspace_still_staged_nullable then 'BLOCK_UNEXPECTED_WORKSPACE_NULLABILITY'
  when not workspace_fk_validated then 'BLOCK_WORKSPACE_FK'
  when not legacy_user_indexes_present then 'BLOCK_LEGACY_INDEX_DRIFT'
  when not legacy_user_policy_present then 'BLOCK_LEGACY_POLICY_DRIFT'
  when not no_dependent_views then 'BLOCK_DEPENDENT_VIEWS'
  when not no_dependent_functions then 'BLOCK_DEPENDENT_FUNCTIONS'
  when not no_dataset_triggers then 'BLOCK_DATASET_TRIGGERS'
  else 'PASS'
end as r3c1_preflight_gate,
state.*
from state;
