-- R6-C read-only postcheck. Expected gate after separately approved apply: PASS.

with state as (
  select
    exists (
      select 1 from information_schema.columns
      where table_schema = 'public'
        and table_name = 'performance_dataset_rows_v2'
        and column_name = 'user_id'
        and is_nullable = 'YES'
    ) as user_id_nullable,
    exists (
      select 1 from information_schema.columns
      where table_schema = 'public'
        and table_name = 'performance_dataset_rows_v2'
        and column_name = 'workspace_id'
        and is_nullable = 'YES'
    ) as workspace_id_still_staged_nullable,
    exists (
      select 1 from pg_indexes
      where schemaname = 'public'
        and tablename = 'performance_dataset_rows_v2'
        and indexname = 'performance_dataset_rows_v2_canonical_uidx'
    ) as legacy_unique_index_retained,
    exists (
      select 1 from pg_policies
      where schemaname = 'public'
        and tablename = 'performance_dataset_rows_v2'
        and policyname = 'performance_dataset_rows_v2_select_own'
    ) as legacy_select_policy_retained,
    (
      select count(*) = 4
      from pg_indexes
      where schemaname = 'public'
        and tablename = 'performance_dataset_rows_v2'
        and indexname in (
          'performance_dataset_rows_v2_workspace_canonical_uidx',
          'performance_dataset_rows_v2_workspace_date_idx',
          'performance_dataset_rows_v2_workspace_account_scope_date_idx',
          'performance_dataset_rows_v2_workspace_entity_history_idx'
        )
    ) as workspace_indexes_retained,
    (select count(*) from public.performance_dataset_rows_v2) as dataset_v2_rows
)
select case
  when user_id_nullable
    and workspace_id_still_staged_nullable
    and legacy_unique_index_retained
    and legacy_select_policy_retained
    and workspace_indexes_retained
    and dataset_v2_rows = 0
  then 'PASS' else 'FAIL'
end as r6c_postcheck_gate,
state.*
from state;

