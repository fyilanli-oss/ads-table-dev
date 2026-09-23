-- R3-A read-only postcheck. Expected gate: PASS.

with state as (
  select
    exists (
      select 1 from information_schema.columns
      where table_schema = 'public'
        and table_name = 'performance_dataset_rows_v2'
        and column_name = 'workspace_id'
        and is_nullable = 'YES'
    ) as nullable_workspace_column,
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
          'performance_dataset_rows_v2_workspace_canonical_uidx',
          'performance_dataset_rows_v2_workspace_date_idx',
          'performance_dataset_rows_v2_workspace_account_scope_date_idx',
          'performance_dataset_rows_v2_workspace_entity_history_idx'
        )
    ) as workspace_indexes_present,
    exists (
      select 1 from pg_indexes
      where schemaname = 'public'
        and tablename = 'performance_dataset_rows_v2'
        and indexname = 'performance_dataset_rows_v2_canonical_uidx'
    ) as legacy_unique_index_retained,
    (select count(*) from public.performance_dataset_rows_v2) as dataset_v2_rows,
    (select count(*) from public.performance_dataset_rows_v2 where workspace_id is not null) as workspace_bound_rows
)
select case
  when nullable_workspace_column
    and workspace_fk_validated
    and workspace_indexes_present
    and legacy_unique_index_retained
    and dataset_v2_rows = 0
    and workspace_bound_rows = 0
  then 'PASS' else 'FAIL'
end as r3_postcheck_gate,
state.*
from state;
