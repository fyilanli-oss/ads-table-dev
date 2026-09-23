-- R6-C read-only production preflight. Expected gate: PASS_PREPARATION_ONLY.
-- A PASS authorizes only a separately approved migration application.

with state as (
  select
    to_regclass('public.performance_dataset_rows_v2') is not null as dataset_table_exists,
    (select count(*) from public.performance_dataset_rows_v2) as dataset_v2_rows,
    (select count(*) from public.performance_dataset_rows_v2 where workspace_id is null) as unbound_rows,
    exists (
      select 1 from information_schema.columns
      where table_schema = 'public'
        and table_name = 'performance_dataset_rows_v2'
        and column_name = 'user_id'
        and is_nullable = 'NO'
    ) as user_id_currently_required,
    exists (
      select 1 from information_schema.columns
      where table_schema = 'public'
        and table_name = 'performance_dataset_rows_v2'
        and column_name = 'workspace_id'
        and is_nullable = 'YES'
    ) as workspace_id_staged_nullable,
    exists (
      select 1 from pg_constraint
      where conrelid = 'public.performance_dataset_rows_v2'::regclass
        and conname = 'performance_dataset_rows_v2_workspace_fk'
        and convalidated
    ) as workspace_fk_validated,
    exists (
      select 1 from pg_indexes
      where schemaname = 'public'
        and tablename = 'performance_dataset_rows_v2'
        and indexname = 'performance_dataset_rows_v2_workspace_canonical_uidx'
    ) as workspace_unique_index_exists,
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
    ) as legacy_select_policy_retained
)
select case
  when dataset_table_exists
    and dataset_v2_rows = 0
    and unbound_rows = 0
    and user_id_currently_required
    and workspace_id_staged_nullable
    and workspace_fk_validated
    and workspace_unique_index_exists
    and legacy_unique_index_retained
    and legacy_select_policy_retained
  then 'PASS_PREPARATION_ONLY' else 'FAIL'
end as r6c_preflight_gate,
state.*
from state;

