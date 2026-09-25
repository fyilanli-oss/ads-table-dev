-- R3-C1 read-only production postcheck. Expected gate: PASS.

with state as (
  select
    (select count(*) from public.performance_dataset_rows_v2) as dataset_v2_rows,
    exists (
      select 1 from information_schema.columns
      where table_schema = 'public'
        and table_name = 'performance_dataset_rows_v2'
        and column_name = 'workspace_id'
        and is_nullable = 'NO'
    ) as workspace_required,
    exists (
      select 1 from information_schema.columns
      where table_schema = 'public'
        and table_name = 'performance_dataset_rows_v2'
        and column_name = 'user_id'
        and is_nullable = 'YES'
    ) as compatibility_actor_nullable,
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
    not exists (
      select 1 from pg_indexes
      where schemaname = 'public'
        and tablename = 'performance_dataset_rows_v2'
        and indexname in (
          'performance_dataset_rows_v2_canonical_uidx',
          'performance_dataset_rows_v2_user_date_idx',
          'performance_dataset_rows_v2_account_scope_date_idx',
          'performance_dataset_rows_v2_entity_history_idx'
        )
    ) as legacy_user_indexes_retired,
    not exists (
      select 1 from pg_policies
      where schemaname = 'public'
        and tablename = 'performance_dataset_rows_v2'
        and policyname = 'performance_dataset_rows_v2_select_own'
    ) as legacy_browser_policy_retired,
    not has_table_privilege('authenticated', 'public.performance_dataset_rows_v2', 'SELECT') as authenticated_direct_select_revoked,
    has_table_privilege('service_role', 'public.performance_dataset_rows_v2', 'SELECT,INSERT,UPDATE,DELETE') as service_role_workspace_access_preserved,
    (select relrowsecurity from pg_class where oid = 'public.performance_dataset_rows_v2'::regclass) as rls_enabled
)
select case
  when dataset_v2_rows = 0
    and workspace_required
    and compatibility_actor_nullable
    and workspace_fk_validated
    and workspace_indexes_present
    and legacy_user_indexes_retired
    and legacy_browser_policy_retired
    and authenticated_direct_select_revoked
    and service_role_workspace_access_preserved
    and rls_enabled
  then 'PASS' else 'FAIL'
end as r3c1_postcheck_gate,
state.*
from state;
