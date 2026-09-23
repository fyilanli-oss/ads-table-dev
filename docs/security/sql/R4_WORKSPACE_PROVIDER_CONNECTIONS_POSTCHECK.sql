-- R4-B read-only postcheck. Expected gate: PASS.
with state as (
  select
    to_regclass('public.workspace_provider_connections') is not null as canonical_table_exists,
    (
      select count(*) = 0
      from public.workspace_provider_connections
    ) as canonical_table_empty,
    exists (
      select 1 from pg_constraint
      where conrelid = 'public.workspace_provider_connections'::regclass
        and contype = 'p'
        and pg_get_constraintdef(oid) = 'PRIMARY KEY (workspace_id, provider)'
    ) as canonical_primary_key,
    exists (
      select 1
      from pg_constraint c
      join pg_class target on target.oid = c.confrelid
      join pg_namespace target_ns on target_ns.oid = target.relnamespace
      where c.conrelid = 'public.workspace_provider_connections'::regclass
        and c.contype = 'f'
        and target_ns.nspname = 'public'
        and target.relname = 'workspaces'
        and c.convalidated
    ) as workspace_fk_validated,
    exists (
      select 1 from pg_indexes
      where schemaname = 'public'
        and tablename = 'workspace_provider_connections'
        and indexname = 'workspace_provider_connections_status_updated_idx'
    ) as lifecycle_index_exists,
    (
      select relrowsecurity and relforcerowsecurity
      from pg_class
      where oid = 'public.workspace_provider_connections'::regclass
    ) as rls_and_force_enabled,
    not exists (
      select 1 from information_schema.role_table_grants
      where table_schema = 'public'
        and table_name = 'workspace_provider_connections'
        and grantee in ('PUBLIC', 'anon', 'authenticated')
    ) as browser_roles_denied,
    (
      select count(*) = 4
      from information_schema.role_table_grants
      where table_schema = 'public'
        and table_name = 'workspace_provider_connections'
        and grantee = 'service_role'
        and privilege_type in ('SELECT', 'INSERT', 'UPDATE', 'DELETE')
    ) as service_crud_exact
)
select
  case
    when canonical_table_exists
      and canonical_table_empty
      and canonical_primary_key
      and workspace_fk_validated
      and lifecycle_index_exists
      and rls_and_force_enabled
      and browser_roles_denied
      and service_crud_exact
    then 'PASS'
    else 'FAIL'
  end as r4_postcheck_gate,
  *
from state;
