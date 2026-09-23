-- R2 post-migration acceptance. Read-only; expected gate is PASS.

with object_checks as (
  select
    to_regclass('public.workspaces') is not null as workspaces_exists,
    to_regclass('public.workspace_settings') is not null as workspace_settings_exists
), rls_checks as (
  select
    count(*) filter (where relname = 'workspaces' and relrowsecurity and relforcerowsecurity) = 1 as workspaces_rls,
    count(*) filter (where relname = 'workspace_settings' and relrowsecurity and relforcerowsecurity) = 1 as settings_rls
  from pg_catalog.pg_class as relation
  join pg_catalog.pg_namespace as namespace on namespace.oid = relation.relnamespace
  where namespace.nspname = 'public'
    and relation.relname in ('workspaces', 'workspace_settings')
), privilege_checks as (
  select
    count(*) filter (where grantee in ('PUBLIC', 'anon', 'authenticated')) = 0 as browser_roles_denied,
    count(*) filter (
      where grantee = 'service_role'
        and privilege_type in ('SELECT', 'INSERT', 'UPDATE', 'DELETE')
    ) = 8 as service_role_exact_crud,
    count(*) filter (
      where grantee = 'service_role'
        and privilege_type not in ('SELECT', 'INSERT', 'UPDATE', 'DELETE')
    ) = 0 as service_role_no_extra_privileges
  from information_schema.role_table_grants
  where table_schema = 'public'
    and table_name in ('workspaces', 'workspace_settings')
), constraint_checks as (
  select count(*) = 1 as installation_fk_validated
  from pg_catalog.pg_constraint as constraint_row
  join pg_catalog.pg_class as relation on relation.oid = constraint_row.conrelid
  join pg_catalog.pg_namespace as namespace on namespace.oid = relation.relnamespace
  where namespace.nspname = 'public'
    and relation.relname = 'shopify_installations'
    and constraint_row.conname = 'shopify_installations_workspace_fk'
    and constraint_row.contype = 'f'
    and constraint_row.convalidated
), data_checks as (
  select
    not exists (
      select 1 from public.shopify_installations as installation
      left join public.workspaces as workspace on workspace.id = installation.workspace_id
      where workspace.id is null
    ) as no_installation_orphans,
    not exists (
      select 1 from public.workspace_settings
      where reporting_currency !~ '^[A-Z]{3}$'
         or reporting_currency_source <> 'merchant_selected'
         or reporting_currency_version <= 0
    ) as settings_valid
)
select
  case when
    object_checks.workspaces_exists
    and object_checks.workspace_settings_exists
    and rls_checks.workspaces_rls
    and rls_checks.settings_rls
    and privilege_checks.browser_roles_denied
    and privilege_checks.service_role_exact_crud
    and privilege_checks.service_role_no_extra_privileges
    and constraint_checks.installation_fk_validated
    and data_checks.no_installation_orphans
    and data_checks.settings_valid
  then 'PASS' else 'FAIL' end as r2_postcheck_gate,
  object_checks.*,
  rls_checks.*,
  privilege_checks.*,
  constraint_checks.*,
  data_checks.*
from object_checks, rls_checks, privilege_checks, constraint_checks, data_checks;
