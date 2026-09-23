-- R2 rollback is allowed only before merchant currency settings or later R3/R4 dependencies exist.
-- It intentionally fails rather than deleting configured workspace data.

begin;

do $$
begin
  if to_regclass('public.workspace_settings') is not null
     and exists (select 1 from public.workspace_settings) then
    raise exception 'R2_ROLLBACK_BLOCKED_CONFIGURED_WORKSPACES';
  end if;

  if exists (
    select 1
    from pg_catalog.pg_constraint as constraint_row
    join pg_catalog.pg_class as referenced on referenced.oid = constraint_row.confrelid
    join pg_catalog.pg_namespace as namespace on namespace.oid = referenced.relnamespace
    where namespace.nspname = 'public'
      and referenced.relname = 'workspaces'
      and constraint_row.conname <> 'shopify_installations_workspace_fk'
  ) then
    raise exception 'R2_ROLLBACK_BLOCKED_DOWNSTREAM_DEPENDENCY';
  end if;
end;
$$;

alter table public.shopify_installations
  drop constraint if exists shopify_installations_workspace_fk;
drop table if exists public.workspace_settings;
drop table if exists public.workspaces;

commit;
