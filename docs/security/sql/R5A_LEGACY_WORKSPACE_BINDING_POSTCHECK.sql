select jsonb_build_object(
  'table_exists', to_regclass('public.legacy_user_workspace_bindings') is not null,
  'binding_rows', (select count(*) from public.legacy_user_workspace_bindings),
  'active_binding_rows', (select count(*) from public.legacy_user_workspace_bindings where status = 'active'),
  'ambiguous_active_legacy_users', (
    select count(*) from (
      select legacy_user_id from public.legacy_user_workspace_bindings
      where status = 'active' group by legacy_user_id having count(*) > 1
    ) ambiguous
  ),
  'rls_enabled', (
    select relrowsecurity and relforcerowsecurity
    from pg_class where oid = 'public.legacy_user_workspace_bindings'::regclass
  ),
  'anon_privileges', has_table_privilege('anon', 'public.legacy_user_workspace_bindings', 'select,insert,update,delete'),
  'authenticated_privileges', has_table_privilege('authenticated', 'public.legacy_user_workspace_bindings', 'select,insert,update,delete'),
  'service_role_privileges', has_table_privilege('service_role', 'public.legacy_user_workspace_bindings', 'select,insert,update,delete'),
  'canonical_connections', (select count(*) from public.workspace_provider_connections),
  'legacy_klaviyo', (select count(*) from public.platform_connections where platform = 'klaviyo'),
  'embedded_klaviyo', (select count(*) from public.shopify_workspace_provider_connections where provider = 'klaviyo')
) as r5a_postcheck;
