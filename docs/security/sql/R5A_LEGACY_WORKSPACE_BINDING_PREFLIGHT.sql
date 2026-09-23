select jsonb_build_object(
  'workspaces', (select count(*) from public.workspaces),
  'canonical_connections', (select count(*) from public.workspace_provider_connections),
  'embedded_klaviyo', (select count(*) from public.shopify_workspace_provider_connections where provider = 'klaviyo'),
  'legacy_klaviyo', (select count(*) from public.platform_connections where platform = 'klaviyo'),
  'matching_nonnull_account_pairs', (
    select count(*)
    from public.platform_connections legacy
    join public.shopify_workspace_provider_connections embedded
      on embedded.provider = 'klaviyo'
     and legacy.platform = 'klaviyo'
     and legacy.account_id is not null
     and legacy.account_id = embedded.active_account_id
  ),
  'existing_binding_table', to_regclass('public.legacy_user_workspace_bindings') is not null,
  'oauth_rows_with_both_authorities', (
    select count(*) from public.oauth_transactions
    where user_id is not null and workspace_id is not null
  ),
  'plaintext_legacy_tokens', (
    select count(*) from public.platform_connections
    where access_token is not null or refresh_token is not null
  )
) as r5a_preflight;
