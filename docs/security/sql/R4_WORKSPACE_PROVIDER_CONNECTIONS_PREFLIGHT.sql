-- R4-B read-only production preflight. Expected gate: PASS.
with state as (
  select
    to_regclass('public.workspaces') is not null as workspaces_exists,
    to_regclass('public.workspace_provider_connections') is not null as canonical_connections_exists,
    to_regclass('public.shopify_workspace_provider_connections') is not null as shopify_connections_exists,
    to_regclass('public.platform_connections') is not null as legacy_connections_exists,
    to_regclass('public.platform_connection_tokens') is not null as legacy_tokens_exists,
    (select count(*) from public.workspaces) as workspace_rows,
    (select count(*) from public.shopify_workspace_provider_connections) as shopify_connection_rows,
    (select count(*) from public.platform_connections) as legacy_connection_rows,
    (select count(*) from public.platform_connection_tokens) as legacy_token_rows,
    (select count(*) from public.platform_connections where access_token is not null or refresh_token is not null)
      as legacy_plaintext_token_rows,
    (
      select count(*) from (
        select workspace_id, provider
        from public.shopify_workspace_provider_connections
        group by workspace_id, provider
        having count(*) > 1
      ) duplicates
    ) as shopify_duplicate_groups
)
select
  case
    when workspaces_exists
      and not canonical_connections_exists
      and shopify_connections_exists
      and legacy_connections_exists
      and legacy_tokens_exists
      and workspace_rows >= 1
      and legacy_plaintext_token_rows = 0
      and shopify_duplicate_groups = 0
    then 'PASS'
    else 'BLOCK_R4B_APPLY'
  end as r4_preflight_gate,
  *
from state;
