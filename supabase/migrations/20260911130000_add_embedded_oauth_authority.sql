-- E10-T6-C1: add an embedded authority variant without weakening standalone OAuth identity.
alter table public.shopify_installations
  add constraint shopify_installations_shop_workspace_key unique (shop_id, workspace_id);

alter table public.oauth_transactions
  alter column user_id drop not null,
  add column surface text not null default 'standalone' check (surface in ('standalone', 'shopify_embedded')),
  add column shop_id text,
  add column workspace_id uuid,
  add column shopify_user_id text,
  add column return_target text,
  add constraint oauth_transactions_shop_workspace_fk
    foreign key (shop_id, workspace_id)
    references public.shopify_installations (shop_id, workspace_id) on delete cascade,
  add constraint oauth_transactions_authority_shape check (
    (surface = 'standalone' and user_id is not null and shop_id is null and workspace_id is null
      and shopify_user_id is null and return_target is null)
    or
    (surface = 'shopify_embedded' and user_id is null and shop_id is not null and workspace_id is not null
      and length(btrim(shopify_user_id)) > 0 and return_target = '/shopify/app/platforms')
  );

drop function public.consume_oauth_transaction(text, text, text);

create function public.consume_oauth_transaction(
  p_state_hash text, p_provider text, p_redirect_uri text
) returns table (
  user_id uuid, provider text, redirect_uri text, pkce_verifier text, created_at timestamptz,
  surface text, shop_id text, workspace_id uuid, shopify_user_id text, return_target text
)
language sql security definer set search_path = public, pg_temp
as $$
  delete from public.oauth_transactions
  where oauth_transactions.state_hash = p_state_hash
    and oauth_transactions.provider = p_provider
    and oauth_transactions.redirect_uri = p_redirect_uri
    and oauth_transactions.expires_at > now()
  returning oauth_transactions.user_id, oauth_transactions.provider,
            oauth_transactions.redirect_uri, oauth_transactions.pkce_verifier, oauth_transactions.created_at,
            oauth_transactions.surface, oauth_transactions.shop_id, oauth_transactions.workspace_id,
            oauth_transactions.shopify_user_id, oauth_transactions.return_target;
$$;

revoke all on function public.consume_oauth_transaction(text, text, text) from public, anon, authenticated;
grant execute on function public.consume_oauth_transaction(text, text, text) to service_role;
