-- R7-B3: move new embedded OAuth continuations to Settings while preserving
-- ten-minute legacy transactions that were issued for the Platforms alias.
begin;

set local lock_timeout = '5s';

alter table public.oauth_transactions
  drop constraint oauth_transactions_authority_shape;

alter table public.oauth_transactions
  add constraint oauth_transactions_authority_shape check (
    (surface = 'standalone' and user_id is not null and shop_id is null and workspace_id is null
      and shopify_user_id is null and return_target is null)
    or
    (surface = 'shopify_embedded' and user_id is null and shop_id is not null and workspace_id is not null
      and length(btrim(shopify_user_id)) > 0
      and return_target in ('/shopify/app/settings', '/shopify/app/platforms'))
  );

commit;
